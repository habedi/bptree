/**
 * @file bptree.h
 * @brief A single-header B+ tree implementation in C.
 *
 * Bptree library provides a generic B+ tree data structure for an in-memory ordered map.
 * It needs only a C compiler supporting C11 standard or later.
 *
 * ===============================================================================
 * Configuration:
 * ===============================================================================
 * Key/value types and linkage can be customized via macros defined BEFORE
 * including the header (e.g., BPTREE_NUMERIC_TYPE, BPTREE_VALUE_TYPE,
 * BPTREE_KEY_TYPE_STRING/BPTREE_KEY_SIZE, BPTREE_STATIC).
 * See implementation details for specific macro effects.
 *
 * ===============================================================================
 * Usage:
 * ===============================================================================
 *
 * 1. Include this header ("bptree.h") in your source files for declarations.
 * 2. In EXACTLY ONE C source file, define BPTREE_IMPLEMENTATION before including
 *    this header to generate the function implementations.
 *
 * Example (in one .c file):
 * #define BPTREE_IMPLEMENTATION
 * // Optional: Define config macros here if needed
 * #include "bptree.h"
 *
 * ===============================================================================
 * Notes:
 * ===============================================================================
 *
 * - Error Handling:
 *   - Functions return `bptree_status` codes; check for `BPTREE_OK` for success.
 *
 * - Memory Management:
 *   - The tree manages memory for its internal nodes.
 *   - The tree DOES NOT manage memory for stored values (type `BPTREE_VALUE_TYPE`).
 *     If storing pointers, the caller must allocate/free the pointed-to data.
 *   - Call `bptree_free()` to release tree structure memory (does not free values).
 *
 * - Thread Safety:
 *   - This implementation is NOT thread-safe. Caller must provide external
 *     synchronization (e.g., mutexes) for concurrent access.
 *
 * @version 0.4.1-beta
 * @author
 *   "Hassan Abedi <hassan.abedi.t+bptree@gmail.com>"
 * @copyright MIT License
 */

#ifndef BPTREE_H
#define BPTREE_H

#ifdef __cplusplus
extern "C" {
#endif

#ifndef BPTREE_STATIC
#ifdef _WIN32
#define BPTREE_API __declspec(dllexport)
#else
#define BPTREE_API __attribute__((visibility("default")))
#endif
#else
#define BPTREE_API static
#endif

#include <assert.h>
#include <stdalign.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef BPTREE_KEY_TYPE_STRING
#ifndef BPTREE_KEY_SIZE
#error "Define BPTREE_KEY_SIZE for fixed-size string keys"
#endif
/**
 * @brief B+ tree key type for fixed-size strings.
 */
typedef struct {
    char data[BPTREE_KEY_SIZE];
} bptree_key_t;
#else
#ifndef BPTREE_NUMERIC_TYPE
#define BPTREE_NUMERIC_TYPE int64_t
#endif
/**
 * @brief B+ tree key type for numeric keys.
 */
typedef BPTREE_NUMERIC_TYPE bptree_key_t;
#endif

#ifndef BPTREE_VALUE_TYPE
#define BPTREE_VALUE_TYPE void*
#endif
/**
 * @brief B+ tree value type.
 *
 * May be a pointer or another type, depending on BPTREE_VALUE_TYPE.
 */
typedef BPTREE_VALUE_TYPE bptree_value_t;

/**
 * @brief Status codes returned by B+ tree functions.
 */
typedef enum {
    BPTREE_OK = 0,             /**< Operation successful */
    BPTREE_DUPLICATE_KEY,      /**< Duplicate key found during insertion */
    BPTREE_KEY_NOT_FOUND,      /**< Key not found */
    BPTREE_ALLOCATION_FAILURE, /**< Memory allocation failure */
    BPTREE_INVALID_ARGUMENT,   /**< Invalid argument passed */
    BPTREE_INTERNAL_ERROR      /**< Internal consistency error */
} bptree_status;

/**
 * @brief Internal B+ tree node.
 *
 * This structure is used internally by the tree; users should not access its members directly.
 */
typedef struct bptree_node bptree_node;
struct bptree_node {
    bool is_leaf;      /**< True if node is a leaf node */
    int num_keys;      /**< Number of keys stored in the node */
    bptree_node* next; /**< Pointer to the next leaf (used in range queries) */
    char data[]; /**< Flexible array member that holds keys and either values or child pointers */
};

/**
 * @brief B+ tree structure.
 *
 * Represents the B+ tree and holds its configuration along with the root pointer.
 */
typedef struct bptree {
    int count;             /**< Total number of key/value pairs in the tree */
    int height;            /**< Current height of the tree */
    bool enable_debug;     /**< If true, debug messages will be printed */
    int max_keys;          /**< Maximum number of keys allowed in any node */
    int min_leaf_keys;     /**< Minimum keys needed in a non-root leaf node */
    int min_internal_keys; /**< Minimum keys needed in a non-root internal node */
    int (*compare)(const bptree_key_t*, const bptree_key_t*); /**< Function to compare two keys */
    bptree_node* root; /**< Pointer to the root node of the tree */
} bptree;

/**
 * @brief B+ tree statistics.
 */
typedef struct bptree_stats {
    int count;      /**< Total number of key/value pairs */
    int height;     /**< Tree height */
    int node_count; /**< Total number of nodes in the tree */
} bptree_stats;

/*------------------------------------------------------------------------------
 * Public API
 *----------------------------------------------------------------------------*/

/**
 * @brief Creates a new B+ tree.
 *
 * Allocates and initializes a new B+ tree with the specified parameters.
 *
 * @param max_keys Maximum number of keys that each node can contain.
 *                 Must be at least 3.
 * @param compare Function pointer for comparing two keys. If NULL, the default comparison is used.
 * @param enable_debug Set to true to enable debug output.
 * @return Pointer to the newly created B+ tree, or NULL if allocation fails.
 */
BPTREE_API bptree* bptree_create(int max_keys,
                                 int (*compare)(const bptree_key_t*, const bptree_key_t*),
                                 bool enable_debug);

/**
 * @brief Frees a B+ tree.
 *
 * Releases all memory allocated for the B+ tree including all its nodes.
 * The values stored in the tree are not freed.
 *
 * @param tree Pointer to the B+ tree to free.
 */
BPTREE_API void bptree_free(bptree* tree);

/**
 * @brief Inserts a key-value pair into the tree.
 *
 * Inserts a new element. Returns BPTREE_DUPLICATE_KEY if the key already exists.
 * The function splits nodes if an overflow occurs.
 *
 * @param tree Pointer to the B+ tree.
 * @param key Pointer to the key to insert.
 * @param value The value associated with the key.
 * @return Status code indicating success or error type.
 */
BPTREE_API bptree_status bptree_put(bptree* tree, const bptree_key_t* key, bptree_value_t value);

/**
 * @brief Retrieves the value associated with a key.
 *
 * Searches the tree for the given key and returns the associated value.
 *
 * @param tree Pointer to the B+ tree.
 * @param key Pointer to the key to search.
 * @param out_value Pointer to store the retrieved value.
 * @return BPTREE_OK if found, otherwise BPTREE_KEY_NOT_FOUND.
 */
BPTREE_API bptree_status bptree_get(const bptree* tree, const bptree_key_t* key,
                                    bptree_value_t* out_value);

/**
 * @brief Removes a key-value pair from the tree.
 *
 * Deletes the pair associated with the key. May trigger rebalancing.
 *
 * @param tree Pointer to the B+ tree.
 * @param key Pointer to the key to remove.
 * @return BPTREE_OK if removal is successful.
 */
BPTREE_API bptree_status bptree_remove(bptree* tree, const bptree_key_t* key);

/**
 * @brief Retrieves a range of values.
 *
 * Returns all values with keys between start and end (inclusive). The results
 * are stored in a newly allocated array that must be freed using bptree_free_range_results().
 *
 * @param tree Pointer to the B+ tree.
 * @param start Starting key of the range.
 * @param end Ending key of the range.
 * @param out_values Pointer to the array pointer that will be allocated.
 * @param n_results Pointer to store the number of results.
 * @return BPTREE_OK if successful.
 */
BPTREE_API bptree_status bptree_get_range(const bptree* tree, const bptree_key_t* start,
                                          const bptree_key_t* end, bptree_value_t** out_values,
                                          int* n_results);

/**
 * @brief Frees a range query result.
 *
 * Releases memory allocated for the range query array.
 *
 * @param results Pointer to the allocated results.
 */
BPTREE_API void bptree_free_range_results(bptree_value_t* results);

/**
 * @brief Gets statistics about the tree.
 *
 * Returns a structure containing element count, tree height, and node count.
 *
 * @param tree Pointer to the B+ tree.
 * @return A bptree_stats structure.
 */
BPTREE_API bptree_stats bptree_get_stats(const bptree* tree);

/**
 * @brief Checks the internal invariants of the tree.
 *
 * Verifies that the tree's properties (key order, occupancy, etc.) are maintained.
 *
 * @param tree Pointer to the B+ tree.
 * @return True if all invariants hold, false otherwise.
 */
BPTREE_API bool bptree_check_invariants(const bptree* tree);

/**
 * @brief Checks if a key exists in the tree.
 *
 * Searches for the key and returns true if found.
 *
 * @param tree Pointer to the B+ tree.
 * @param key Pointer to the key to check.
 * @return True if found, false otherwise.
 */
BPTREE_API bool bptree_contains(const bptree* tree, const bptree_key_t* key);

#ifdef BPTREE_IMPLEMENTATION

/*==============================================================================
 * Internal Functions and Implementation Details
 *============================================================================*/

/**
 * @brief Print a debug message if debugging is enabled.
 *
 * This function wraps vprintf to add a timestamp and a tag to the debug output.
 *
 * @param enable Debug flag.
 * @param fmt Format string.
 * @param ... Additional arguments.
 */
static void bptree_debug_print(const bool enable, const char* fmt, ...) {
    if (!enable) return;  // Skip if debugging is turned off
    char time_buf[64];
    const time_t now = time(NULL);
    const struct tm* tm_info = localtime(&now);
    // Format the timestamp as "YYYY-MM-DD HH:MM:SS"
    strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", tm_info);
    printf("[%s] [BPTREE DEBUG] ", time_buf);
    va_list args;
    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
}

/**
 * @brief Compute the size of the keys area within a node.
 *
 * This function calculates the size needed to store all keys (max_keys+1)
 * and any padding required to meet the alignment of the values or child pointers.
 *
 * @param max_keys Maximum number of keys per node.
 * @return Size in bytes required for the keys area.
 */
static size_t bptree_keys_area_size(const int max_keys) {
    const size_t keys_size = (size_t)(max_keys + 1) * sizeof(bptree_key_t);
    const size_t req_align = (sizeof(bptree_value_t) > sizeof(bptree_node*) ? sizeof(bptree_value_t)
                                                                            : sizeof(bptree_node*));
    // Calculate required padding to meet alignment constraints
    const size_t pad = (req_align - (keys_size % req_align)) % req_align;
    return keys_size + pad;
}

/**
 * @brief Get pointer to keys in a node.
 *
 * Returns a pointer where the keys are stored in the node's flexible array.
 *
 * @param node Pointer to the node.
 * @return Pointer to the key array.
 */
static bptree_key_t* bptree_node_keys(const bptree_node* node) { return (bptree_key_t*)node->data; }

/**
 * @brief Get pointer to values stored in a leaf node.
 *
 * Computes the starting address of the values area within the node.
 *
 * @param node Pointer to the node.
 * @param max_keys Maximum keys per node.
 * @return Pointer to the values array.
 */
static bptree_value_t* bptree_node_values(bptree_node* node, const int max_keys) {
    const size_t offset = bptree_keys_area_size(max_keys);
    return (bptree_value_t*)(node->data + offset);
}

/**
 * @brief Get pointer to child pointers in an internal node.
 *
 * Computes the starting address of the children area within the node.
 *
 * @param node Pointer to the node.
 * @param max_keys Maximum keys per node.
 * @return Pointer to the array of child node pointers.
 */
static bptree_node** bptree_node_children(bptree_node* node, const int max_keys) {
    const size_t offset = bptree_keys_area_size(max_keys);
    return (bptree_node**)(node->data + offset);
}

#ifdef BPTREE_KEY_TYPE_STRING
/**
 * @brief Default key comparison for string keys.
 *
 * Compares two keys as fixed-size strings.
 *
 * @param a Pointer to the first key.
 * @param b Pointer to the second key.
 * @return Comparison result similar to memcmp.
 */
static inline int bptree_default_compare(const bptree_key_t* a, const bptree_key_t* b) {
    return memcmp(a->data, b->data, BPTREE_KEY_SIZE);
}
#else
/**
 * @brief Default key comparison for numeric keys.
 *
 * Compares two numeric keys.
 *
 * @param a Pointer to the first key.
 * @param b Pointer to the second key.
 * @return -1 if *a < *b, 1 if *a > *b, or 0 if equal.
 */
static int bptree_default_compare(const bptree_key_t* a, const bptree_key_t* b) {
    return (*a < *b) ? -1 : ((*a > *b) ? 1 : 0);
}
#endif

/**
 * @brief Find the smallest key in a subtree.
 *
 * Walks down the leftmost branch until a leaf is reached.
 *
 * @param node Pointer to the current node.
 * @param max_keys Maximum keys per node.
 * @return The smallest key found.
 */
static bptree_key_t bptree_find_smallest_key(bptree_node* node, const int max_keys) {
    assert(node != NULL);
    while (!node->is_leaf) {
        assert(node->num_keys >= 0);
        node = bptree_node_children(node, max_keys)[0];
        assert(node != NULL);
    }
    assert(node->num_keys > 0);
    return bptree_node_keys(node)[0];
}

/**
 * @brief Find the largest key in a subtree.
 *
 * Walks down the rightmost branch until a leaf is reached.
 *
 * @param node Pointer to the current node.
 * @param max_keys Maximum keys per node.
 * @return The largest key found.
 */
static bptree_key_t bptree_find_largest_key(bptree_node* node, const int max_keys) {
    assert(node != NULL);
    while (!node->is_leaf) {
        assert(node->num_keys >= 0);
        node = bptree_node_children(node, max_keys)[node->num_keys];
        assert(node != NULL);
    }
    assert(node->num_keys > 0);
    return bptree_node_keys(node)[node->num_keys - 1];
}

/**
 * @brief Count the total number of nodes in the subtree.
 *
 * Recursively traverses the subtree to count each node.
 *
 * @param node Pointer to the current node.
 * @param tree Pointer to the tree (for configuration values).
 * @return Total count of nodes.
 */
static int bptree_count_nodes(const bptree_node* node, const bptree* tree) {
    if (!node) return 0;
    if (node->is_leaf) return 1;
    int count = 1;  // Count the current internal node
    bptree_node** children = bptree_node_children((bptree_node*)node, tree->max_keys);
    for (int i = 0; i <= node->num_keys; i++) {
        count += bptree_count_nodes(children[i], tree);
    }
    return count;
}

/**
 * @brief Recursively check internal invariants of the tree.
 *
 * Verifies ordering, occupancy limits, and structural consistency.
 *
 * @param node Pointer to the current node.
 * @param tree Pointer to the tree.
 * @param depth Current depth in the tree.
 * @param leaf_depth Pointer to store the expected depth of leaves.
 * @return True if the invariants pass, false if any check fails.
 */
static bool bptree_check_invariants_node(bptree_node* node, const bptree* tree, const int depth,
                                         int* leaf_depth) {
    if (!node) return false;
    const bptree_key_t* keys = bptree_node_keys(node);
    const bool is_root = (tree->root == node);

    // Check that keys are in sorted order.
    for (int i = 1; i < node->num_keys; i++) {
        if (tree->compare(&keys[i - 1], &keys[i]) >= 0) {
            bptree_debug_print(tree->enable_debug, "Invariant Fail: Keys not sorted in node %p\n",
                               (void*)node);
            return false;
        }
    }

    if (node->is_leaf) {
        // For leaf nodes, record depth and ensure all leaves have the same depth.
        if (*leaf_depth == -1) {
            *leaf_depth = depth;
        } else if (depth != *leaf_depth) {
            bptree_debug_print(tree->enable_debug,
                               "Invariant Fail: Leaf depth mismatch (%d != %d) for node %p\n",
                               depth, *leaf_depth, (void*)node);
            return false;
        }
        // Check occupancy bounds for non-root leaf nodes.
        if (!is_root && (node->num_keys < tree->min_leaf_keys || node->num_keys > tree->max_keys)) {
            bptree_debug_print(
                tree->enable_debug,
                "Invariant Fail: Leaf node %p key count out of range [%d, %d] (%d keys)\n",
                (void*)node, tree->min_leaf_keys, tree->max_keys, node->num_keys);
            return false;
        }
        if (is_root && (node->num_keys > tree->max_keys && tree->count > 0)) {
            bptree_debug_print(tree->enable_debug,
                               "Invariant Fail: Root leaf node %p key count > max_keys (%d > %d)\n",
                               (void*)node, node->num_keys, tree->max_keys);
            return false;
        }
        if (is_root && tree->count == 0 && node->num_keys != 0) {
            bptree_debug_print(tree->enable_debug,
                               "Invariant Fail: Empty tree root leaf %p has keys (%d)\n",
                               (void*)node, node->num_keys);
            return false;
        }
        return true;
    } else {
        // For internal nodes, check occupancy constraints.
        if (!is_root &&
            (node->num_keys < tree->min_internal_keys || node->num_keys > tree->max_keys)) {
            bptree_debug_print(
                tree->enable_debug,
                "Invariant Fail: Internal node %p key count out of range [%d, %d] (%d keys)\n",
                (void*)node, tree->min_internal_keys, tree->max_keys, node->num_keys);
            return false;
        }
        if (is_root && tree->count > 0 && node->num_keys < 1) {
            bptree_debug_print(
                tree->enable_debug,
                "Invariant Fail: Internal root node %p has < 1 key (%d keys) in non-empty tree\n",
                (void*)node, node->num_keys);
            return false;
        }
        if (is_root && node->num_keys > tree->max_keys) {
            bptree_debug_print(tree->enable_debug,
                               "Invariant Fail: Internal root node %p has > max_keys (%d > %d)\n",
                               (void*)node, node->num_keys, tree->max_keys);
            return false;
        }
        bptree_node** children = bptree_node_children(node, tree->max_keys);
        // Check child pointers and recursively validate children.
        if (node->num_keys >= 0) {
            if (!children[0]) {
                bptree_debug_print(tree->enable_debug,
                                   "Invariant Fail: Internal node %p missing child[0]\n",
                                   (void*)node);
                return false;
            }
            // Validate left child's maximum key relative to parent's key[0].
            if (node->num_keys > 0 && (children[0]->num_keys > 0 || !children[0]->is_leaf)) {
                const bptree_key_t max_in_child0 =
                    bptree_find_largest_key(children[0], tree->max_keys);
                if (tree->compare(&max_in_child0, &keys[0]) >= 0) {
                    bptree_debug_print(tree->enable_debug,
                                       "Invariant Fail: max(child[0]) >= key[0] in node %p -- "
                                       "MaxChild=%lld Key=%lld\n",
                                       (void*)node, (long long)max_in_child0, (long long)keys[0]);
                    return false;
                }
            }
            if (!bptree_check_invariants_node(children[0], tree, depth + 1, leaf_depth))
                return false;
            // Loop over remaining children.
            for (int i = 1; i <= node->num_keys; i++) {
                if (!children[i]) {
                    bptree_debug_print(tree->enable_debug,
                                       "Invariant Fail: Internal node %p missing child[%d]\n",
                                       (void*)node, i);
                    return false;
                }
                if (children[i]->num_keys > 0 || !children[i]->is_leaf) {
                    bptree_key_t min_in_child =
                        bptree_find_smallest_key(children[i], tree->max_keys);
                    if (tree->compare(&keys[i - 1], &min_in_child) != 0) {
                        bptree_debug_print(tree->enable_debug,
                                           "Invariant Fail: key[%d] != min(child[%d]) in node %p\n",
                                           i - 1, i, (void*)node);
                        return false;
                    }
                    if (i < node->num_keys) {
                        bptree_key_t max_in_child =
                            bptree_find_largest_key(children[i], tree->max_keys);
                        if (tree->compare(&max_in_child, &keys[i]) >= 0) {
                            bptree_debug_print(
                                tree->enable_debug,
                                "Invariant Fail: max(child[%d]) >= key[%d] in node %p\n", i, i,
                                (void*)node);
                            return false;
                        }
                    }
                } else if (children[i]->is_leaf && children[i]->num_keys == 0 && tree->count > 0) {
                    bptree_debug_print(tree->enable_debug,
                                       "Invariant Fail: Internal node %p points to empty leaf "
                                       "child[%d] in non-empty tree\n",
                                       (void*)node, i);
                    return false;
                }
                if (!bptree_check_invariants_node(children[i], tree, depth + 1, leaf_depth))
                    return false;
            }
        } else {
            if (!is_root || tree->count > 0) {
                bptree_debug_print(tree->enable_debug,
                                   "Invariant Fail: Internal node %p has < 0 keys (%d)\n",
                                   (void*)node, node->num_keys);
                return false;
            }
        }
        return true;
    }
}

/**
 * @brief Calculate the total allocation size needed for a node.
 *
 * Determines the memory required for a node including its header,
 * keys area, and either the child pointers area (for internals) or the values area (for leaves).
 *
 * @param tree Pointer to the tree structure.
 * @param is_leaf True if node is a leaf.
 * @return Size in bytes required for the node allocation.
 */
static size_t bptree_node_alloc_size(const bptree* tree, const bool is_leaf) {
    const int max_keys = tree->max_keys;
    const size_t keys_area_sz = bptree_keys_area_size(max_keys);
    size_t data_payload_size;
    if (is_leaf) {
        data_payload_size = (size_t)(max_keys + 1) * sizeof(bptree_value_t);
    } else {
        data_payload_size = (size_t)(max_keys + 2) * sizeof(bptree_node*);
    }
    const size_t total_data_size = keys_area_sz + data_payload_size;
    return sizeof(bptree_node) + total_data_size;
}

/**
 * @brief Allocate a new node.
 *
 * Allocates memory for a node (leaf or internal) with proper alignment.
 *
 * @param tree Pointer to the tree.
 * @param is_leaf True if the node should be a leaf.
 * @return Pointer to the allocated node, or NULL on failure.
 */
static bptree_node* bptree_node_alloc(const bptree* tree, const bool is_leaf) {
    size_t max_align = alignof(bptree_node);
    max_align = (max_align > alignof(bptree_key_t)) ? max_align : alignof(bptree_key_t);
    if (is_leaf) {
        max_align = (max_align > alignof(bptree_value_t)) ? max_align : alignof(bptree_value_t);
    } else {
        max_align = (max_align > alignof(bptree_node*)) ? max_align : alignof(bptree_node*);
    }
    size_t size = bptree_node_alloc_size(tree, is_leaf);
    // Adjust size to be a multiple of the required alignment.
    size = (size + max_align - 1) & ~(max_align - 1);
    bptree_node* node = aligned_alloc(max_align, size);
    if (node) {
        node->is_leaf = is_leaf;
        node->num_keys = 0;
        node->next = NULL;
    } else {
        bptree_debug_print(tree->enable_debug, "Node allocation failed (size: %zu, align: %zu)\n",
                           size, max_align);
    }
    return node;
}

/**
 * @brief Recursively free a node and its children.
 *
 * Frees the memory for a node. For internal nodes, it recurses into all child nodes.
 *
 * @param node Pointer to the node to free.
 * @param tree Pointer to the tree.
 */
static void bptree_free_node(bptree_node* node, bptree* tree) {
    if (!node) return;
    if (!node->is_leaf) {
        bptree_node** children = bptree_node_children(node, tree->max_keys);
        for (int i = 0; i <= node->num_keys; i++) {
            bptree_free_node(children[i], tree);
        }
    }
    free(node);
}

/**
 * @brief Rebalance the tree upward from a given node.
 *
 * After a deletion, this function walks up the node stack and rebalances by borrowing
 * keys from siblings or merging nodes, if needed.
 *
 * @param tree Pointer to the tree.
 * @param node_stack Array of pointers representing the path from root to the underflowing node.
 * @param index_stack Array of indexes corresponding to the child position in each parent node.
 * @param depth Current depth where the underflow occurred.
 */
static void bptree_rebalance_up(bptree* tree, bptree_node** node_stack, const int* index_stack,
                                const int depth) {
    for (int d = depth - 1; d >= 0; d--) {
        bptree_node* parent = node_stack[d];
        const int child_idx = index_stack[d];
        bptree_node** children = bptree_node_children(parent, tree->max_keys);
        bptree_node* child = children[child_idx];
        const int min_keys = child->is_leaf ? tree->min_leaf_keys : tree->min_internal_keys;
        // If the node has enough keys, no need for rebalancing.
        if (child->num_keys >= min_keys) {
            bptree_debug_print(tree->enable_debug,
                               "Rebalance unnecessary at depth %d, child %d has %d keys (min %d)\n",
                               d, child_idx, child->num_keys, min_keys);
            break;
        }
        bptree_debug_print(tree->enable_debug,
                           "Rebalance needed at depth %d for child %d (%d keys < min %d)\n", d,
                           child_idx, child->num_keys, min_keys);
        // Try borrowing from the left sibling.
        if (child_idx > 0) {
            bptree_node* left_sibling = children[child_idx - 1];
            const int left_min =
                left_sibling->is_leaf ? tree->min_leaf_keys : tree->min_internal_keys;
            if (left_sibling->num_keys > left_min) {
                bptree_debug_print(tree->enable_debug,
                                   "Attempting borrow from left sibling (idx %d)\n", child_idx - 1);
                bptree_key_t* parent_keys = bptree_node_keys(parent);
                if (child->is_leaf) {
                    bptree_key_t* child_keys = bptree_node_keys(child);
                    bptree_value_t* child_vals = bptree_node_values(child, tree->max_keys);
                    const bptree_key_t* left_keys = bptree_node_keys(left_sibling);
                    const bptree_value_t* left_vals =
                        bptree_node_values(left_sibling, tree->max_keys);
                    // Shift keys and values right to open space at index 0.
                    memmove(&child_keys[1], &child_keys[0], child->num_keys * sizeof(bptree_key_t));
                    memmove(&child_vals[1], &child_vals[0],
                            child->num_keys * sizeof(bptree_value_t));
                    // Move the last key/value from the left sibling.
                    child_keys[0] = left_keys[left_sibling->num_keys - 1];
                    child_vals[0] = left_vals[left_sibling->num_keys - 1];
                    child->num_keys++;
                    left_sibling->num_keys--;
                    // Update the parent separator.
                    parent_keys[child_idx - 1] = child_keys[0];
                    bptree_debug_print(tree->enable_debug,
                                       "Borrowed leaf key from left. Parent key updated.\n");
                    break;
                } else {
                    // Internal node case: shift keys and children to insert the borrowed key.
                    bptree_key_t* child_keys = bptree_node_keys(child);
                    bptree_node** child_children = bptree_node_children(child, tree->max_keys);
                    bptree_key_t* left_keys = bptree_node_keys(left_sibling);
                    bptree_node** left_children =
                        bptree_node_children(left_sibling, tree->max_keys);
                    memmove(&child_keys[1], &child_keys[0], child->num_keys * sizeof(bptree_key_t));
                    memmove(&child_children[1], &child_children[0],
                            (child->num_keys + 1) * sizeof(bptree_node*));
                    child_keys[0] = parent_keys[child_idx - 1];
                    child_children[0] = left_children[left_sibling->num_keys];
                    parent_keys[child_idx - 1] = left_keys[left_sibling->num_keys - 1];
                    child->num_keys++;
                    left_sibling->num_keys--;
                    bptree_debug_print(
                        tree->enable_debug,
                        "Borrowed internal key/child from left. Parent key updated.\n");
                    break;
                }
            }
        }
        // Try borrowing from the right sibling.
        if (child_idx < parent->num_keys) {
            bptree_node* right_sibling = children[child_idx + 1];
            const int right_min =
                right_sibling->is_leaf ? tree->min_leaf_keys : tree->min_internal_keys;
            if (right_sibling->num_keys > right_min) {
                bptree_debug_print(tree->enable_debug,
                                   "Attempting borrow from right sibling (idx %d)\n",
                                   child_idx + 1);
                bptree_key_t* parent_keys = bptree_node_keys(parent);
                if (child->is_leaf) {
                    bptree_key_t* child_keys = bptree_node_keys(child);
                    bptree_value_t* child_vals = bptree_node_values(child, tree->max_keys);
                    bptree_key_t* right_keys = bptree_node_keys(right_sibling);
                    bptree_value_t* right_vals = bptree_node_values(right_sibling, tree->max_keys);
                    // Borrow the first key/value from the right sibling.
                    child_keys[child->num_keys] = right_keys[0];
                    child_vals[child->num_keys] = right_vals[0];
                    child->num_keys++;
                    right_sibling->num_keys--;
                    // Shift right sibling's keys/values left.
                    memmove(&right_keys[0], &right_keys[1],
                            right_sibling->num_keys * sizeof(bptree_key_t));
                    memmove(&right_vals[0], &right_vals[1],
                            right_sibling->num_keys * sizeof(bptree_value_t));
                    parent_keys[child_idx] = right_keys[0];
                    bptree_debug_print(tree->enable_debug,
                                       "Borrowed leaf key from right. Parent key updated.\n");
                    break;
                } else {
                    // Internal node: borrow key and child pointer from right sibling.
                    bptree_key_t* child_keys = bptree_node_keys(child);
                    bptree_node** child_children = bptree_node_children(child, tree->max_keys);
                    bptree_key_t* right_keys = bptree_node_keys(right_sibling);
                    bptree_node** right_children =
                        bptree_node_children(right_sibling, tree->max_keys);
                    child_keys[child->num_keys] = parent_keys[child_idx];
                    child_children[child->num_keys + 1] = right_children[0];
                    parent_keys[child_idx] = right_keys[0];
                    child->num_keys++;
                    right_sibling->num_keys--;
                    memmove(&right_keys[0], &right_keys[1],
                            right_sibling->num_keys * sizeof(bptree_key_t));
                    memmove(&right_children[0], &right_children[1],
                            (right_sibling->num_keys + 1) * sizeof(bptree_node*));
                    bptree_debug_print(
                        tree->enable_debug,
                        "Borrowed internal key/child from right. Parent key updated.\n");
                    break;
                }
            }
        }
        // If borrowing failed, attempt a merge.
        bptree_debug_print(tree->enable_debug, "Borrow failed, attempting merge\n");
        if (child_idx > 0) {
            // Merge with left sibling.
            bptree_node* left_sibling = children[child_idx - 1];
            bptree_debug_print(tree->enable_debug, "Merging child %d into left sibling %d\n",
                               child_idx, child_idx - 1);
            if (child->is_leaf) {
                bptree_key_t* left_keys = bptree_node_keys(left_sibling);
                bptree_value_t* left_vals = bptree_node_values(left_sibling, tree->max_keys);
                const bptree_key_t* child_keys = bptree_node_keys(child);
                const bptree_value_t* child_vals = bptree_node_values(child, tree->max_keys);
                const int combined_keys = left_sibling->num_keys + child->num_keys;
                if (combined_keys > tree->max_keys) {
                    fprintf(stderr,
                            "[BPTree FATAL] Merge-Left (Leaf) Buffer Overflow PREVENTED! Combined "
                            "keys %d > max_keys %d.\n",
                            combined_keys, tree->max_keys);
                    abort();
                }
                // Copy all keys and values from child to the left sibling.
                memcpy(left_keys + left_sibling->num_keys, child_keys,
                       child->num_keys * sizeof(bptree_key_t));
                memcpy(left_vals + left_sibling->num_keys, child_vals,
                       child->num_keys * sizeof(bptree_value_t));
                left_sibling->num_keys = combined_keys;
                left_sibling->next = child->next;
                free(child);
                children[child_idx] = NULL;
            } else {
                bptree_key_t* left_keys = bptree_node_keys(left_sibling);
                bptree_node** left_children = bptree_node_children(left_sibling, tree->max_keys);
                bptree_key_t* child_keys = bptree_node_keys(child);
                bptree_node** child_children = bptree_node_children(child, tree->max_keys);
                bptree_key_t* parent_keys = bptree_node_keys(parent);
                const int combined_keys = left_sibling->num_keys + 1 + child->num_keys;
                const int combined_children = (left_sibling->num_keys + 1) + (child->num_keys + 1);
                if (combined_keys > tree->max_keys) {
                    fprintf(stderr,
                            "[BPTree FATAL] Merge-Left (Internal) Key Buffer Overflow PREVENTED! "
                            "Combined keys %d > buffer %d.\n",
                            combined_keys, tree->max_keys);
                    abort();
                }
                if (combined_children > tree->max_keys + 1) {
                    fprintf(stderr,
                            "[BPTree FATAL] Merge-Left (Internal) Children Buffer Overflow "
                            "PREVENTED! Combined children %d > buffer %d.\n",
                            combined_children, tree->max_keys + 1);
                    abort();
                }
                left_keys[left_sibling->num_keys] = parent_keys[child_idx - 1];
                memcpy(left_keys + left_sibling->num_keys + 1, child_keys,
                       child->num_keys * sizeof(bptree_key_t));
                memcpy(left_children + left_sibling->num_keys + 1, child_children,
                       (child->num_keys + 1) * sizeof(bptree_node*));
                left_sibling->num_keys = combined_keys;
                free(child);
                children[child_idx] = NULL;
            }
            // Remove the parent separator key that pointed to the merged node.
            bptree_key_t* parent_keys = bptree_node_keys(parent);
            memmove(&parent_keys[child_idx - 1], &parent_keys[child_idx],
                    (parent->num_keys - child_idx) * sizeof(bptree_key_t));
            memmove(&children[child_idx], &children[child_idx + 1],
                    (parent->num_keys - child_idx) * sizeof(bptree_node*));
            parent->num_keys--;
            bptree_debug_print(tree->enable_debug, "Merge with left complete. Parent updated.\n");
        } else {
            // Merge with right sibling if no left sibling is available.
            bptree_node* right_sibling = children[child_idx + 1];
            bptree_debug_print(tree->enable_debug, "Merging right sibling %d into child %d\n",
                               child_idx + 1, child_idx);
            if (child->is_leaf) {
                bptree_key_t* child_keys = bptree_node_keys(child);
                bptree_value_t* child_vals = bptree_node_values(child, tree->max_keys);
                const bptree_key_t* right_keys = bptree_node_keys(right_sibling);
                const bptree_value_t* right_vals =
                    bptree_node_values(right_sibling, tree->max_keys);
                const int combined_keys = child->num_keys + right_sibling->num_keys;
                if (combined_keys > tree->max_keys) {
                    fprintf(stderr,
                            "[BPTree FATAL] Merge-Right (Leaf) Buffer Overflow PREVENTED! Combined "
                            "keys %d > max_keys %d.\n",
                            combined_keys, tree->max_keys);
                    abort();
                }
                memcpy(child_keys + child->num_keys, right_keys,
                       right_sibling->num_keys * sizeof(bptree_key_t));
                memcpy(child_vals + child->num_keys, right_vals,
                       right_sibling->num_keys * sizeof(bptree_value_t));
                child->num_keys = combined_keys;
                child->next = right_sibling->next;
                free(right_sibling);
                children[child_idx + 1] = NULL;
            } else {
                bptree_key_t* child_keys = bptree_node_keys(child);
                bptree_node** child_children = bptree_node_children(child, tree->max_keys);
                const bptree_key_t* right_keys = bptree_node_keys(right_sibling);
                bptree_node** right_children = bptree_node_children(right_sibling, tree->max_keys);
                const bptree_key_t* parent_keys = bptree_node_keys(parent);
                const int combined_keys = child->num_keys + 1 + right_sibling->num_keys;
                const int combined_children = (child->num_keys + 1) + (right_sibling->num_keys + 1);
                if (combined_keys > tree->max_keys) {
                    fprintf(stderr,
                            "[BPTree FATAL] Merge-Right (Internal) Key Buffer Overflow PREVENTED! "
                            "Combined keys %d > buffer %d.\n",
                            combined_keys, tree->max_keys);
                    abort();
                }
                if (combined_children > tree->max_keys + 1) {
                    fprintf(stderr,
                            "[BPTree FATAL] Merge-Right (Internal) Children Buffer Overflow "
                            "PREVENTED! Combined children %d > buffer %d.\n",
                            combined_children, tree->max_keys + 1);
                    abort();
                }
                child_keys[child->num_keys] = parent_keys[child_idx];
                memcpy(child_keys + child->num_keys + 1, right_keys,
                       right_sibling->num_keys * sizeof(bptree_key_t));
                memcpy(child_children + child->num_keys + 1, right_children,
                       (right_sibling->num_keys + 1) * sizeof(bptree_node*));
                child->num_keys = combined_keys;
                free(right_sibling);
                children[child_idx + 1] = NULL;
            }
            bptree_key_t* parent_keys = bptree_node_keys(parent);
            memmove(&parent_keys[child_idx], &parent_keys[child_idx + 1],
                    (parent->num_keys - child_idx - 1) * sizeof(bptree_key_t));
            memmove(&children[child_idx + 1], &children[child_idx + 2],
                    (parent->num_keys - child_idx - 1) * sizeof(bptree_node*));
            parent->num_keys--;
            bptree_debug_print(tree->enable_debug, "Merge with right complete. Parent updated.\n");
        }
    }
    // Check for the special case where the root becomes empty and the height can be reduced.
    if (tree->root && !tree->root->is_leaf && tree->root->num_keys == 0 && tree->count > 0) {
        bptree_debug_print(tree->enable_debug,
                           "Root node is internal and empty, shrinking height.\n");
        bptree_node* old_root = tree->root;
        tree->root = bptree_node_children(old_root, tree->max_keys)[0];
        tree->height--;
        free(old_root);
    } else if (tree->count == 0 && tree->root && tree->root->num_keys != 0) {
        bptree_debug_print(tree->enable_debug, "Tree empty, ensuring root node is empty.\n");
        tree->root->num_keys = 0;
    }
}

/**
 * @brief Binary search for a key in a node.
 *
 * Searches for the first position in the node's key array where the key could be inserted.
 *
 * @param tree Pointer to the tree.
 * @param node Pointer to the node.
 * @param key Pointer to the key.
 * @return The index at which the key is found or should be inserted.
 */
static int bptree_node_search(const bptree* tree, const bptree_node* node,
                              const bptree_key_t* key) {
    int low = 0, high = node->num_keys;
    const bptree_key_t* keys = bptree_node_keys(node);
    // Adjust behavior for leaf and internal nodes
    if (node->is_leaf) {
        while (low < high) {
            const int mid = low + (high - low) / 2;
            const int cmp = tree->compare(key, &keys[mid]);
            if (cmp <= 0) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
    } else {
        while (low < high) {
            const int mid = low + (high - low) / 2;
            const int cmp = tree->compare(key, &keys[mid]);
            if (cmp < 0) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
    }
    return low;
}

/**
 * @brief Recursive insertion helper.
 *
 * Inserts a key/value pair into the subtree rooted at @p node.
 * Handles splitting of nodes if necessary and propagates split keys upward.
 *
 * @param tree Pointer to the B+ tree.
 * @param node Current node.
 * @param key Pointer to the key to insert.
 * @param value Value to insert.
 * @param promoted_key Pointer to store the key to be promoted if a split occurs.
 * @param new_child Pointer to store the new node created from the split.
 * @return Status code indicating success or failure.
 */
static bptree_status bptree_insert_internal(bptree* tree, bptree_node* node,
                                            const bptree_key_t* key, const bptree_value_t value,
                                            bptree_key_t* promoted_key, bptree_node** new_child) {
    const int pos = bptree_node_search(tree, node, key);
    if (node->is_leaf) {
        bptree_key_t* keys = bptree_node_keys(node);
        bptree_value_t* values = bptree_node_values(node, tree->max_keys);
        // If key exists, report duplicate.
        if (pos < node->num_keys && tree->compare(key, &keys[pos]) == 0) {
            bptree_debug_print(tree->enable_debug, "Insert failed: Duplicate key found.\n");
            return BPTREE_DUPLICATE_KEY;
        }
        // Shift keys and values to make room for the new key/value.
        memmove(&keys[pos + 1], &keys[pos], (node->num_keys - pos) * sizeof(bptree_key_t));
        memmove(&values[pos + 1], &values[pos], (node->num_keys - pos) * sizeof(bptree_value_t));
        keys[pos] = *key;
        values[pos] = value;
        node->num_keys++;
        *new_child = NULL;
        bptree_debug_print(tree->enable_debug, "Inserted key in leaf. Node keys: %d\n",
                           node->num_keys);
        // Check for overflow and split if needed.
        if (node->num_keys > tree->max_keys) {
            bptree_debug_print(tree->enable_debug, "Leaf node overflow (%d > %d), splitting.\n",
                               node->num_keys, tree->max_keys);
            const int total_keys = node->num_keys;
            const int split_idx = (total_keys + 1) / 2;
            const int new_node_keys = total_keys - split_idx;
            bptree_node* new_leaf = bptree_node_alloc(tree, true);
            if (!new_leaf) {
                node->num_keys--;
                bptree_debug_print(tree->enable_debug, "Leaf split allocation failed!\n");
                return BPTREE_ALLOCATION_FAILURE;
            }
            bptree_key_t* new_keys = bptree_node_keys(new_leaf);
            bptree_value_t* new_values = bptree_node_values(new_leaf, tree->max_keys);
            // Move the latter half keys/values to the new leaf.
            memcpy(new_keys, &keys[split_idx], new_node_keys * sizeof(bptree_key_t));
            memcpy(new_values, &values[split_idx], new_node_keys * sizeof(bptree_value_t));
            new_leaf->num_keys = new_node_keys;
            node->num_keys = split_idx;
            new_leaf->next = node->next;
            node->next = new_leaf;
            *promoted_key = new_keys[0];
            *new_child = new_leaf;
            bptree_debug_print(tree->enable_debug,
                               "Leaf split complete. Promoted key. Left keys: %d, Right keys: %d\n",
                               node->num_keys, new_leaf->num_keys);
        }
        return BPTREE_OK;
    } else {
        // Recurse into the appropriate child.
        bptree_node** children = bptree_node_children(node, tree->max_keys);
        bptree_key_t child_promoted_key;
        bptree_node* child_new_node = NULL;
        const bptree_status status = bptree_insert_internal(tree, children[pos], key, value,
                                                            &child_promoted_key, &child_new_node);
        if (status != BPTREE_OK || child_new_node == NULL) {
            return status;
        }
        bptree_debug_print(tree->enable_debug,
                           "Child split propagated. Inserting promoted key into internal node.\n");
        bptree_key_t* keys = bptree_node_keys(node);
        // Shift parent's keys and child pointers to insert the promoted key.
        memmove(&keys[pos + 1], &keys[pos], (node->num_keys - pos) * sizeof(bptree_key_t));
        memmove(&children[pos + 2], &children[pos + 1],
                (node->num_keys - pos) * sizeof(bptree_node*));
        keys[pos] = child_promoted_key;
        children[pos + 1] = child_new_node;
        node->num_keys++;
        bptree_debug_print(tree->enable_debug, "Internal node keys: %d\n", node->num_keys);
        // Split internal node if it exceeds capacity.
        if (node->num_keys > tree->max_keys) {
            bptree_debug_print(tree->enable_debug, "Internal node overflow (%d > %d), splitting.\n",
                               node->num_keys, tree->max_keys);
            const int total_keys = node->num_keys;
            const int split_idx = total_keys / 2;
            const int new_node_keys = total_keys - split_idx - 1;
            bptree_node* new_internal = bptree_node_alloc(tree, false);
            if (!new_internal) {
                node->num_keys--;
                bptree_debug_print(tree->enable_debug, "Internal split allocation failed!\n");
                return BPTREE_ALLOCATION_FAILURE;
            }
            bptree_key_t* new_keys = bptree_node_keys(new_internal);
            bptree_node** new_children = bptree_node_children(new_internal, tree->max_keys);
            *promoted_key = keys[split_idx];
            *new_child = new_internal;
            memcpy(new_keys, &keys[split_idx + 1], new_node_keys * sizeof(bptree_key_t));
            memcpy(new_children, &children[split_idx + 1],
                   (new_node_keys + 1) * sizeof(bptree_node*));
            new_internal->num_keys = new_node_keys;
            node->num_keys = split_idx;
            bptree_debug_print(
                tree->enable_debug,
                "Internal split complete. Promoted key. Left keys: %d, Right keys: %d\n",
                node->num_keys, new_internal->num_keys);
        } else {
            *new_child = NULL;
        }
        return BPTREE_OK;
    }
}

BPTREE_API bptree_status bptree_put(bptree* tree, const bptree_key_t* key, bptree_value_t value) {
    if (!tree || !key) return BPTREE_INVALID_ARGUMENT;
    if (!tree->root) return BPTREE_INTERNAL_ERROR;
    bptree_key_t promoted_key;
    bptree_node* new_node = NULL;
    const bptree_status status =
        bptree_insert_internal(tree, tree->root, key, value, &promoted_key, &new_node);
    if (status == BPTREE_OK) {
        // If a split occurred at the root, create a new root.
        if (new_node != NULL) {
            bptree_debug_print(tree->enable_debug, "Root split occurred. Creating new root.\n");
            bptree_node* new_root = bptree_node_alloc(tree, false);
            if (!new_root) {
                bptree_free_node(new_node, tree);
                return BPTREE_ALLOCATION_FAILURE;
            }
            bptree_key_t* root_keys = bptree_node_keys(new_root);
            bptree_node** root_children = bptree_node_children(new_root, tree->max_keys);
            root_keys[0] = promoted_key;
            root_children[0] = tree->root;
            root_children[1] = new_node;
            new_root->num_keys = 1;
            tree->root = new_root;
            tree->height++;
            bptree_debug_print(tree->enable_debug, "New root created. Tree height: %d\n",
                               tree->height);
        }
        tree->count++;
    } else {
        bptree_debug_print(tree->enable_debug,
                           "Insertion failed (Status: %d), count not incremented.\n", status);
    }
    return status;
}

BPTREE_API bptree_status bptree_get(const bptree* tree, const bptree_key_t* key,
                                    bptree_value_t* out_value) {
    if (!tree || !tree->root || !key || !out_value) return BPTREE_INVALID_ARGUMENT;
    if (tree->count == 0) return BPTREE_KEY_NOT_FOUND;
    bptree_node* node = tree->root;
    // Traverse the tree until a leaf is reached.
    while (!node->is_leaf) {
        const int pos = bptree_node_search(tree, node, key);
        node = bptree_node_children(node, tree->max_keys)[pos];
        if (!node) return BPTREE_INTERNAL_ERROR;
    }
    int pos = bptree_node_search(tree, node, key);
    const bptree_key_t* keys = bptree_node_keys(node);
    if (pos < node->num_keys && tree->compare(key, &keys[pos]) == 0) {
        *out_value = bptree_node_values(node, tree->max_keys)[pos];
        return BPTREE_OK;
    }
    return BPTREE_KEY_NOT_FOUND;
}

BPTREE_API bptree_status bptree_remove(bptree* tree, const bptree_key_t* key) {
#define BPTREE_MAX_HEIGHT_REMOVE 64
    bptree_node* node_stack[BPTREE_MAX_HEIGHT_REMOVE];
    int index_stack[BPTREE_MAX_HEIGHT_REMOVE];
    int depth = 0;
    if (!tree || !tree->root || !key) return BPTREE_INVALID_ARGUMENT;
    if (tree->count == 0) return BPTREE_KEY_NOT_FOUND;
    bptree_node* node = tree->root;
    // Traverse down the tree and record the path (nodes and child indexes)
    while (!node->is_leaf) {
        if (depth >= BPTREE_MAX_HEIGHT_REMOVE) {
            return BPTREE_INTERNAL_ERROR;
        }
        const int pos = bptree_node_search(tree, node, key);
        node_stack[depth] = node;
        index_stack[depth] = pos;
        depth++;
        node = bptree_node_children(node, tree->max_keys)[pos];
        if (!node) return BPTREE_INTERNAL_ERROR;
    }
    const int pos = bptree_node_search(tree, node, key);
    bptree_key_t* keys = bptree_node_keys(node);
    if (pos >= node->num_keys || tree->compare(key, &keys[pos]) != 0) {
        return BPTREE_KEY_NOT_FOUND;
    }
    // Save the key being deleted for potential parent updates.
    const bptree_key_t deleted_key_copy = keys[pos];
    bptree_value_t* values = bptree_node_values(node, tree->max_keys);
    // Remove key and value by shifting remaining entries left.
    memmove(&keys[pos], &keys[pos + 1], (node->num_keys - pos - 1) * sizeof(bptree_key_t));
    memmove(&values[pos], &values[pos + 1], (node->num_keys - pos - 1) * sizeof(bptree_value_t));
    node->num_keys--;
    tree->count--;
    bptree_debug_print(tree->enable_debug, "Removed key from leaf. Node keys: %d, Tree count: %d\n",
                       node->num_keys, tree->count);
    // Update parent's separator if the smallest key in the leaf has changed.
    if (pos == 0 && depth > 0 && node->num_keys > 0) {
        // The smallest key in `node` was deleted. We must find the separator key
        // in an ancestor that referred to the deleted key and update it to the
        // new smallest key in `node`.
        for (int d = depth - 1; d >= 0; d--) {
            const int parent_child_idx = index_stack[d];
            if (parent_child_idx > 0) {
                bptree_node* parent = node_stack[d];
                bptree_key_t* parent_keys = bptree_node_keys(parent);
                const int separator_idx = parent_child_idx - 1;
                if (separator_idx < parent->num_keys &&
                    tree->compare(&parent_keys[separator_idx], &deleted_key_copy) == 0) {
                    bptree_debug_print(tree->enable_debug,
                                       "Updating ancestor separator key [%d] at depth %d.\n",
                                       separator_idx, d);
                    parent_keys[separator_idx] = keys[0];
                    break;  // Found and updated the key, no need to go higher.
                }
            }
        }
    }
    const bool root_is_leaf = (depth == 0);
    // If underflow occurs in a non-root leaf, rebalance upward.
    if (!root_is_leaf && node->num_keys < tree->min_leaf_keys) {
        bptree_debug_print(tree->enable_debug, "Leaf underflow (%d < %d), starting rebalance.\n",
                           node->num_keys, tree->min_leaf_keys);
        bptree_rebalance_up(tree, node_stack, index_stack, depth);
    } else if (root_is_leaf && tree->count == 0) {
        assert(tree->root == node);
        assert(tree->root->num_keys == 0);
        bptree_debug_print(tree->enable_debug, "Last key removed, root is empty leaf.\n");
    }
#undef BPTREE_MAX_HEIGHT_REMOVE
    return BPTREE_OK;
}

BPTREE_API bptree_status bptree_get_range(const bptree* tree, const bptree_key_t* start,
                                          const bptree_key_t* end, bptree_value_t** out_values,
                                          int* n_results) {
    if (!tree || !tree->root || !start || !end || !out_values || !n_results) {
        return BPTREE_INVALID_ARGUMENT;
    }
    *out_values = NULL;
    *n_results = 0;
    if (tree->compare(start, end) > 0) {
        return BPTREE_INVALID_ARGUMENT;
    }
    if (tree->count == 0) {
        return BPTREE_OK;
    }
    bptree_node* node = tree->root;
    // Locate the starting leaf node.
    while (!node->is_leaf) {
        const int pos = bptree_node_search(tree, node, start);
        node = bptree_node_children(node, tree->max_keys)[pos];
        if (!node) return BPTREE_INTERNAL_ERROR;
    }
    int count = 0;
    bptree_node* current_node = node;
    bool past_end = false;
    // Count how many keys fall within the range.
    while (current_node && !past_end) {
        const bptree_key_t* keys = bptree_node_keys(current_node);
        for (int i = 0; i < current_node->num_keys; i++) {
            if (tree->compare(&keys[i], start) >= 0) {
                if (tree->compare(&keys[i], end) <= 0) {
                    count++;
                } else {
                    past_end = true;
                    break;
                }
            }
        }
        if (!past_end) {
            current_node = current_node->next;
        }
    }
    if (count == 0) {
        return BPTREE_OK;
    }
    const size_t alloc_size = (size_t)count * sizeof(bptree_value_t);
    *out_values = malloc(alloc_size);
    if (!*out_values) {
        return BPTREE_ALLOCATION_FAILURE;
    }
    int index = 0;
    current_node = node;
    past_end = false;
    // Populate the output array with values within the key range.
    while (current_node && !past_end && index < count) {
        const bptree_key_t* keys = bptree_node_keys(current_node);
        const bptree_value_t* values = bptree_node_values(current_node, tree->max_keys);
        for (int i = 0; i < current_node->num_keys; i++) {
            if (tree->compare(&keys[i], start) >= 0) {
                if (tree->compare(&keys[i], end) <= 0) {
                    if (index < count) {
                        (*out_values)[index++] = values[i];
                    } else {
                        bptree_debug_print(tree->enable_debug,
                                           "Range Error: Exceeded count during collection.\n");
                        past_end = true;
                        break;
                    }
                } else {
                    past_end = true;
                    break;
                }
            }
        }
        if (!past_end) {
            current_node = current_node->next;
        }
    }
    *n_results = index;
    if (index != count) {
        bptree_debug_print(tree->enable_debug, "Range Warning: Final index %d != counted %d\n",
                           index, count);
    }
    return BPTREE_OK;
}

BPTREE_API void bptree_free_range_results(bptree_value_t* results) { free(results); }

BPTREE_API bptree_stats bptree_get_stats(const bptree* tree) {
    bptree_stats stats;
    if (!tree) {
        stats.count = 0;
        stats.height = 0;
        stats.node_count = 0;
    } else {
        stats.count = tree->count;
        stats.height = tree->height;
        stats.node_count = bptree_count_nodes(tree->root, tree);
    }
    return stats;
}

BPTREE_API bool bptree_check_invariants(const bptree* tree) {
    if (!tree || !tree->root) return false;
    if (tree->count == 0) {
        if (tree->root->is_leaf && tree->root->num_keys == 0 && tree->height == 1) {
            return true;
        } else {
            bptree_debug_print(tree->enable_debug, "Invariant Fail: Empty tree state incorrect.\n");
            return false;
        }
    }
    int leaf_depth = -1;
    return bptree_check_invariants_node(tree->root, tree, 0, &leaf_depth);
}

BPTREE_API bool bptree_contains(const bptree* tree, const bptree_key_t* key) {
    bptree_value_t dummy_value;
    return (bptree_get(tree, key, &dummy_value) == BPTREE_OK);
}

BPTREE_API bptree* bptree_create(const int max_keys,
                                 int (*compare)(const bptree_key_t*, const bptree_key_t*),
                                 const bool enable_debug) {
    if (max_keys < 3) {
        fprintf(stderr, "[BPTREE CREATE] Error: max_keys must be at least 3.\n");
        return NULL;
    }
    bptree* tree = malloc(sizeof(bptree));
    if (!tree) {
        fprintf(stderr, "[BPTREE CREATE] Error: Failed to allocate memory for tree structure.\n");
        return NULL;
    }
    tree->count = 0;
    tree->height = 1;
    tree->enable_debug = enable_debug;
    tree->max_keys = max_keys;
    tree->min_internal_keys = ((max_keys + 2) / 2) - 1;
    if (tree->min_internal_keys < 1) {
        tree->min_internal_keys = 1;
    }
    tree->min_leaf_keys = (max_keys + 1) / 2;
    if (tree->min_leaf_keys < 1) {
        tree->min_leaf_keys = 1;
    }
    if (tree->min_leaf_keys > tree->max_keys) tree->min_leaf_keys = tree->max_keys;
    bptree_debug_print(enable_debug, "Creating tree. max_keys=%d, min_internal=%d, min_leaf=%d\n",
                       tree->max_keys, tree->min_internal_keys, tree->min_leaf_keys);
    tree->compare = compare ? compare : bptree_default_compare;
    tree->root = bptree_node_alloc(tree, true);
    if (!tree->root) {
        fprintf(stderr, "[BPTREE CREATE] Error: Failed to allocate initial root node.\n");
        free(tree);
        return NULL;
    }
    bptree_debug_print(enable_debug, "Tree created successfully.\n");
    return tree;
}

BPTREE_API void bptree_free(bptree* tree) {
    if (!tree) return;
    if (tree->root) {
        bptree_free_node(tree->root, tree);
    }
    free(tree);
}

#endif

#ifdef __cplusplus
}
#endif

#endif
