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
typedef struct {
    char data[BPTREE_KEY_SIZE];
} bptree_key_t;
#else
#ifndef BPTREE_NUMERIC_TYPE
#define BPTREE_NUMERIC_TYPE int64_t
#endif
typedef BPTREE_NUMERIC_TYPE bptree_key_t;
#endif

#ifndef BPTREE_VALUE_TYPE
#define BPTREE_VALUE_TYPE void *
#endif
typedef BPTREE_VALUE_TYPE bptree_value_t;

typedef enum {
    BPTREE_OK = 0,
    BPTREE_DUPLICATE_KEY,
    BPTREE_KEY_NOT_FOUND,
    BPTREE_ALLOCATION_FAILURE,
    BPTREE_INVALID_ARGUMENT,
    BPTREE_INTERNAL_ERROR
} bptree_status;

typedef struct bptree_node bptree_node;
struct bptree_node {
    bool is_leaf;
    int num_keys;
    bptree_node *next;
    char data[];
};

typedef struct bptree {
    int count;
    int height;
    bool enable_debug;
    int max_keys;
    int min_leaf_keys;
    int min_internal_keys;
    int (*compare)(const bptree_key_t *, const bptree_key_t *);
    bptree_node *root;
} bptree;

typedef struct bptree_stats {
    int count;
    int height;
    int node_count;
} bptree_stats;

/* Public API */
BPTREE_API bptree *bptree_create(int max_keys,
                                 int (*compare)(const bptree_key_t *, const bptree_key_t *),
                                 bool enable_debug);
BPTREE_API void bptree_free(bptree *tree);
BPTREE_API bptree_status bptree_put(bptree *tree, const bptree_key_t *key, bptree_value_t value);
BPTREE_API bptree_status bptree_get(const bptree *tree, const bptree_key_t *key,
                                    bptree_value_t *out_value);
BPTREE_API bptree_status bptree_remove(bptree *tree, const bptree_key_t *key);
BPTREE_API bptree_status bptree_get_range(const bptree *tree, const bptree_key_t *start,
                                          const bptree_key_t *end, bptree_value_t **out_values,
                                          int *n_results);
BPTREE_API void bptree_free_range_results(bptree_value_t *results);
BPTREE_API bptree_stats bptree_get_stats(const bptree *tree);
BPTREE_API bool bptree_check_invariants(const bptree *tree);
BPTREE_API bool bptree_contains(const bptree *tree, const bptree_key_t *key);

#ifdef BPTREE_IMPLEMENTATION

static inline void bptree_debug_print(bool enable, const char *fmt, ...) {
    if (!enable) return;
    char time_buf[64];
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", tm_info);
    printf("[%s] [BPTREE DEBUG] ", time_buf);
    va_list args;
    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
}

#define BPTREE_MAX_ALIGN(a, b) ((a) > (b) ? (a) : (b))

/*
   Reserve space for (max_keys+1) keys.
   This extra slot is used for the temporary overflow before splitting.
*/
static inline size_t bptree_keys_area_size(int max_keys) {
    size_t keys_size = (size_t)(max_keys + 1) * sizeof(bptree_key_t);
    // Ensure alignment for subsequent value/pointer arrays
    size_t req_align = BPTREE_MAX_ALIGN(alignof(bptree_value_t), alignof(bptree_node *));
    size_t pad = (req_align - (keys_size % req_align)) % req_align;
    return keys_size + pad;
}

static inline bptree_key_t *bptree_node_keys(bptree_node *node) {
    // Keys start immediately after the node header
    return (bptree_key_t *)node->data;
}

/*
   For leaf nodes: reserve (max_keys+1) values for temporary overflow during insertion.
*/
static inline bptree_value_t *bptree_node_values(bptree_node *node, int max_keys) {
    size_t offset = bptree_keys_area_size(max_keys);
    // Values start after the padded key area
    return (bptree_value_t *)(node->data + offset);
}

/*
   For internal nodes: reserve (max_keys+2) child pointers for temporary overflow during insertion.
   (An internal node with k keys has k+1 children; max_keys+1 keys require max_keys+2 children).
*/
static inline bptree_node **bptree_node_children(bptree_node *node, int max_keys) {
    size_t offset = bptree_keys_area_size(max_keys);
    // Children start after the padded key area
    return (bptree_node **)(node->data + offset);
}

#ifdef BPTREE_KEY_TYPE_STRING
static inline int bptree_default_compare(const bptree_key_t *a, const bptree_key_t *b) {
    // Ensure null termination isn't relied upon if keys can fill BPTREE_KEY_SIZE
    return memcmp(a->data, b->data, BPTREE_KEY_SIZE);
    // Original used strncmp, which stops at null terminators. memcmp is safer for fixed-size binary
    // data. return strncmp(a->data, b->data, BPTREE_KEY_SIZE);
}
#else
static inline int bptree_default_compare(const bptree_key_t *a, const bptree_key_t *b) {
    // Standard comparison for numeric types
    return (*a < *b) ? -1 : ((*a > *b) ? 1 : 0);
}
#endif

// Finds the smallest key in the subtree rooted at 'node'
static inline bptree_key_t bptree_find_smallest_key(bptree_node *node, int max_keys) {
    assert(node != NULL);
    // Smallest key is always in the leftmost leaf descendant
    while (!node->is_leaf) {
        assert(node->num_keys >= 0);  // Allow 0 keys temporarily during ops
        node = bptree_node_children(node, max_keys)[0];
        assert(node != NULL);
    }
    assert(node->num_keys > 0);  // Final leaf must have keys if tree isn't empty
    return bptree_node_keys(node)[0];
}

// Finds the largest key in the subtree rooted at 'node'
static inline bptree_key_t bptree_find_largest_key(bptree_node *node, int max_keys) {
    assert(node != NULL);
    // Largest key is always in the rightmost leaf descendant
    while (!node->is_leaf) {
        assert(node->num_keys >= 0);
        node = bptree_node_children(node, max_keys)[node->num_keys];
        assert(node != NULL);
    }
    assert(node->num_keys > 0);
    return bptree_node_keys(node)[node->num_keys - 1];
}

// Recursively counts nodes in the subtree
static inline int bptree_count_nodes(const bptree_node *node, const bptree *tree) {
    if (!node) return 0;
    if (node->is_leaf) return 1;

    int count = 1;  // Count this internal node
    bptree_node **children = bptree_node_children((bptree_node *)node, tree->max_keys);
    for (int i = 0; i <= node->num_keys; i++) {
        count += bptree_count_nodes(children[i], tree);
    }
    return count;
}

// Recursively checks B+ Tree invariants for a node and its subtree
// --- UPDATED VERSION ---
static inline bool bptree_check_invariants_node(bptree_node *node, const bptree *tree, int depth,
                                                int *leaf_depth) {
    if (!node) return false;  // NULL nodes are invalid in a well-formed tree

    bptree_key_t *keys = bptree_node_keys(node);
    bool is_root = (tree->root == node);

    // 1. Check Keys are Sorted
    for (int i = 1; i < node->num_keys; i++) {
        if (tree->compare(&keys[i - 1], &keys[i]) >= 0) {
            bptree_debug_print(tree->enable_debug, "Invariant Fail: Keys not sorted in node %p\n",
                               (void *)node);
            return false;  // Keys must be strictly increasing
        }
    }

    if (node->is_leaf) {
        // 2. Check Leaf Depth Consistency
        if (*leaf_depth == -1) {
            *leaf_depth = depth;  // Set expected depth based on first leaf found
        } else if (depth != *leaf_depth) {
            bptree_debug_print(tree->enable_debug,
                               "Invariant Fail: Leaf depth mismatch (%d != %d) for node %p\n",
                               depth, *leaf_depth, (void *)node);
            return false;  // All leaves must be at the same depth
        }

        // 3. Check Leaf Key Count Limits (for stable state)
        if (!is_root && (node->num_keys < tree->min_leaf_keys || node->num_keys > tree->max_keys)) {
            bptree_debug_print(
                tree->enable_debug,
                "Invariant Fail: Leaf node %p key count out of range [%d, %d] (%d keys)\n",
                (void *)node, tree->min_leaf_keys, tree->max_keys, node->num_keys);
            return false;
        }
        if (is_root && (node->num_keys > tree->max_keys && tree->count > 0)) {
            bptree_debug_print(tree->enable_debug,
                               "Invariant Fail: Root leaf node %p key count > max_keys (%d > %d)\n",
                               (void *)node, node->num_keys, tree->max_keys);
            return false;
        }
        if (is_root && tree->count == 0 && node->num_keys != 0) {
            bptree_debug_print(tree->enable_debug,
                               "Invariant Fail: Empty tree root leaf %p has keys (%d)\n",
                               (void *)node, node->num_keys);
            return false;
        }

        return true;  // Leaf node checks passed

    } else {  // Internal Node Checks
              // 4. Check Internal Node Key Count Limits (for stable state)
        if (!is_root &&
            (node->num_keys < tree->min_internal_keys || node->num_keys > tree->max_keys)) {
            bptree_debug_print(
                tree->enable_debug,
                "Invariant Fail: Internal node %p key count out of range [%d, %d] (%d keys)\n",
                (void *)node, tree->min_internal_keys, tree->max_keys, node->num_keys);
            return false;
        }
        // An internal root must have at least 1 key if the tree is not empty (tree->count > 0)
        // and at least 2 children. If count is 0, root should be a leaf.
        if (is_root && tree->count > 0 && node->num_keys < 1) {
            bptree_debug_print(
                tree->enable_debug,
                "Invariant Fail: Internal root node %p has < 1 key (%d keys) in non-empty tree\n",
                (void *)node, node->num_keys);
            return false;  // Internal root needs >= 1 key unless tree is empty (handled above)
        }
        if (is_root && node->num_keys > tree->max_keys) {
            bptree_debug_print(tree->enable_debug,
                               "Invariant Fail: Internal root node %p has > max_keys (%d > %d)\n",
                               (void *)node, node->num_keys, tree->max_keys);
            return false;
        }

        // 5. Check Children Exist and Relationships
        bptree_node **children = bptree_node_children(node, tree->max_keys);
        if (node->num_keys >= 0) {  // Should always have >= 0 keys unless completely empty root
            // Check leftmost child relative to first key
            if (!children[0]) {
                bptree_debug_print(tree->enable_debug,
                                   "Invariant Fail: Internal node %p missing child[0]\n",
                                   (void *)node);
                return false;
            }
            // All keys in leftmost child's subtree must be less than keys[0] (if keys[0] exists)
            if (node->num_keys > 0 && (children[0]->num_keys > 0 ||
                                       !children[0]->is_leaf)) {  // Check if child[0] has content
                bptree_key_t max_in_child0 = bptree_find_largest_key(children[0], tree->max_keys);
                if (tree->compare(&max_in_child0, &keys[0]) >= 0) {  // max(child0) must be < key[0]
                    // Use appropriate format specifier for your key type
                    bptree_debug_print(tree->enable_debug,
                                       "Invariant Fail: max(child[0]) >= key[0] in node %p -- "
                                       "MaxChild=%lld Key=%lld\n",
                                       (void *)node, (long long)max_in_child0, (long long)keys[0]);
                    return false;
                }
            }
            // Recursively check leftmost child
            if (!bptree_check_invariants_node(children[0], tree, depth + 1, leaf_depth))
                return false;

            // Check remaining children relative to keys
            for (int i = 1; i <= node->num_keys; i++) {
                if (!children[i]) {
                    bptree_debug_print(tree->enable_debug,
                                       "Invariant Fail: Internal node %p missing child[%d]\n",
                                       (void *)node, i);
                    return false;
                }

                // Check relationship: keys[i-1] <= min_key_in_subtree(children[i])
                if (children[i]->num_keys > 0 ||
                    !children[i]->is_leaf) {  // Only check if child has keys or is internal
                    bptree_key_t min_in_child =
                        bptree_find_smallest_key(children[i], tree->max_keys);

                    // --- MODIFIED INVARIANT CHECK ---
                    // Standard B+ Tree Invariant: Parent Key <= Smallest Key in Right Child Subtree
                    if (tree->compare(&keys[i - 1], &min_in_child) >
                        0) {  // Check if keys[i-1] > min_in_child
                        bptree_debug_print(
                            tree->enable_debug,
                            "Invariant Fail: key[%d] > min(child[%d]) -- Node Addr: %p, Node Keys: "
                            "%d, Parent Key[%d]: %lld, Found Min: %lld, Child[%d] Addr: %p\n",
                            i - 1, i, (void *)node, node->num_keys, i - 1, (long long)keys[i - 1],
                            (long long)min_in_child, i, (void *)children[i]);
                        return false;  // Failed: Separator key is larger than minimum in right
                                       // subtree
                    }
                    // --- END MODIFICATION ---

                    // Check right boundary: max key in child[i] must be < keys[i] if keys[i] exists
                    if (i < node->num_keys) {
                        bptree_key_t max_in_child =
                            bptree_find_largest_key(children[i], tree->max_keys);
                        if (tree->compare(&max_in_child, &keys[i]) >=
                            0) {  // max(child[i]) must be < key[i]
                            bptree_debug_print(tree->enable_debug,
                                               "Invariant Fail: max(child[%d]) >= key[%d] in node "
                                               "%p -- MaxChild=%lld Key=%lld\n",
                                               i, i, (void *)node, (long long)max_in_child,
                                               (long long)keys[i]);
                            return false;
                        }
                    }
                } else if (children[i]->is_leaf && children[i]->num_keys == 0 && tree->count > 0) {
                    // It's an error to point to an empty leaf unless the whole tree is empty
                    bptree_debug_print(tree->enable_debug,
                                       "Invariant Fail: Internal node %p points to empty leaf "
                                       "child[%d] %p in non-empty tree\n",
                                       (void *)node, i, (void *)children[i]);
                    return false;
                }

                // Recursively check child[i]
                if (!bptree_check_invariants_node(children[i], tree, depth + 1, leaf_depth))
                    return false;
            }
        } else {  // num_keys is < 0 - should not happen for internal nodes except perhaps during
                  // transient states? Stable state check:
            if (!is_root || tree->count > 0) {  // Only root of empty tree could conceptually have
                                                // <0, but root should be leaf then.
                bptree_debug_print(tree->enable_debug,
                                   "Invariant Fail: Internal node %p has < 0 keys (%d)\n",
                                   (void *)node, node->num_keys);
                return false;
            }
        }
        return true;  // Internal node checks passed
    }
}

/*
   Calculates the total size needed for a node, including header and data area.
   Ensures space for temporary overflow (+1 key, +1 value or +2 children).
*/
static inline size_t bptree_node_alloc_size(bptree *tree, bool is_leaf) {
    int max_keys = tree->max_keys;
    size_t keys_area_sz = bptree_keys_area_size(max_keys);  // Size including padding
    size_t data_payload_size;

    if (is_leaf) {
        // space for (max_keys+1) values
        data_payload_size = (size_t)(max_keys + 1) * sizeof(bptree_value_t);
    } else {
        // space for (max_keys+2) children pointers
        data_payload_size = (size_t)(max_keys + 2) * sizeof(bptree_node *);
    }

    size_t total_data_size = keys_area_sz + data_payload_size;
    return sizeof(bptree_node) + total_data_size;
}

// Allocates memory for a new node, ensuring proper alignment.
static inline bptree_node *bptree_node_alloc(bptree *tree, bool is_leaf) {
    // Determine the maximum alignment required among node header, keys, and values/pointers
    size_t max_align = alignof(bptree_node);
    max_align = BPTREE_MAX_ALIGN(max_align, alignof(bptree_key_t));
    if (is_leaf) {
        max_align = BPTREE_MAX_ALIGN(max_align, alignof(bptree_value_t));
    } else {
        max_align = BPTREE_MAX_ALIGN(max_align, alignof(bptree_node *));
    }

    size_t size = bptree_node_alloc_size(tree, is_leaf);
    // Round size up to the nearest multiple of max_align for aligned_alloc requirement
    size = (size + max_align - 1) & ~(max_align - 1);

    bptree_node *node = aligned_alloc(max_align, size);

    if (node) {
        // Initialize header fields
        node->is_leaf = is_leaf;
        node->num_keys = 0;
        node->next = NULL;  // Only relevant for leaves, NULL for internal nodes initially
        // Note: node->data area is uninitialized
    } else {
        bptree_debug_print(tree->enable_debug, "Node allocation failed (size: %zu, align: %zu)\n",
                           size, max_align);
    }
    return node;
}

// Frees a node and recursively frees its children if it's an internal node.
static void bptree_free_node(bptree_node *node, bptree *tree) {
    if (!node) return;

    if (!node->is_leaf) {
        // Recursively free children first
        bptree_node **children = bptree_node_children(node, tree->max_keys);
        for (int i = 0; i <= node->num_keys; i++) {
            bptree_free_node(children[i], tree);
        }
    }
    // Free the node itself using standard free (as allocated by aligned_alloc)
    free(node);
}

// Performs rebalancing (borrow or merge) upwards from a child node that became underfull.
// Performs rebalancing (borrow or merge) upwards from a child node that became underfull.
static void bptree_rebalance_up(bptree *tree, bptree_node **node_stack, int *index_stack,
                                int depth) {
    // Iterate from the parent of the underfull node up towards the root
    for (int d = depth - 1; d >= 0; d--) {
        bptree_node *parent = node_stack[d];
        int child_idx =
            index_stack[d];  // Index of the pointer TO the child (potentially underfull)
        bptree_node **children = bptree_node_children(parent, tree->max_keys);
        bptree_node *child = children[child_idx];  // The potentially underfull node

        // Determine minimum required keys for the child node
        int min_keys = child->is_leaf ? tree->min_leaf_keys : tree->min_internal_keys;

        // If the child meets the minimum key requirement, no more rebalancing needed at this level
        // or above
        if (child->num_keys >= min_keys) {
            bptree_debug_print(tree->enable_debug,
                               "Rebalance unnecessary at depth %d, child %d has %d keys (min %d)\n",
                               d, child_idx, child->num_keys, min_keys);
            break;
        }
        bptree_debug_print(tree->enable_debug,
                           "Rebalance needed at depth %d for child %d (%d keys < min %d)\n", d,
                           child_idx, child->num_keys, min_keys);

        // Attempt to borrow from the left sibling
        if (child_idx > 0) {  // Check if a left sibling exists
            bptree_node *left_sibling = children[child_idx - 1];
            int left_min = left_sibling->is_leaf ? tree->min_leaf_keys : tree->min_internal_keys;

            // Check if the left sibling has more than the minimum number of keys
            if (left_sibling->num_keys > left_min) {
                // *** BORROW FROM LEFT LOGIC - IMPLEMENTED ***
                bptree_debug_print(tree->enable_debug,
                                   "Attempting borrow from left sibling (idx %d)\n", child_idx - 1);
                bptree_key_t *parent_keys = bptree_node_keys(parent);

                if (child->is_leaf) {
                    bptree_key_t *child_keys = bptree_node_keys(child);
                    bptree_value_t *child_vals = bptree_node_values(child, tree->max_keys);
                    bptree_key_t *left_keys = bptree_node_keys(left_sibling);
                    bptree_value_t *left_vals = bptree_node_values(left_sibling, tree->max_keys);

                    // Make space at the beginning of the child node
                    memmove(&child_keys[1], &child_keys[0], child->num_keys * sizeof(bptree_key_t));
                    memmove(&child_vals[1], &child_vals[0],
                            child->num_keys * sizeof(bptree_value_t));

                    // Copy the largest key/value from the left sibling to the child
                    child_keys[0] = left_keys[left_sibling->num_keys - 1];
                    child_vals[0] = left_vals[left_sibling->num_keys - 1];

                    // Update counts
                    child->num_keys++;
                    left_sibling->num_keys--;

                    // Update the parent separator key to the new smallest key in the child
                    parent_keys[child_idx - 1] = child_keys[0];

                    bptree_debug_print(tree->enable_debug,
                                       "Borrowed leaf key from left. Parent key updated.\n");
                    break;  // Rebalancing successful at this level
                } else {    // Internal Node Borrow from Left
                    bptree_key_t *child_keys = bptree_node_keys(child);
                    bptree_node **child_children = bptree_node_children(child, tree->max_keys);
                    bptree_key_t *left_keys = bptree_node_keys(left_sibling);
                    bptree_node **left_children =
                        bptree_node_children(left_sibling, tree->max_keys);

                    // Make space at the beginning of the child node for key and child pointer
                    memmove(&child_keys[1], &child_keys[0], child->num_keys * sizeof(bptree_key_t));
                    memmove(&child_children[1], &child_children[0],
                            (child->num_keys + 1) * sizeof(bptree_node *));

                    // Copy the separator key from the parent down to the child
                    child_keys[0] = parent_keys[child_idx - 1];

                    // Copy the rightmost child pointer from the left sibling to the child
                    child_children[0] = left_children[left_sibling->num_keys];

                    // Copy the largest key from the left sibling up to the parent separator
                    parent_keys[child_idx - 1] = left_keys[left_sibling->num_keys - 1];

                    // Update counts
                    child->num_keys++;
                    left_sibling->num_keys--;

                    bptree_debug_print(
                        tree->enable_debug,
                        "Borrowed internal key/child from left. Parent key updated.\n");
                    break;  // Rebalancing successful at this level
                }
            }
        }

        // Attempt to borrow from the right sibling
        // Check child_idx against parent->num_keys (NOT parent->num_keys+1) because
        // the separator key index goes up to parent->num_keys-1, and child indices go up to
        // parent->num_keys. So, right sibling exists if child_idx < parent->num_keys.
        if (child_idx < parent->num_keys) {  // Check if a right sibling exists
            bptree_node *right_sibling = children[child_idx + 1];
            int right_min = right_sibling->is_leaf ? tree->min_leaf_keys : tree->min_internal_keys;

            // Check if the right sibling has more than the minimum number of keys
            if (right_sibling->num_keys > right_min) {
                // *** BORROW FROM RIGHT LOGIC - IMPLEMENTED ***
                bptree_debug_print(tree->enable_debug,
                                   "Attempting borrow from right sibling (idx %d)\n",
                                   child_idx + 1);
                bptree_key_t *parent_keys = bptree_node_keys(parent);

                if (child->is_leaf) {
                    bptree_key_t *child_keys = bptree_node_keys(child);
                    bptree_value_t *child_vals = bptree_node_values(child, tree->max_keys);
                    bptree_key_t *right_keys = bptree_node_keys(right_sibling);
                    bptree_value_t *right_vals = bptree_node_values(right_sibling, tree->max_keys);

                    // Copy the smallest key/value from the right sibling to the end of the child
                    child_keys[child->num_keys] = right_keys[0];
                    child_vals[child->num_keys] = right_vals[0];

                    // Update counts
                    child->num_keys++;
                    right_sibling->num_keys--;

                    // Shift keys/values in the right sibling left
                    memmove(&right_keys[0], &right_keys[1],
                            right_sibling->num_keys * sizeof(bptree_key_t));
                    memmove(&right_vals[0], &right_vals[1],
                            right_sibling->num_keys * sizeof(bptree_value_t));

                    // Update the parent separator key to the new smallest key in the right sibling
                    parent_keys[child_idx] = right_keys[0];

                    bptree_debug_print(tree->enable_debug,
                                       "Borrowed leaf key from right. Parent key updated.\n");
                    break;  // Rebalancing successful at this level
                } else {    // Internal Node Borrow from Right
                    bptree_key_t *child_keys = bptree_node_keys(child);
                    bptree_node **child_children = bptree_node_children(child, tree->max_keys);
                    bptree_key_t *right_keys = bptree_node_keys(right_sibling);
                    bptree_node **right_children =
                        bptree_node_children(right_sibling, tree->max_keys);

                    // Copy the separator key from the parent down to the end of the child's keys
                    child_keys[child->num_keys] = parent_keys[child_idx];

                    // Copy the leftmost child pointer from the right sibling to the end of child's
                    // children
                    child_children[child->num_keys + 1] = right_children[0];

                    // Copy the smallest key from the right sibling up to the parent separator
                    parent_keys[child_idx] = right_keys[0];

                    // Update counts
                    child->num_keys++;
                    right_sibling->num_keys--;

                    // Shift keys/children in the right sibling left
                    memmove(&right_keys[0], &right_keys[1],
                            right_sibling->num_keys * sizeof(bptree_key_t));
                    memmove(&right_children[0], &right_children[1],
                            (right_sibling->num_keys + 1) * sizeof(bptree_node *));

                    bptree_debug_print(
                        tree->enable_debug,
                        "Borrowed internal key/child from right. Parent key updated.\n");
                    break;  // Rebalancing successful at this level
                }
            }
        }

        // If borrowing failed, attempt to merge
        bptree_debug_print(tree->enable_debug, "Borrow failed, attempting merge\n");
        if (child_idx > 0) {
            // *** MERGE CHILD WITH LEFT SIBLING ***
            bptree_node *left_sibling = children[child_idx - 1];
            bptree_debug_print(tree->enable_debug, "Merging child %d into left sibling %d\n",
                               child_idx, child_idx - 1);

            if (child->is_leaf) {
                bptree_key_t *left_keys = bptree_node_keys(left_sibling);
                bptree_value_t *left_vals = bptree_node_values(left_sibling, tree->max_keys);
                bptree_key_t *child_keys = bptree_node_keys(child);
                bptree_value_t *child_vals = bptree_node_values(child, tree->max_keys);

                // --- BOUNDS CHECK ---
                int combined_keys = left_sibling->num_keys + child->num_keys;
                if (combined_keys >
                    tree->max_keys) {  // Check against max_keys; buffer has max_keys+1
                    fprintf(stderr,
                            "[BPTree FATAL] Merge-Left (Leaf) Buffer Overflow PREVENTED! "
                            "Combined keys %d > max_keys %d. Invariants likely violated.\n",
                            combined_keys, tree->max_keys);
                    abort();  // Abort as this indicates a serious logic error
                }
                // --- END CHECK ---

                // Copy all keys/values from child to end of left sibling
                memcpy(left_keys + left_sibling->num_keys, child_keys,
                       child->num_keys * sizeof(bptree_key_t));
                memcpy(left_vals + left_sibling->num_keys, child_vals,
                       child->num_keys * sizeof(bptree_value_t));
                left_sibling->num_keys = combined_keys;  // Update count *after* copy

                // Update sibling pointer and free merged node
                left_sibling->next = child->next;
                free(child);
                children[child_idx] = NULL;  // Avoid dangling pointer
            } else {                         // Internal node merge
                bptree_key_t *left_keys = bptree_node_keys(left_sibling);
                bptree_node **left_children = bptree_node_children(left_sibling, tree->max_keys);
                bptree_key_t *child_keys = bptree_node_keys(child);
                bptree_node **child_children = bptree_node_children(child, tree->max_keys);
                bptree_key_t *parent_keys = bptree_node_keys(parent);

                // --- BOUNDS CHECK ---
                // Need space for left_keys + separator_key + child_keys
                int combined_keys = left_sibling->num_keys + 1 + child->num_keys;
                // Need space for left_children + child_children
                int combined_children = (left_sibling->num_keys + 1) + (child->num_keys + 1);
                // Use max_keys+1 as check limit for keys buffer, max_keys+2 for children buffer
                if (combined_keys > tree->max_keys + 1) {
                    fprintf(stderr,
                            "[BPTree FATAL] Merge-Left (Internal) Key Buffer Overflow PREVENTED! "
                            "Combined keys %d > buffer %d. Invariants likely violated.\n",
                            combined_keys, tree->max_keys + 1);
                    abort();
                }
                if (combined_children > tree->max_keys + 2) {
                    fprintf(
                        stderr,
                        "[BPTree FATAL] Merge-Left (Internal) Children Buffer Overflow PREVENTED! "
                        "Combined children %d > buffer %d. Invariants likely violated.\n",
                        combined_children, tree->max_keys + 2);
                    abort();
                }
                // --- END CHECK ---

                // Pull separator key from parent down into left sibling's key array
                left_keys[left_sibling->num_keys] = parent_keys[child_idx - 1];

                // Copy keys from child node into left sibling (after separator)
                memcpy(left_keys + left_sibling->num_keys + 1, child_keys,
                       child->num_keys * sizeof(bptree_key_t));

                // Copy child pointers from child node into left sibling
                memcpy(left_children + left_sibling->num_keys + 1, child_children,
                       (child->num_keys + 1) * sizeof(bptree_node *));

                // Update left sibling's key count
                left_sibling->num_keys = combined_keys;

                // Free merged node
                free(child);
                children[child_idx] = NULL;  // Avoid dangling pointer
            }

            // Remove separator key and pointer to merged child from parent
            bptree_key_t *parent_keys = bptree_node_keys(parent);
            // Shift keys left starting from the removed separator's position
            memmove(&parent_keys[child_idx - 1], &parent_keys[child_idx],
                    (parent->num_keys - child_idx) * sizeof(bptree_key_t));
            // Shift child pointers left starting from the removed child's position
            memmove(&children[child_idx], &children[child_idx + 1],
                    (parent->num_keys - child_idx) * sizeof(bptree_node *));
            parent->num_keys--;
            bptree_debug_print(tree->enable_debug, "Merge with left complete. Parent updated.\n");

            // Continue rebalancing up the tree from the parent (now potentially underfull)
        } else {
            // *** MERGE CHILD WITH RIGHT SIBLING *** (child is the leftmost node at this level,
            // child_idx == 0)
            bptree_node *right_sibling = children[child_idx + 1];  // == children[1]
            bptree_debug_print(tree->enable_debug, "Merging right sibling %d into child %d\n",
                               child_idx + 1, child_idx);

            if (child->is_leaf) {
                bptree_key_t *child_keys = bptree_node_keys(child);
                bptree_value_t *child_vals = bptree_node_values(child, tree->max_keys);
                bptree_key_t *right_keys = bptree_node_keys(right_sibling);
                bptree_value_t *right_vals = bptree_node_values(right_sibling, tree->max_keys);

                // --- BOUNDS CHECK ---
                int combined_keys = child->num_keys + right_sibling->num_keys;
                if (combined_keys >
                    tree->max_keys) {  // Check against max_keys; buffer has max_keys+1
                    fprintf(stderr,
                            "[BPTree FATAL] Merge-Right (Leaf) Buffer Overflow PREVENTED! "
                            "Combined keys %d > max_keys %d. Invariants likely violated.\n",
                            combined_keys, tree->max_keys);
                    abort();
                }
                // --- END CHECK ---

                // Copy all keys/values from right sibling to end of child
                memcpy(child_keys + child->num_keys, right_keys,
                       right_sibling->num_keys * sizeof(bptree_key_t));
                memcpy(child_vals + child->num_keys, right_vals,
                       right_sibling->num_keys * sizeof(bptree_value_t));
                child->num_keys = combined_keys;  // Update count

                // Update sibling pointer and free merged node
                child->next = right_sibling->next;
                free(right_sibling);
                children[child_idx + 1] = NULL;
            } else {  // Internal node merge
                bptree_key_t *child_keys = bptree_node_keys(child);
                bptree_node **child_children = bptree_node_children(child, tree->max_keys);
                bptree_key_t *right_keys = bptree_node_keys(right_sibling);
                bptree_node **right_children = bptree_node_children(right_sibling, tree->max_keys);
                bptree_key_t *parent_keys = bptree_node_keys(parent);

                // --- BOUNDS CHECK ---
                // Need space for child_keys + separator_key + right_keys
                int combined_keys = child->num_keys + 1 + right_sibling->num_keys;
                // Need space for child_children + right_children
                int combined_children = (child->num_keys + 1) + (right_sibling->num_keys + 1);
                // Use max_keys+1 as check limit for keys buffer, max_keys+2 for children buffer
                if (combined_keys > tree->max_keys + 1) {
                    fprintf(stderr,
                            "[BPTree FATAL] Merge-Right (Internal) Key Buffer Overflow PREVENTED! "
                            "Combined keys %d > buffer %d. Invariants likely violated.\n",
                            combined_keys, tree->max_keys + 1);
                    abort();
                }
                if (combined_children > tree->max_keys + 2) {
                    fprintf(
                        stderr,
                        "[BPTree FATAL] Merge-Right (Internal) Children Buffer Overflow PREVENTED! "
                        "Combined children %d > buffer %d. Invariants likely violated.\n",
                        combined_children, tree->max_keys + 2);
                    abort();
                }
                // --- END CHECK ---

                // Pull separator key from parent down into child's key array
                child_keys[child->num_keys] = parent_keys[child_idx];  // child_idx is 0 here

                // Copy keys from right sibling into child (after separator)
                memcpy(child_keys + child->num_keys + 1, right_keys,
                       right_sibling->num_keys * sizeof(bptree_key_t));

                // Copy child pointers from right sibling into child
                memcpy(child_children + child->num_keys + 1, right_children,
                       (right_sibling->num_keys + 1) * sizeof(bptree_node *));

                // Update child's key count
                child->num_keys = combined_keys;

                // Free merged node
                free(right_sibling);
                children[child_idx + 1] = NULL;
            }

            // Remove separator key and pointer to merged node from parent
            bptree_key_t *parent_keys = bptree_node_keys(parent);
            // Shift keys left starting from the key *after* the removed separator
            memmove(&parent_keys[child_idx], &parent_keys[child_idx + 1],
                    (parent->num_keys - child_idx - 1) * sizeof(bptree_key_t));
            // Shift child pointers left starting from the position *after* the merged node's ptr
            memmove(&children[child_idx + 1], &children[child_idx + 2],
                    (parent->num_keys - child_idx - 1) * sizeof(bptree_node *));
            parent->num_keys--;
            bptree_debug_print(tree->enable_debug, "Merge with right complete. Parent updated.\n");

            // Continue rebalancing up the tree from the parent
        }
        // The loop continues to check the parent node (node_stack[d]) at the next iteration
    }

    // Special case: Root node might need shrinking after rebalancing
    // Check if root is internal, has 0 keys, *and* the tree is not empty (count > 0 implies
    // the single child must exist)
    if (!tree->root->is_leaf && tree->root->num_keys == 0 && tree->count > 0) {
        bptree_debug_print(tree->enable_debug,
                           "Root node is internal and empty, shrinking height.\n");
        bptree_node *old_root = tree->root;
        // The only child (at index 0) becomes the new root
        tree->root = bptree_node_children(old_root, tree->max_keys)[0];
        tree->height--;
        free(old_root);
    } else if (tree->count == 0 && tree->root && tree->root->num_keys != 0) {
        // If the last element was deleted, the root (which must be a leaf) should be empty
        bptree_debug_print(tree->enable_debug, "Tree empty, ensuring root node is empty.\n");
        tree->root->num_keys = 0;  // Clear keys in the existing root leaf
    }
}  // End of bptree_rebalance_up

/* --- Binary search within a node ---
   Leaf node: Returns index `i` such that `key <= keys[i]`. Finds exact match or insertion point.
   Internal node: Returns index `i` such that `keys[i-1] <= key < keys[i]`. Finds correct child ptr
index.
----------------------------------------------------------------------------- */
static inline int bptree_node_search(const bptree *tree, bptree_node *node,
                                     const bptree_key_t *key) {
    int low = 0, high = node->num_keys;
    bptree_key_t *keys = bptree_node_keys(node);

    if (node->is_leaf) {
        // Standard binary search (lower bound) to find first element >= key
        while (low < high) {
            int mid = low + (high - low) / 2;  // Avoid overflow
            int cmp = tree->compare(key, &keys[mid]);
            if (cmp <= 0) {  // key <= keys[mid] -> search left half (inclusive)
                high = mid;
            } else {  // key > keys[mid] -> search right half (exclusive)
                low = mid + 1;
            }
        }
    } else {  // Internal Node
        // Binary search to find the correct child pointer index `i`
        // We want the first key `keys[mid]` such that `key < keys[mid]`
        // or index `num_keys` if `key >= keys[num_keys - 1]`
        while (low < high) {
            int mid = low + (high - low) / 2;
            int cmp = tree->compare(key, &keys[mid]);
            if (cmp < 0) {  // key < keys[mid] -> search left half (inclusive)
                high = mid;
            } else {  // key >= keys[mid] -> search right half (exclusive)
                low = mid + 1;
            }
        }
    }
    // `low` is the result:
    // Leaf: index of first key >= target, or num_keys if target > all keys.
    // Internal: index of child pointer to follow.
    return low;
}

/* --- Internal Recursive Insert ---
   Descends the tree to the appropriate leaf.
   Inserts the key/value pair.
   Handles node splitting and key promotion on the way back up.
   NOTE: This updated version does NOT modify tree->count directly.
------------------------------------------------------------------------------------------ */
static inline bptree_status bptree_insert_internal(
    bptree *tree, bptree_node *node, const bptree_key_t *key, bptree_value_t value,
    bptree_key_t *promoted_key,  // Out param for promoted key
    bptree_node **new_child)     // Out param for new node after split
{
    // Find the appropriate position or child to descend into
    int pos = bptree_node_search(tree, node, key);

    if (node->is_leaf) {
        bptree_key_t *keys = bptree_node_keys(node);
        bptree_value_t *values = bptree_node_values(node, tree->max_keys);

        // Check for duplicate key
        if (pos < node->num_keys && tree->compare(key, &keys[pos]) == 0) {
            bptree_debug_print(tree->enable_debug, "Insert failed: Duplicate key found.\n");
            return BPTREE_DUPLICATE_KEY;
        }

        // Shift keys and values to make room for the new entry
        memmove(&keys[pos + 1], &keys[pos], (node->num_keys - pos) * sizeof(bptree_key_t));
        memmove(&values[pos + 1], &values[pos], (node->num_keys - pos) * sizeof(bptree_value_t));

        // Insert the new key and value
        keys[pos] = *key;
        values[pos] = value;
        node->num_keys++;
        // tree->count++; // <<< REMOVED THIS LINE - Count handled by caller (bptree_put)
        *new_child = NULL;  // No new node created yet

        bptree_debug_print(tree->enable_debug, "Inserted key in leaf. Node keys: %d\n",
                           node->num_keys);

        // Check if the leaf node needs to be split (overflow)
        if (node->num_keys > tree->max_keys) {
            bptree_debug_print(tree->enable_debug, "Leaf node overflow (%d > %d), splitting.\n",
                               node->num_keys, tree->max_keys);
            int total_keys = node->num_keys;  // == max_keys + 1
            // Split point: ceil(total_keys / 2.0), which is (total_keys + 1) / 2 for integers
            int split_idx = (total_keys + 1) / 2;
            int new_node_keys = total_keys - split_idx;  // Number of keys moving to the new node

            // Allocate the new right sibling leaf node
            bptree_node *new_leaf = bptree_node_alloc(tree, true);
            if (!new_leaf) {
                // If allocation fails here, the key was inserted but node is inconsistent.
                // Caller (bptree_put) needs to handle this status without incrementing count.
                // Revert num_keys? Hard to recover state perfectly here.
                // Best to return failure status and let caller handle count consistency.
                node->num_keys--;  // Attempt to revert local node change before returning failure
                // Need to shift elements back? More complex recovery needed if strict consistency
                // required. Simplest is to return failure; the tree is inconsistent until
                // fixed/rebuilt. For now, just return failure.
                bptree_debug_print(tree->enable_debug, "Leaf split allocation failed!\n");
                return BPTREE_ALLOCATION_FAILURE;
            }

            bptree_key_t *new_keys = bptree_node_keys(new_leaf);
            bptree_value_t *new_values = bptree_node_values(new_leaf, tree->max_keys);

            // Copy the second half of keys/values to the new node
            memcpy(new_keys, &keys[split_idx], new_node_keys * sizeof(bptree_key_t));
            memcpy(new_values, &values[split_idx], new_node_keys * sizeof(bptree_value_t));
            new_leaf->num_keys = new_node_keys;

            // Update the original node's key count
            node->num_keys = split_idx;

            // Link the new leaf into the sibling list
            new_leaf->next = node->next;
            node->next = new_leaf;

            // Promote the smallest key of the new node to the parent
            *promoted_key = new_keys[0];
            *new_child = new_leaf;  // Return the newly created node

            bptree_debug_print(tree->enable_debug,
                               "Leaf split complete. Promoted key. Left keys: %d, Right keys: %d\n",
                               node->num_keys, new_leaf->num_keys);
        }
        // If successful insertion (and maybe split), return OK.
        // Count will be incremented by the caller.
        return BPTREE_OK;

    } else {  // Internal Node: Descend recursively
        bptree_node **children = bptree_node_children(node, tree->max_keys);
        bptree_key_t child_promoted_key;     // To receive key promoted from child split
        bptree_node *child_new_node = NULL;  // To receive new node from child split

        // Recursively insert into the appropriate child
        bptree_status status = bptree_insert_internal(tree, children[pos], key, value,
                                                      &child_promoted_key, &child_new_node);

        // If recursive call resulted in error or no split, propagate status up
        if (status != BPTREE_OK || child_new_node == NULL) {
            return status;  // Propagate error or OK status if no split occurred below
        }

        // Child was split, need to insert promoted key and new child pointer into this node
        bptree_debug_print(tree->enable_debug,
                           "Child split propagated. Inserting promoted key into internal node.\n");
        bptree_key_t *keys = bptree_node_keys(node);

        // Shift keys and children pointers to make room
        memmove(&keys[pos + 1], &keys[pos], (node->num_keys - pos) * sizeof(bptree_key_t));
        memmove(&children[pos + 2], &children[pos + 1],
                (node->num_keys - pos) *
                    sizeof(bptree_node *));  // Shift num_keys-pos pointers starting AFTER pos+1

        // Insert the promoted key and the new child pointer
        keys[pos] = child_promoted_key;
        children[pos + 1] = child_new_node;
        node->num_keys++;

        bptree_debug_print(tree->enable_debug, "Internal node keys: %d\n", node->num_keys);

        // Check if this internal node now needs to be split
        if (node->num_keys > tree->max_keys) {
            bptree_debug_print(tree->enable_debug, "Internal node overflow (%d > %d), splitting.\n",
                               node->num_keys, tree->max_keys);
            int total_keys = node->num_keys;  // == max_keys + 1
            // Split point for internal node: middle key is promoted, not copied.
            // Key at index `split_idx` is promoted.
            int split_idx =
                total_keys / 2;  // e.g., max_keys=4, total=5, split_idx=2. keys[2] promoted.
            int new_node_keys =
                total_keys - split_idx - 1;  // Keys to the right of the promoted key

            // Allocate the new right sibling internal node
            bptree_node *new_internal = bptree_node_alloc(tree, false);
            if (!new_internal) {
                // If allocation fails here, the promoted key/child were added but node is
                // inconsistent. Revert num_keys? Revert memmoves? Difficult. Return failure status.
                // Caller handles count.
                node->num_keys--;  // Attempt to revert local node change
                // Tree state potentially inconsistent.
                bptree_debug_print(tree->enable_debug, "Internal split allocation failed!\n");
                return BPTREE_ALLOCATION_FAILURE;  // Propagate allocation failure
            }

            bptree_key_t *new_keys = bptree_node_keys(new_internal);
            bptree_node **new_children = bptree_node_children(new_internal, tree->max_keys);

            // Key to be promoted up
            *promoted_key = keys[split_idx];
            *new_child = new_internal;  // Return the newly created node

            // Copy keys and children pointers to the new node
            memcpy(new_keys, &keys[split_idx + 1], new_node_keys * sizeof(bptree_key_t));
            // Children from index split_idx+1 onwards go to the new node
            memcpy(new_children, &children[split_idx + 1],
                   (new_node_keys + 1) * sizeof(bptree_node *));
            new_internal->num_keys = new_node_keys;

            // Update the original node's key count
            node->num_keys = split_idx;

            bptree_debug_print(
                tree->enable_debug,
                "Internal split complete. Promoted key. Left keys: %d, Right keys: %d\n",
                node->num_keys, new_internal->num_keys);

        } else {
            // Internal node did not overflow, clear new_child output param as no split occurred *at
            // this level*
            *new_child = NULL;
        }
        // If split occurred at this level or below, return OK.
        // If split occurred below but not here, *new_child is NULL, return OK.
        return BPTREE_OK;  // Insertion successful at this level (split handled or not needed)
    }
}

// Public API: Inserts a key-value pair into the tree.
// --- UPDATED VERSION ---
BPTREE_API bptree_status bptree_put(bptree *tree, const bptree_key_t *key, bptree_value_t value) {
    if (!tree || !key) return BPTREE_INVALID_ARGUMENT;
    if (!tree->root) return BPTREE_INTERNAL_ERROR;  // Should always have a root

    bptree_key_t promoted_key;     // Receives key if root splits
    bptree_node *new_node = NULL;  // Receives new node if root splits

    // Start insertion from the root
    // NOTE: Assumes bptree_insert_internal NO LONGER increments tree->count.
    bptree_status status =
        bptree_insert_internal(tree, tree->root, key, value, &promoted_key, &new_node);

    if (status == BPTREE_OK) {
        // If insert_internal succeeded, handle potential root split
        if (new_node != NULL) {  // Root split occurred
            bptree_debug_print(tree->enable_debug, "Root split occurred. Creating new root.\n");
            // Create a new root node
            bptree_node *new_root = bptree_node_alloc(tree, false);  // New root is internal
            if (!new_root) {
                // Failed to allocate new root. The insertion conceptually failed overall.
                // Tree state might be less consistent here, but count was not yet incremented.
                bptree_free_node(new_node, tree);  // Free the orphaned node from the split
                // Do NOT increment count. Return the allocation failure.
                return BPTREE_ALLOCATION_FAILURE;
            }

            // Set up the new root
            bptree_key_t *root_keys = bptree_node_keys(new_root);
            bptree_node **root_children = bptree_node_children(new_root, tree->max_keys);

            root_keys[0] = promoted_key;    // The single key is the one promoted from the split
            root_children[0] = tree->root;  // Old root is the left child
            root_children[1] = new_node;    // New node from split is the right child
            new_root->num_keys = 1;

            // Update tree metadata
            tree->root = new_root;
            tree->height++;
            bptree_debug_print(tree->enable_debug, "New root created. Tree height: %d\n",
                               tree->height);
        }
        // --------->>> CORE FIX <<<---------
        // Increment tree count ONLY after insert_internal succeeded AND
        // any potential root split allocation/setup also succeeded.
        tree->count++;
        // ---------------------------------

    } else {
        // If insert_internal failed (e.g., duplicate key, allocation failure lower down),
        // do not increment count. Just return the status.
        bptree_debug_print(tree->enable_debug,
                           "Insertion failed (Status: %d), count not incremented.\n", status);
    }

    return status;  // Return the final status of the operation
}

// Public API: Retrieves the value associated with a key.
BPTREE_API bptree_status bptree_get(const bptree *tree, const bptree_key_t *key,
                                    bptree_value_t *out_value) {
    if (!tree || !tree->root || !key || !out_value) return BPTREE_INVALID_ARGUMENT;
    if (tree->count == 0) return BPTREE_KEY_NOT_FOUND;

    bptree_node *node = tree->root;

    // Traverse down the tree following the appropriate paths
    while (!node->is_leaf) {
        int pos = bptree_node_search(tree, node, key);
        node = bptree_node_children(node, tree->max_keys)[pos];
        if (!node) return BPTREE_INTERNAL_ERROR;  // Should not happen in valid tree
    }

    // Search within the leaf node
    int pos = bptree_node_search(tree, node, key);
    bptree_key_t *keys = bptree_node_keys(node);

    // Check if the key was found at the determined position
    if (pos < node->num_keys && tree->compare(key, &keys[pos]) == 0) {
        *out_value = bptree_node_values(node, tree->max_keys)[pos];
        return BPTREE_OK;
    }

    return BPTREE_KEY_NOT_FOUND;  // Key not found in the leaf
}

// Public API: Removes a key-value pair from the tree.
// --- UPDATED VERSION ---
BPTREE_API bptree_status bptree_remove(bptree *tree, const bptree_key_t *key) {
#define BPTREE_MAX_HEIGHT_REMOVE 64  // Static maximum height for stack allocation
    bptree_node *node_stack[BPTREE_MAX_HEIGHT_REMOVE];
    int index_stack[BPTREE_MAX_HEIGHT_REMOVE];  // Stores index of child pointer taken at each level
    int depth = 0;

    if (!tree || !tree->root || !key) return BPTREE_INVALID_ARGUMENT;
    if (tree->count == 0) return BPTREE_KEY_NOT_FOUND;  // Cannot remove from empty tree

    bptree_node *node = tree->root;

    // 1. Traverse down to the leaf node containing the key, recording the path
    while (!node->is_leaf) {
        if (depth >= BPTREE_MAX_HEIGHT_REMOVE) {
            // ... (handle height limit error) ...
            return BPTREE_INTERNAL_ERROR;
        }
        int pos = bptree_node_search(tree, node, key);
        node_stack[depth] = node;
        index_stack[depth] = pos;  // Record index of child pointer followed
        depth++;
        node = bptree_node_children(node, tree->max_keys)[pos];
        if (!node) return BPTREE_INTERNAL_ERROR;
    }

    // 2. Search for the key within the leaf node
    int pos = bptree_node_search(tree, node, key);
    bptree_key_t *keys = bptree_node_keys(node);

    // 3. Check if key exists in the leaf
    if (pos >= node->num_keys || tree->compare(key, &keys[pos]) != 0) {
        return BPTREE_KEY_NOT_FOUND;
    }

    // --- Store the key being deleted for later comparison ---
    bptree_key_t deleted_key_copy = keys[pos];
    // -------------------------------------------------------

    // 4. Remove the key and value from the leaf node
    bptree_value_t *values = bptree_node_values(node, tree->max_keys);
    memmove(&keys[pos], &keys[pos + 1], (node->num_keys - pos - 1) * sizeof(bptree_key_t));
    memmove(&values[pos], &values[pos + 1], (node->num_keys - pos - 1) * sizeof(bptree_value_t));
    node->num_keys--;
    tree->count--;

    bptree_debug_print(tree->enable_debug, "Removed key from leaf. Node keys: %d, Tree count: %d\n",
                       node->num_keys, tree->count);

    // --- FIX: Update Parent Separator if Smallest Key Deleted ---
    // If the deleted key was the smallest in the leaf (pos == 0) AND it's not the root's leaf
    if (pos == 0 && depth > 0 && node->num_keys > 0) {
        int parent_child_idx = index_stack[depth - 1];  // Index of this leaf in parent's children
        // The separator key in the parent is at index parent_child_idx - 1
        if (parent_child_idx > 0) {
            int separator_idx = parent_child_idx - 1;
            bptree_node *parent = node_stack[depth - 1];
            bptree_key_t *parent_keys = bptree_node_keys(parent);

            // Check if the parent separator matched the key we just deleted
            if (separator_idx < parent->num_keys &&
                tree->compare(&parent_keys[separator_idx], &deleted_key_copy) == 0) {
                bptree_debug_print(
                    tree->enable_debug,
                    "Updating parent separator key [%d] after deleting smallest leaf key.\n",
                    separator_idx);
                // Update the parent separator to the new smallest key in the leaf
                parent_keys[separator_idx] = keys[0];
            }
        }
        // If parent_child_idx == 0, the deleted key was the smallest overall in the leftmost
        // leaf, which doesn't correspond to a separator key in the parent in the same way.
        // The invariant check comparing max(child[0]) < key[0] handles this side.
    }
    // ---------------------------------------------------------

    // 5. Rebalance the tree if necessary, starting from the affected leaf's parent
    bool root_is_leaf = (depth == 0);
    if (!root_is_leaf && node->num_keys < tree->min_leaf_keys) {
        bptree_debug_print(tree->enable_debug, "Leaf underflow (%d < %d), starting rebalance.\n",
                           node->num_keys, tree->min_leaf_keys);
        bptree_rebalance_up(tree, node_stack, index_stack, depth);
    } else if (root_is_leaf && tree->count == 0) {
        assert(tree->root == node);
        assert(tree->root->num_keys == 0);
        bptree_debug_print(tree->enable_debug, "Last key removed, root is empty leaf.\n");
    }
    // Note: The rebalance function *also* needs to correctly update parent keys
    // during borrow/merge, which it appears to do based on prior analysis.

#undef BPTREE_MAX_HEIGHT_REMOVE
    return BPTREE_OK;
}

// Public API: Retrieves values for keys within a specified range [start, end].
BPTREE_API bptree_status bptree_get_range(const bptree *tree, const bptree_key_t *start,
                                          const bptree_key_t *end, bptree_value_t **out_values,
                                          int *n_results) {
    if (!tree || !tree->root || !start || !end || !out_values || !n_results) {
        return BPTREE_INVALID_ARGUMENT;
    }

    *out_values = NULL;
    *n_results = 0;

    // Ensure start key is not greater than end key
    if (tree->compare(start, end) > 0) {
        return BPTREE_INVALID_ARGUMENT;  // Or return OK with 0 results? API decision.
    }
    if (tree->count == 0) {
        return BPTREE_OK;  // Empty tree, 0 results
    }

    // 1. Find the first leaf node that *could* contain the start key or keys greater than it
    bptree_node *node = tree->root;
    while (!node->is_leaf) {
        int pos = bptree_node_search(tree, node, start);
        node = bptree_node_children(node, tree->max_keys)[pos];
        if (!node) return BPTREE_INTERNAL_ERROR;
    }

    // `node` is now the starting leaf node.

    // --- Inefficiency Note ---
    // The current implementation uses two passes: one to count, one to collect.
    // A single pass using dynamic allocation (e.g., realloc) would be more efficient
    // but adds complexity. For simplicity, two passes are kept here.

    // 2. First Pass: Count the number of results in the range
    int count = 0;
    bptree_node *current_node = node;
    bool past_end = false;
    while (current_node && !past_end) {
        bptree_key_t *keys = bptree_node_keys(current_node);
        for (int i = 0; i < current_node->num_keys; i++) {
            // Check if key >= start
            if (tree->compare(&keys[i], start) >= 0) {
                // Check if key <= end
                if (tree->compare(&keys[i], end) <= 0) {
                    count++;
                } else {
                    // Key > end, no more keys in this or subsequent nodes will match
                    past_end = true;
                    break;  // Exit inner loop
                }
            }
            // If key < start, continue checking keys in this node
        }
        if (!past_end) {
            current_node = current_node->next;  // Move to the next leaf node
        }
    }

    if (count == 0) {
        return BPTREE_OK;  // No keys found in the specified range
    }

    // 3. Allocate memory for the results
    // Use calloc for zero-initialization, though not strictly necessary
    // Ensure alignment matches bptree_value_t
    size_t alloc_size = (size_t)count * sizeof(bptree_value_t);
    // Use aligned_alloc if value type requires specific alignment > default malloc alignment
    // For simplicity assuming standard alignment works or value type itself is standard.
    // Use malloc here for broader compatibility unless alignment is critical.
    *out_values = malloc(alloc_size);
    if (!*out_values) {
        return BPTREE_ALLOCATION_FAILURE;
    }

    // 4. Second Pass: Collect the values
    int index = 0;
    current_node = node;  // Reset to the starting leaf node
    past_end = false;
    while (current_node && !past_end && index < count) {
        bptree_key_t *keys = bptree_node_keys(current_node);
        bptree_value_t *values = bptree_node_values(current_node, tree->max_keys);
        for (int i = 0; i < current_node->num_keys; i++) {
            if (tree->compare(&keys[i], start) >= 0) {
                if (tree->compare(&keys[i], end) <= 0) {
                    if (index < count) {  // Defensive check
                        (*out_values)[index++] = values[i];
                    } else {
                        // Should not happen if count was correct
                        bptree_debug_print(tree->enable_debug,
                                           "Range Error: Exceeded count during collection.\n");
                        past_end = true;  // Stop processing
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

    *n_results = index;  // Return the actual number of items copied
    if (index != count) {
        // This indicates a logic error or race condition if threading were involved
        bptree_debug_print(tree->enable_debug, "Range Warning: Final index %d != counted %d\n",
                           index, count);
        // Optionally resize the allocated block down to 'index * sizeof', but usually not critical.
    }

    return BPTREE_OK;
}

// Public API: Frees the memory allocated for range query results.
BPTREE_API void bptree_free_range_results(bptree_value_t *results) {
    free(results);  // Use standard free as allocated by malloc
}

// Public API: Gets statistics about the tree.
BPTREE_API bptree_stats bptree_get_stats(const bptree *tree) {
    bptree_stats stats;
    if (!tree) {
        stats.count = 0;
        stats.height = 0;
        stats.node_count = 0;
    } else {
        stats.count = tree->count;
        stats.height = tree->height;
        // Count nodes starting from the root
        stats.node_count = bptree_count_nodes(tree->root, tree);
    }
    return stats;
}

// Public API: Checks if the tree structure adheres to B+ Tree invariants.
BPTREE_API bool bptree_check_invariants(const bptree *tree) {
    if (!tree || !tree->root) return false;  // Invalid tree object or no root

    // Handle empty tree case: root should be a leaf with 0 keys
    if (tree->count == 0) {
        if (tree->root->is_leaf && tree->root->num_keys == 0 && tree->height == 1) {
            return true;
        } else {
            bptree_debug_print(tree->enable_debug, "Invariant Fail: Empty tree state incorrect.\n");
            return false;
        }
    }

    // For non-empty trees, start recursive check from the root
    int leaf_depth = -1;  // Initialize leaf depth tracker
    return bptree_check_invariants_node(tree->root, tree, 0, &leaf_depth);
}

// Public API: Checks if a key exists in the tree.
BPTREE_API bool bptree_contains(const bptree *tree, const bptree_key_t *key) {
    bptree_value_t dummy_value;  // We only care about the status, not the value
    return (bptree_get(tree, key, &dummy_value) == BPTREE_OK);
}

// Public API: Creates and initializes a new B+ Tree instance.
BPTREE_API inline bptree *bptree_create(int max_keys,
                                        int (*compare)(const bptree_key_t *, const bptree_key_t *),
                                        bool enable_debug) {
    // Basic validation for max_keys (order m, max degree).
    // max_keys is the maximum number of keys in a node.
    // Minimum sensible value is 2 for a 2-3+ tree, but >=3 is common.
    if (max_keys < 3) {  // Allow max_keys greater than or equal to 3
        fprintf(stderr, "[BPTREE CREATE] Error: max_keys must be at least 3.\n");
        return NULL;
    }

    bptree *tree = malloc(sizeof(bptree));
    if (!tree) {
        fprintf(stderr, "[BPTREE CREATE] Error: Failed to allocate memory for tree structure.\n");
        return NULL;
    }

    // Initialize tree metadata
    tree->count = 0;
    tree->height = 1;  // Starts with a single root node (leaf)
    tree->enable_debug = enable_debug;
    tree->max_keys = max_keys;

    // Calculate minimum key counts based on B+ Tree rules (often ceil(m/2) where m=order/degree)
    // Order m usually refers to max children for internal nodes (m = max_keys + 1)
    // Min children = ceil(m/2) = ceil((max_keys+1)/2)
    // Min keys internal = Min children - 1 = ceil((max_keys+1)/2) - 1
    // Min keys leaf = floor(m/2) or ceil(m/2) depending on definition. Using floor here.
    // Let's stick to the common definition: min keys = floor(max_keys/2) or ceil(order/2)-1 keys.
    // Using ceil((max_keys+1)/2) for min children:
    tree->min_internal_keys = ((max_keys + 1) / 2) - 1;  // Min keys for internal node
    if (tree->min_internal_keys < 1) {
        tree->min_internal_keys = 1;  // Internal root needs at least 1 key if not leaf
    }
    // Min keys for leaf nodes (can often hold more than internal nodes)
    // Often defined as ceil(max_keys / 2) or floor(max_keys/2)
    // Using (max_keys + 1) / 2 matches common definition derived from order 'm'.
    tree->min_leaf_keys = (max_keys + 1) / 2;  // <<< BUGGY CALCULATION HERE
    // Ensure minimums make sense
    if (tree->min_leaf_keys < 1 && max_keys >= 1)
        tree->min_leaf_keys = 1;  // Leaf must hold at least 1 key unless tree empty
    if (tree->min_leaf_keys > tree->max_keys) tree->min_leaf_keys = tree->max_keys;  // Sanity check

    bptree_debug_print(enable_debug, "Creating tree. max_keys=%d, min_internal=%d, min_leaf=%d\n",
                       tree->max_keys, tree->min_internal_keys, tree->min_leaf_keys);

    // Assign comparison function (use default if NULL)
    tree->compare = compare ? compare : bptree_default_compare;

    // Allocate the initial root node (which is a leaf)
    tree->root = bptree_node_alloc(tree, true);
    if (!tree->root) {
        fprintf(stderr, "[BPTREE CREATE] Error: Failed to allocate initial root node.\n");
        free(tree);  // Clean up allocated tree structure
        return NULL;
    }

    bptree_debug_print(enable_debug, "Tree created successfully.\n");
    return tree;
}

// Public API: Frees all memory associated with the B+ Tree.
BPTREE_API inline void bptree_free(bptree *tree) {
    if (!tree) return;
    // Recursively free all nodes starting from the root
    if (tree->root) {
        bptree_free_node(tree->root, tree);
    }
    // Free the tree structure itself
    free(tree);
}

#endif  // BPTREE_IMPLEMENTATION

#ifdef __cplusplus
}
#endif

#endif
