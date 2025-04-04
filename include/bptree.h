/**
 * @file bptree.h
 * @brief A single-header B+tree implementation in pure C.
 *
 * This is a generic, memory-efficient B+tree implementation that can be used
 * for key-value storage with different data types. It is implemented as a single
 * header file with no external dependencies other than the C standard library.
 *
 * Usage:
 * 1. Include this header in your source files
 * 2. In ONE source file, define BPTREE_IMPLEMENTATION before including
 *    to generate the implementation
 *
 * Example:
 *   #define BPTREE_IMPLEMENTATION
 *   #include "bptree.h"
 *
 * @note Thread-safety: This library is not explicitly thread-safe.
 *       The caller must handle synchronization if used in a multi-threaded environment.
 */

#ifndef BPTREE_H
#define BPTREE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <time.h>

// Define the maximum size of the timestamp buffer
#define TIMESTAMP_BUF_SIZE 32

/**
 * @brief Gets the current timestamp as a formatted string.
 *
 * This function generates a timestamp string using the current time.
 *
 * @return A pointer to a static buffer containing the formatted timestamp.
 */
static const char *logger_timestamp(void) {
    static char buf[TIMESTAMP_BUF_SIZE];
    const time_t now = time(NULL);
    const struct tm *tm_info = localtime(&now);
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", tm_info);
    return buf;
}

/**
 * @brief Logs a debug message with a timestamp.
 *
 * This function prints a debug message to standard output with the current timestamp.
 *
 * @param fmt Format string (printf-style).
 * @param ... Additional arguments for the format string.
 */
static void bptree_logger(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    printf("[%s] [DBG] ", logger_timestamp());
    vprintf(fmt, args);
    printf("\n");
    va_end(args);
}

/**
 * @brief Custom memory allocation function type for B+Tree.
 *
 * @param size Number of bytes to allocate.
 * @return Pointer to the allocated memory.
 */
typedef void *(*bptree_malloc_t)(size_t size);

/**
 * @brief Custom memory free function type for B+Tree.
 *
 * @param ptr Pointer to the memory to free.
 */
typedef void (*bptree_free_t)(void *ptr);

/**
 * @brief Status codes returned by B+Tree operations.
 */
typedef enum {
    BPTREE_OK,               /**< Operation completed successfully. */
    BPTREE_DUPLICATE,        /**< Attempt to insert a duplicate key. */
    BPTREE_ALLOCATION_ERROR, /**< Memory allocation failure occurred. */
    BPTREE_NOT_FOUND,        /**< Specified key was not found. */
    BPTREE_ERROR             /**< Generic error code. */
} bptree_status;

/**
 * @brief Opaque structure representing a B+Tree.
 */
typedef struct bptree bptree;

/**
 * @brief Creates a new B+Tree.
 *
 * @param max_keys Maximum number of keys in a node.
 * @param compare Comparison function to order keys.
 * @param user_data User-provided data for the comparison function.
 * @param malloc_fn Custom memory allocation function.
 * @param free_fn Custom memory free function.
 * @param debug_enabled Enable or disable debug logging.
 * @return Pointer to the newly created B+Tree, or NULL on failure.
 */
bptree *bptree_new(int max_keys,
                   int (*compare)(const void *first, const void *second, const void *user_data),
                   void *user_data, bptree_malloc_t malloc_fn, bptree_free_t free_fn,
                   bool debug_enabled);

/**
 * @brief Frees the memory allocated for the B+Tree.
 *
 * @param tree Pointer to the B+Tree to free.
 */
void bptree_free(bptree *tree);

/**
 * @brief Inserts an item into the B+Tree.
 *
 * @param tree Pointer to the B+Tree.
 * @param item Pointer to the item to insert.
 * @return Status code indicating the result of the operation.
 */
bptree_status bptree_put(bptree *tree, void *item);

/**
 * @brief Removes an item from the B+Tree.
 *
 * @param tree Pointer to the B+Tree.
 * @param key Pointer to the key of the item to remove.
 * @return Status code indicating the result of the operation.
 */
bptree_status bptree_remove(bptree *tree, const void *key);

/**
 * @brief Retrieves an item from the B+Tree.
 *
 * @param tree Pointer to the B+Tree.
 * @param key Pointer to the key of the item to retrieve.
 * @return Pointer to the item if found, or NULL if not found.
 */
void *bptree_get(const bptree *tree, const void *key);

/**
 * @brief Retrieves a range of items from the B+Tree.
 *
 * The range is inclusive of both start_key and end_key.
 *
 * @param tree Pointer to the B+Tree.
 * @param start_key Pointer to the starting key of the range.
 * @param end_key Pointer to the ending key of the range.
 * @param count Pointer to an integer that will hold the number of items returned.
 * @return Array of pointers to the items in the specified range.
 */
void **bptree_get_range(const bptree *tree, const void *start_key, const void *end_key, int *count);

/**
 * @brief Bulk loads a sorted array of items into a B+Tree.
 *
 * @param max_keys Maximum number of keys in a node.
 * @param compare Comparison function to order keys.
 * @param user_data User-provided data for the comparison function.
 * @param malloc_fn Custom memory allocation function.
 * @param free_fn Custom memory free function.
 * @param debug_enabled Enable or disable debug logging.
 * @param sorted_items Array of sorted items to load.
 * @param n_items Number of items in the array.
 * @return Pointer to the newly created B+Tree, or NULL on failure.
 */
bptree *bptree_bulk_load(int max_keys,
                         int (*compare)(const void *first, const void *second,
                                        const void *user_data),
                         void *user_data, bptree_malloc_t malloc_fn, bptree_free_t free_fn,
                         bool debug_enabled, void **sorted_items, int n_items);

/**
 * @brief Structure representing an iterator for traversing the B+Tree.
 */
typedef struct bptree_iterator {
    struct bptree_node *current_leaf; /**< Current leaf node in the iteration. */
    int index;                        /**< Current index within the leaf node. */
} bptree_iterator;

/**
 * @brief Creates a new iterator for the B+Tree.
 *
 * The iterator traverses items in sorted order.
 *
 * @param tree Pointer to the B+Tree.
 * @return Pointer to the newly created iterator, or NULL on failure.
 */
bptree_iterator *bptree_iterator_new(const bptree *tree);

/**
 * @brief Advances the iterator to the next item.
 *
 * @param iter Pointer to the B+Tree iterator.
 * @return Pointer to the next item, or NULL if the end is reached.
 */
void *bptree_iterator_next(bptree_iterator *iter);

/**
 * @brief Frees the memory allocated for the iterator.
 *
 * @param iter Pointer to the iterator.
 * @param free_fn Custom memory free function.
 */
void bptree_iterator_free(bptree_iterator *iter, bptree_free_t free_fn);

/**
 * @brief Structure containing statistics about the B+Tree.
 */
typedef struct bptree_stats {
    int count;      /**< Total number of items stored in the tree. */
    int height;     /**< Height of the tree. */
    int node_count; /**< Total number of nodes in the tree. */
} bptree_stats;

/**
 * @brief Retrieves statistics about the B+Tree.
 *
 * @param tree Pointer to the B+Tree.
 * @return A structure containing the tree statistics.
 */
bptree_stats bptree_get_stats(const bptree *tree);

#ifdef __cplusplus
}
#endif

#ifdef BPTREE_IMPLEMENTATION

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#define BPTREE_LOG_DEBUG(tree, ...)     \
    do {                                \
        if ((tree)->debug_enabled) {    \
            bptree_logger(__VA_ARGS__); \
        }                               \
    } while (0)

/* Internal default allocation functions */

/**
 * @brief Default memory allocation function.
 *
 * @param size Number of bytes to allocate.
 * @return Pointer to the allocated memory.
 */
static void *default_malloc(const size_t size) { return malloc(size); }

/**
 * @brief Default memory free function.
 *
 * @param ptr Pointer to the memory to free.
 */
static void default_free(void *ptr) { free(ptr); }

/* Internal structure representing a node in the B+Tree */
typedef struct bptree_node {
    int is_leaf;  /**< Flag indicating whether the node is a leaf (non-zero) or internal (zero). */
    int num_keys; /**< Number of keys currently stored in the node. */
    void **keys;  /**< Array of keys stored in the node. */
    union {
        struct {
            void **items;             /**< Array of item pointers (for leaf nodes). */
            struct bptree_node *next; /**< Pointer to the next leaf node. */
        } leaf;
        struct {
            struct bptree_node *
                *children; /**< Array of child node pointers (for internal nodes). */
        } internal;
    } ptr;
} bptree_node;

/* Definition of the main B+Tree structure */
struct bptree {
    int max_keys; /**< Maximum number of keys in a node. */
    int min_keys; /**< Minimum number of keys required in a node (except root). */
    int height;   /**< Current height of the tree. */
    int count;    /**< Total number of items stored in the tree. */
    int (*compare)(const void *first, const void *second,
                   const void *user_data); /**< Comparison function for keys. */
    void *udata;                           /**< User-provided data for the comparison function. */
    bptree_node *root;                     /**< Pointer to the root node of the tree. */
    bptree_malloc_t malloc_fn;             /**< Memory allocation function. */
    bptree_free_t free_fn;                 /**< Memory free function. */
    bool debug_enabled;                    /**< Debug logging flag. */
};

/* Internal helper functions documented below */

/**
 * @brief Performs a binary search on an array of keys.
 *
 * @param tree Pointer to the B+Tree.
 * @param array Array of key pointers.
 * @param count Number of keys in the array.
 * @param key Key to search for.
 * @return Index of the found key or insertion index if not found.
 */
static int binary_search(const bptree *tree, void *const *array, const int count, const void *key) {
    int low = 0, high = count - 1;
    while (low <= high) {
        const int mid = (low + high) / 2;
        const int cmp = tree->compare(key, array[mid], tree->udata);
        if (cmp == 0) {
            return mid;
        }
        if (cmp < 0) {
            high = mid - 1;
        } else {
            low = mid + 1;
        }
    }
    return low;
}

/**
 * @brief Searches for a key in a leaf node.
 *
 * @param tree Pointer to the B+Tree.
 * @param keys Array of keys in the leaf node.
 * @param count Number of keys in the leaf node.
 * @param key Key to search for.
 * @return Index of the found key or insertion index if not found.
 */
static int leaf_node_search(const bptree *tree, void *const *keys, const int count,
                            const void *key) {
    return binary_search(tree, keys, count, key);
}

/**
 * @brief Searches for a key in an internal node.
 *
 * @param tree Pointer to the B+Tree.
 * @param keys Array of keys in the internal node.
 * @param count Number of keys in the internal node.
 * @param key Key to search for.
 * @return Index of the child pointer to follow.
 */
static int internal_node_search(const bptree *tree, void *const *keys, const int count,
                                const void *key) {
    int low = 0, high = count;
    while (low < high) {
        const int mid = (low + high) / 2;
        if (tree->compare(key, keys[mid], tree->udata) < 0) {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    return low;
}

/**
 * @brief Creates a new leaf node.
 *
 * @param tree Pointer to the B+Tree.
 * @return Pointer to the new leaf node, or NULL on failure.
 */
static bptree_node *create_leaf(const bptree *tree) {
    bptree_node *node = tree->malloc_fn(sizeof(bptree_node));
    if (!node) {
        BPTREE_LOG_DEBUG(tree, "Allocation failure (leaf node)");
        return NULL;
    }
    node->is_leaf = 1;
    node->num_keys = 0;
    node->keys = (void **)tree->malloc_fn(tree->max_keys * sizeof(void *));
    if (!node->keys) {
        tree->free_fn(node);
        BPTREE_LOG_DEBUG(tree, "Allocation failure (leaf keys)");
        return NULL;
    }
    node->ptr.leaf.items = (void **)tree->malloc_fn(tree->max_keys * sizeof(void *));
    if (!node->ptr.leaf.items) {
        tree->free_fn(node->keys);
        tree->free_fn(node);
        BPTREE_LOG_DEBUG(tree, "Allocation failure (leaf items)");
        return NULL;
    }
    node->ptr.leaf.next = NULL;
    return node;
}

/**
 * @brief Creates a new internal node.
 *
 * @param tree Pointer to the B+Tree.
 * @return Pointer to the new internal node, or NULL on failure.
 */
static bptree_node *create_internal(const bptree *tree) {
    bptree_node *node = tree->malloc_fn(sizeof(bptree_node));
    if (!node) {
        BPTREE_LOG_DEBUG(tree, "Allocation failure (internal node)");
        return NULL;
    }
    node->is_leaf = 0;
    node->num_keys = 0;
    node->keys = (void **)tree->malloc_fn(tree->max_keys * sizeof(void *));
    if (!node->keys) {
        tree->free_fn(node);
        BPTREE_LOG_DEBUG(tree, "Allocation failure (internal keys)");
        return NULL;
    }
    node->ptr.internal.children =
        (bptree_node **)tree->malloc_fn((tree->max_keys + 1) * sizeof(bptree_node *));
    if (!node->ptr.internal.children) {
        tree->free_fn(node->keys);
        tree->free_fn(node);
        BPTREE_LOG_DEBUG(tree, "Allocation failure (internal children)");
        return NULL;
    }
    return node;
}

/**
 * @brief Recursively frees a node and its descendants.
 *
 * @param tree Pointer to the B+Tree.
 * @param node Pointer to the node to free.
 */
static void free_node(bptree *tree, bptree_node *node) {
    if (node == NULL) {
        return;
    }
    if (node->is_leaf) {
        tree->free_fn(node->keys);
        tree->free_fn(node->ptr.leaf.items);
    } else {
        for (int i = 0; i <= node->num_keys; i++) {
            free_node(tree, node->ptr.internal.children[i]);
        }
        tree->free_fn(node->keys);
        tree->free_fn(node->ptr.internal.children);
    }
    tree->free_fn(node);
}

/* Structure for internal result handling during insertion. */
typedef struct {
    void *promoted_key;     /**< Key to be promoted to parent node. */
    bptree_node *new_child; /**< Pointer to the new node created after split. */
    bptree_status status;   /**< Status of the insertion operation. */
} insert_result;

/**
 * @brief Splits an internal node and promotes a key.
 *
 * @param tree Pointer to the B+Tree.
 * @param node Internal node to split.
 * @param new_key Key to insert.
 * @param new_child Child pointer corresponding to new_key.
 * @param pos Position to insert the new key.
 * @return Structure containing the promoted key, new child, and status.
 */
static insert_result split_internal(const bptree *tree, bptree_node *node, void *new_key,
                                    bptree_node *new_child, const int pos) {
    insert_result res = {NULL, NULL, BPTREE_ERROR};
    const int total = node->num_keys + 1;
    const int split = total / 2;
    void **all_keys = tree->malloc_fn(total * sizeof(void *));
    bptree_node **all_children = tree->malloc_fn((total + 1) * sizeof(bptree_node *));
    if (!all_keys || !all_children) {
        if (all_keys) {
            tree->free_fn(all_keys);
        }
        if (all_children) {
            tree->free_fn(all_children);
        }
        BPTREE_LOG_DEBUG(tree, "Allocation failure during split_internal");
        return res;
    }
    memcpy(all_keys, node->keys, node->num_keys * sizeof(void *));
    memcpy(all_children, node->ptr.internal.children, (node->num_keys + 1) * sizeof(bptree_node *));
    memmove(&all_keys[pos + 1], &all_keys[pos], (node->num_keys - pos) * sizeof(void *));
    all_keys[pos] = new_key;
    memmove(&all_children[pos + 2], &all_children[pos + 1],
            (node->num_keys - pos) * sizeof(bptree_node *));
    all_children[pos + 1] = new_child;
    node->num_keys = split;
    memcpy(node->keys, all_keys, split * sizeof(void *));
    memcpy(node->ptr.internal.children, all_children, (split + 1) * sizeof(bptree_node *));
    bptree_node *new_internal = create_internal(tree);
    if (!new_internal) {
        tree->free_fn(all_keys);
        tree->free_fn(all_children);
        return res;
    }
    new_internal->num_keys = total - split - 1;
    memcpy(new_internal->keys, &all_keys[split + 1], (total - split - 1) * sizeof(void *));
    memcpy(new_internal->ptr.internal.children, &all_children[split + 1],
           (total - split) * sizeof(bptree_node *));
    res.promoted_key = all_keys[split];
    assert(res.promoted_key != NULL);
    res.new_child = new_internal;
    res.status = BPTREE_OK;
    tree->free_fn(all_keys);
    tree->free_fn(all_children);
    return res;
}

/**
 * @brief Recursively inserts an item into the B+Tree.
 *
 * @param tree Pointer to the B+Tree.
 * @param node Current node in the recursion.
 * @param item Pointer to the item to insert.
 * @return Structure containing information about a potential key promotion and status.
 */
static insert_result insert_recursive(bptree *tree, bptree_node *node, void *item) {
    insert_result result = {NULL, NULL, BPTREE_ERROR};
    if (node->is_leaf) {
        const int pos = leaf_node_search(tree, node->keys, node->num_keys, item);
        if (pos < node->num_keys && tree->compare(item, node->keys[pos], tree->udata) == 0) {
            result.status = BPTREE_DUPLICATE;
            return result;
        }
        if (node->num_keys < tree->max_keys) {
            memmove(&node->keys[pos + 1], &node->keys[pos],
                    (node->num_keys - pos) * sizeof(void *));
            memmove(&node->ptr.leaf.items[pos + 1], &node->ptr.leaf.items[pos],
                    (node->num_keys - pos) * sizeof(void *));
            node->keys[pos] = item;
            node->ptr.leaf.items[pos] = item;
            node->num_keys++;
            result.status = BPTREE_OK;
            return result;
        }
        const int total = node->num_keys + 1;
        const int split = total / 2;
        void **temp_keys = tree->malloc_fn(total * sizeof(void *));
        void **temp_items = tree->malloc_fn(total * sizeof(void *));
        if (!temp_keys || !temp_items) {
            if (temp_keys) {
                tree->free_fn(temp_keys);
            }
            if (temp_items) {
                tree->free_fn(temp_items);
            }
            BPTREE_LOG_DEBUG(tree, "Allocation failure during leaf split");
            return result;
        }
        memcpy(temp_keys, node->keys, pos * sizeof(void *));
        memcpy(temp_items, node->ptr.leaf.items, pos * sizeof(void *));
        temp_keys[pos] = item;
        temp_items[pos] = item;
        memcpy(&temp_keys[pos + 1], &node->keys[pos], (node->num_keys - pos) * sizeof(void *));
        memcpy(&temp_items[pos + 1], &node->ptr.leaf.items[pos],
               (node->num_keys - pos) * sizeof(void *));
        node->num_keys = split;
        memcpy(node->keys, temp_keys, split * sizeof(void *));
        memcpy(node->ptr.leaf.items, temp_items, split * sizeof(void *));
        bptree_node *new_leaf = create_leaf(tree);
        if (!new_leaf) {
            tree->free_fn(temp_keys);
            tree->free_fn(temp_items);
            return result;
        }
        new_leaf->num_keys = total - split;
        memcpy(new_leaf->keys, &temp_keys[split], (total - split) * sizeof(void *));
        memcpy(new_leaf->ptr.leaf.items, &temp_items[split], (total - split) * sizeof(void *));
        new_leaf->ptr.leaf.next = node->ptr.leaf.next;
        node->ptr.leaf.next = new_leaf;
        result.promoted_key = new_leaf->keys[0];
        result.new_child = new_leaf;
        result.status = BPTREE_OK;
        tree->free_fn(temp_keys);
        tree->free_fn(temp_items);
        return result;
    }
    const int pos = internal_node_search(tree, node->keys, node->num_keys, item);
    const insert_result child_result =
        insert_recursive(tree, node->ptr.internal.children[pos], item);
    if (child_result.status == BPTREE_DUPLICATE) {
        return child_result;
    }
    if (child_result.status != BPTREE_OK) {
        return child_result;
    }
    if (child_result.promoted_key == NULL) {
        return child_result;
    }
    if (node->num_keys < tree->max_keys) {
        memmove(&node->keys[pos + 1], &node->keys[pos], (node->num_keys - pos) * sizeof(void *));
        memmove(&node->ptr.internal.children[pos + 2], &node->ptr.internal.children[pos + 1],
                (node->num_keys - pos) * sizeof(bptree_node *));
        node->keys[pos] = child_result.promoted_key;
        node->ptr.internal.children[pos + 1] = child_result.new_child;
        node->num_keys++;
        result.status = BPTREE_OK;
        return result;
    }
    return split_internal(tree, node, child_result.promoted_key, child_result.new_child, pos);
}

inline bptree_status bptree_put(bptree *tree, void *item) {
    const insert_result result = insert_recursive(tree, tree->root, item);
    if (result.status == BPTREE_DUPLICATE) {
        return BPTREE_DUPLICATE;
    }
    if (result.status != BPTREE_OK) {
        return result.status;
    }
    if (result.promoted_key == NULL) {
        tree->count++;
        return BPTREE_OK;
    }
    bptree_node *new_root = create_internal(tree);
    if (!new_root) {
        return BPTREE_ALLOCATION_ERROR;
    }
    new_root->num_keys = 1;
    new_root->keys[0] = result.promoted_key;
    new_root->ptr.internal.children[0] = tree->root;
    new_root->ptr.internal.children[1] = result.new_child;
    tree->root = new_root;
    tree->height++;
    tree->count++;
    return BPTREE_OK;
}

inline void *bptree_get(const bptree *tree, const void *key) {
    const bptree_node *node = tree->root;
    while (!node->is_leaf) {
        const int pos = internal_node_search(tree, node->keys, node->num_keys, key);
        node = node->ptr.internal.children[pos];
    }
    const int pos = leaf_node_search(tree, node->keys, node->num_keys, key);
    if (pos < node->num_keys && tree->compare(key, node->keys[pos], tree->udata) == 0) {
        return node->ptr.leaf.items[pos];
    }
    return NULL;
}

/* Structure used during deletion to track traversal */
typedef struct {
    bptree_node *node; /**< Current node in deletion stack. */
    int pos;           /**< Position of the child pointer in the parent node. */
} delete_stack_item;

inline bptree_status bptree_remove(bptree *tree, const void *key) {
    if (tree == NULL || tree->root == NULL) {
        return BPTREE_ERROR;
    }
    const int INITIAL_STACK_CAPACITY = 16;
    int stack_capacity = INITIAL_STACK_CAPACITY;
    int depth = 0;
    delete_stack_item *stack = tree->malloc_fn(stack_capacity * sizeof(delete_stack_item));
    if (stack == NULL) {
        return BPTREE_ALLOCATION_ERROR;
    }
    bptree_node *node = tree->root;
    while (!node->is_leaf) {
        const int pos = internal_node_search(tree, node->keys, node->num_keys, key);
        if (depth >= stack_capacity) {
            const int new_capacity = stack_capacity * 2;
            delete_stack_item *new_stack =
                tree->malloc_fn(new_capacity * sizeof(delete_stack_item));
            if (new_stack == NULL) {
                tree->free_fn(stack);
                return BPTREE_ALLOCATION_ERROR;
            }
            memcpy(new_stack, stack, depth * sizeof(delete_stack_item));
            tree->free_fn(stack);
            stack = new_stack;
            stack_capacity = new_capacity;
        }
        stack[depth].node = node;
        stack[depth].pos = pos;
        depth++;
        node = node->ptr.internal.children[pos];
    }
    const int pos = leaf_node_search(tree, node->keys, node->num_keys, key);
    if (pos >= node->num_keys || tree->compare(key, node->keys[pos], tree->udata) != 0) {
        tree->free_fn(stack);
        return BPTREE_NOT_FOUND;
    }
    memmove(&node->ptr.leaf.items[pos], &node->ptr.leaf.items[pos + 1],
            (node->num_keys - pos - 1) * sizeof(void *));
    memmove(&node->keys[pos], &node->keys[pos + 1], (node->num_keys - pos - 1) * sizeof(void *));
    node->num_keys--;
    bool underflow = node != tree->root && node->num_keys < tree->min_keys;
    while (underflow && depth > 0) {
        depth--;
        bptree_node *parent = stack[depth].node;
        const int child_index = stack[depth].pos;
        bptree_node *child = parent->ptr.internal.children[child_index];
        bptree_node *left = child_index > 0 ? parent->ptr.internal.children[child_index - 1] : NULL;
        bptree_node *right =
            child_index < parent->num_keys ? parent->ptr.internal.children[child_index + 1] : NULL;
        BPTREE_LOG_DEBUG(tree,
                         "Iterative deletion at depth %d: parent num_keys=%d, child index=%d "
                         "(is_leaf=%d, num_keys=%d)",
                         depth, parent->num_keys, child_index, child->is_leaf, child->num_keys);
        bool merged = false;
        if (left && left->num_keys > tree->min_keys) {
            if (child->is_leaf) {
                memmove(&child->ptr.leaf.items[1], child->ptr.leaf.items,
                        child->num_keys * sizeof(void *));
                memmove(&child->keys[1], child->keys, child->num_keys * sizeof(void *));
                child->ptr.leaf.items[0] = left->ptr.leaf.items[left->num_keys - 1];
                child->keys[0] = left->keys[left->num_keys - 1];
                left->num_keys--;
                child->num_keys++;
                parent->keys[child_index - 1] = child->keys[0];
            } else {
                memmove(&child->keys[1], child->keys, child->num_keys * sizeof(void *));
                memmove(&child->ptr.internal.children[1], child->ptr.internal.children,
                        (child->num_keys + 1) * sizeof(bptree_node *));
                child->keys[0] = parent->keys[child_index - 1];
                parent->keys[child_index - 1] = left->keys[left->num_keys - 1];
                child->ptr.internal.children[0] = left->ptr.internal.children[left->num_keys];
                left->num_keys--;
                child->num_keys++;
            }
            merged = true;
        } else if (right && right->num_keys > tree->min_keys) {
            if (child->is_leaf) {
                child->ptr.leaf.items[child->num_keys] = right->ptr.leaf.items[0];
                child->keys[child->num_keys] = right->keys[0];
                memmove(&right->ptr.leaf.items[0], &right->ptr.leaf.items[1],
                        (right->num_keys - 1) * sizeof(void *));
                memmove(&right->keys[0], &right->keys[1], (right->num_keys - 1) * sizeof(void *));
                right->num_keys--;
                parent->keys[child_index] = right->keys[0];
                child->num_keys++;
            } else {
                child->keys[child->num_keys] = parent->keys[child_index];
                child->ptr.internal.children[child->num_keys + 1] = right->ptr.internal.children[0];
                parent->keys[child_index] = right->keys[0];
                memmove(&right->keys[0], &right->keys[1], (right->num_keys - 1) * sizeof(void *));
                memmove(&right->ptr.internal.children[0], &right->ptr.internal.children[1],
                        (right->num_keys - 1) * sizeof(bptree_node *));
                right->num_keys--;
                child->num_keys++;
            }
            merged = true;
        } else {
            if (left) {
                BPTREE_LOG_DEBUG(tree, "Merging child index %d with left sibling", child_index);
                if (child->is_leaf) {
                    memcpy(&left->ptr.leaf.items[left->num_keys], child->ptr.leaf.items,
                           child->num_keys * sizeof(void *));
                    memcpy(&left->keys[left->num_keys], child->keys,
                           child->num_keys * sizeof(void *));
                    left->num_keys += child->num_keys;
                    left->ptr.leaf.next = child->ptr.leaf.next;
                } else {
                    left->keys[left->num_keys] = parent->keys[child_index - 1];
                    left->num_keys++;
                    memcpy(&left->keys[left->num_keys], child->keys,
                           child->num_keys * sizeof(void *));
                    memcpy(&left->ptr.internal.children[left->num_keys],
                           child->ptr.internal.children,
                           (child->num_keys + 1) * sizeof(bptree_node *));
                    left->num_keys += child->num_keys;
                }
                {
                    const int old_children = parent->num_keys + 1;
                    memmove(&parent->ptr.internal.children[child_index],
                            &parent->ptr.internal.children[child_index + 1],
                            (old_children - child_index - 1) * sizeof(bptree_node *));
                    parent->ptr.internal.children[old_children - 1] = NULL;
                }
                {
                    const int old_keys = parent->num_keys;
                    memmove(&parent->keys[child_index - 1], &parent->keys[child_index],
                            (old_keys - child_index) * sizeof(void *));
                    parent->num_keys = old_keys - 1;
                }
                free_node(tree, child);
                merged = true;
                break;
            }
            if (right) {
                BPTREE_LOG_DEBUG(tree, "Merging child index %d with right sibling", child_index);
                if (child->is_leaf) {
                    memcpy(&child->ptr.leaf.items[child->num_keys], right->ptr.leaf.items,
                           right->num_keys * sizeof(void *));
                    memcpy(&child->keys[child->num_keys], right->keys,
                           right->num_keys * sizeof(void *));
                    child->num_keys += right->num_keys;
                    child->ptr.leaf.next = right->ptr.leaf.next;
                } else {
                    child->keys[child->num_keys] = parent->keys[child_index];
                    child->num_keys++;
                    memcpy(&child->keys[child->num_keys], right->keys,
                           right->num_keys * sizeof(void *));
                    memcpy(&child->ptr.internal.children[child->num_keys],
                           right->ptr.internal.children,
                           (right->num_keys + 1) * sizeof(bptree_node *));
                    child->num_keys += right->num_keys;
                }
                {
                    const int old_children = parent->num_keys + 1;
                    memmove(&parent->ptr.internal.children[child_index + 1],
                            &parent->ptr.internal.children[child_index + 2],
                            (old_children - child_index - 2) * sizeof(bptree_node *));
                    parent->ptr.internal.children[old_children - 1] = NULL;
                }
                {
                    const int old_keys = parent->num_keys;
                    memmove(&parent->keys[child_index], &parent->keys[child_index + 1],
                            (old_keys - child_index - 1) * sizeof(void *));
                    parent->num_keys = old_keys - 1;
                }
                free_node(tree, right);
                merged = true;
                break;
            }
        }
        if (merged) {
            child = parent->ptr.internal.children[child_index];
            underflow = child != tree->root && child->num_keys < tree->min_keys;
        } else {
            underflow = false;
        }
    }
    if (tree->root->num_keys == 0 && !tree->root->is_leaf) {
        bptree_node *old_root = tree->root;
        tree->root = tree->root->ptr.internal.children[0];
        tree->free_fn(old_root->keys);
        tree->free_fn(old_root->ptr.internal.children);
        tree->free_fn(old_root);
        tree->height--;
    }
    tree->count--;
    tree->free_fn(stack);
    return BPTREE_OK;
}

inline bptree *bptree_new(
    int max_keys, int (*compare)(const void *first, const void *second, const void *user_data),
    void *user_data, bptree_malloc_t malloc_fn, bptree_free_t free_fn, const bool debug_enabled) {
    if (max_keys < 3) {
        max_keys = 3;
    }
    if (!malloc_fn) {
        malloc_fn = default_malloc;
    }
    if (!free_fn) {
        free_fn = default_free;
    }
    bptree *tree = malloc_fn(sizeof(bptree));
    if (!tree) {
        return NULL;
    }
    tree->max_keys = max_keys;
    tree->min_keys = (max_keys + 1) / 2;
    tree->height = 1;
    tree->count = 0;
    tree->compare = compare;
    tree->udata = user_data;
    tree->malloc_fn = malloc_fn;
    tree->free_fn = free_fn;
    tree->debug_enabled = debug_enabled;
    BPTREE_LOG_DEBUG(tree, "B+tree created (max_keys=%d)", tree->max_keys);
    tree->root = create_leaf(tree);
    if (!tree->root) {
        tree->free_fn(tree);
        return NULL;
    }
    return tree;
}

inline void bptree_free(bptree *tree) {
    if (tree == NULL) {
        return;
    }
    free_node(tree, tree->root);
    tree->free_fn(tree);
}

inline void **bptree_get_range(const bptree *tree, const void *start_key, const void *end_key,
                               int *count) {
    *count = 0;
    if (tree == NULL || tree->root == NULL) {
        return NULL;
    }
    const bptree_node *node = tree->root;
    while (!node->is_leaf) {
        const int pos = internal_node_search(tree, node->keys, node->num_keys, start_key);
        node = node->ptr.internal.children[pos];
    }
    int capacity = 16;
    void **results = tree->malloc_fn(capacity * sizeof(void *));
    if (results == NULL) {
        return NULL;
    }
    while (node) {
        for (int i = 0; i < node->num_keys; i++) {
            if (tree->compare(node->keys[i], start_key, tree->udata) >= 0 &&
                tree->compare(node->keys[i], end_key, tree->udata) <= 0) {
                if (*count >= capacity) {
                    capacity *= 2;
                    void **temp = tree->malloc_fn(capacity * sizeof(void *));
                    if (temp == NULL) {
                        tree->free_fn(results);
                        return NULL;
                    }
                    memcpy(temp, results, *count * sizeof(void *));
                    tree->free_fn(results);
                    results = temp;
                }
                results[(*count)++] = node->ptr.leaf.items[i];
            } else if (tree->compare(node->keys[i], end_key, tree->udata) > 0) {
                return results;
            }
        }
        node = node->ptr.leaf.next;
    }
    return results;
}

bptree *bptree_bulk_load(int max_keys,
                         int (*compare)(const void *first, const void *second,
                                        const void *user_data),
                         void *user_data, bptree_malloc_t malloc_fn, bptree_free_t free_fn,
                         bool debug_enabled, void **sorted_items, int n_items) {
    if (n_items <= 0 || !sorted_items) {
        return NULL;
    }
    bptree *tree = bptree_new(max_keys, compare, user_data, malloc_fn, free_fn, debug_enabled);
    if (!tree) {
        return NULL;
    }
    int items_per_leaf = tree->max_keys;
    int n_leaves = (n_items + items_per_leaf - 1) / items_per_leaf;
    bptree_node **leaves = tree->malloc_fn(n_leaves * sizeof(bptree_node *));
    if (!leaves) {
        bptree_free(tree);
        return NULL;
    }
    int item_index = 0;
    for (int i = 0; i < n_leaves; i++) {
        bptree_node *leaf = create_leaf(tree);
        if (!leaf) {
            for (int j = 0; j < i; j++) {
                free_node(tree, leaves[j]);
            }
            tree->free_fn(leaves);
            bptree_free(tree);
            return NULL;
        }
        int count = 0;
        while (count < items_per_leaf && item_index < n_items) {
            leaf->keys[count] = sorted_items[item_index];
            leaf->ptr.leaf.items[count] = sorted_items[item_index];
            count++;
            item_index++;
        }
        leaf->num_keys = count;
        leaves[i] = leaf;
    }
    for (int i = 0; i < n_leaves - 1; i++) {
        leaves[i]->ptr.leaf.next = leaves[i + 1];
    }
    int level_count = n_leaves;
    bptree_node **current_level = leaves;
    // Build internal levels using a fixed group size equal to max_keys.
    while (level_count > 1) {
        int group_size = tree->max_keys;
        int parent_count = (level_count + group_size - 1) / group_size;
        bptree_node **parent_level = tree->malloc_fn(parent_count * sizeof(bptree_node *));
        if (!parent_level) {
            for (int j = 0; j < level_count; j++) {
                free_node(tree, current_level[j]);
            }
            if (current_level != leaves) {
                tree->free_fn(current_level);
            }
            bptree_free(tree);
            return NULL;
        }
        int parent_index = 0;
        int i = 0;
        while (i < level_count) {
            bptree_node *parent = create_internal(tree);
            if (!parent) {
                for (int j = 0; j < level_count; j++) {
                    free_node(tree, current_level[j]);
                }
                for (int j = 0; j < parent_index; j++) {
                    free_node(tree, parent_level[j]);
                }
                tree->free_fn(parent_level);
                if (current_level != leaves) {
                    tree->free_fn(current_level);
                }
                bptree_free(tree);
                return NULL;
            }
            int child_count = 0;
            for (int j = 0; j < group_size && i < level_count; j++, i++) {
                parent->ptr.internal.children[child_count] = current_level[i];
                if (child_count > 0) {
                    bptree_node *child = current_level[i];
                    while (!child->is_leaf) {
                        child = child->ptr.internal.children[0];
                    }
                    parent->keys[child_count - 1] = child->keys[0];
                }
                child_count++;
            }
            parent->num_keys = child_count - 1;
            parent_level[parent_index++] = parent;
        }
        tree->height++;
        if (current_level != leaves) {
            tree->free_fn(current_level);
        }
        current_level = parent_level;
        level_count = parent_count;
    }
    // Save and free the initial root allocated by bptree_new.
    bptree_node *old_root = tree->root;
    tree->root = current_level[0];
    free_node(tree, old_root);
    if (current_level != leaves) {
        tree->free_fn(current_level);
    }
    tree->free_fn(leaves);
    if (!tree->root->is_leaf && tree->root->num_keys == 0) {
        bptree_node *temp = tree->root;
        tree->root = temp->ptr.internal.children[0];
        tree->free_fn(temp->keys);
        tree->free_fn(temp->ptr.internal.children);
        tree->free_fn(temp);
        tree->height--;
    }
    tree->count = n_items;
    return tree;
}

bptree_iterator *bptree_iterator_new(const bptree *tree) {
    if (!tree || !tree->root) {
        return NULL;
    }
    bptree_iterator *iter = tree->malloc_fn(sizeof(bptree_iterator));
    if (!iter) {
        return NULL;
    }
    bptree_node *node = tree->root;
    while (!node->is_leaf) {
        node = node->ptr.internal.children[0];
    }
    iter->current_leaf = node;
    iter->index = 0;
    return iter;
}

void *bptree_iterator_next(bptree_iterator *iter) {
    if (!iter || !iter->current_leaf) {
        return NULL;
    }
    if (iter->index < iter->current_leaf->num_keys) {
        return iter->current_leaf->ptr.leaf.items[iter->index++];
    } else {
        iter->current_leaf = iter->current_leaf->ptr.leaf.next;
        iter->index = 0;
        if (!iter->current_leaf) {
            return NULL;
        }
        return iter->current_leaf->ptr.leaf.items[iter->index++];
    }
}

void bptree_iterator_free(bptree_iterator *iter, bptree_free_t free_fn) {
    if (iter && free_fn) {
        free_fn(iter);
    }
}

/**
 * @brief Recursively counts the nodes in the B+Tree.
 *
 * @param tree Pointer to the B+Tree.
 * @param node Pointer to the current node.
 * @return Total count of nodes in the subtree.
 */
static int count_nodes(const bptree *tree, bptree_node *node) {
    if (!node) return 0;
    if (node->is_leaf) {
        return 1;
    }
    int total = 1;
    for (int i = 0; i <= node->num_keys; i++) {
        total += count_nodes(tree, node->ptr.internal.children[i]);
    }
    return total;
}

bptree_stats bptree_get_stats(const bptree *tree) {
    bptree_stats stats;
    if (!tree) {
        stats.count = 0;
        stats.height = 0;
        stats.node_count = 0;
        return stats;
    }
    stats.count = tree->count;
    stats.height = tree->height;
    stats.node_count = count_nodes(tree, tree->root);
    return stats;
}

#endif /* BPTREE_IMPLEMENTATION */
#endif /* BPTREE_H */
