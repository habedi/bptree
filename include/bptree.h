/**
 * @file bptree.h
 * @brief A single-header B+ tree implementation in pure C.
 *
 * This is a generic, in-memory B+ tree implementation that can be used
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

/* Buffer size for formatted timestamp. */
#define TIMESTAMP_BUF_SIZE 32

/**
 * Gets the current timestamp as a formatted string.
 */
static const char *logger_timestamp(void) {
    static char buf[TIMESTAMP_BUF_SIZE];
    const time_t now = time(NULL);
    const struct tm *tm_info = localtime(&now);
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", tm_info);
    return buf;
}

/**
 * Logs a debug message with a timestamp.
 */
static void bptree_logger(const char *fmt, ...) {
    va_list args;
    va_start(args, fmt);
    printf("[%s] [DBG] ", logger_timestamp());
    vprintf(fmt, args);
    printf("\n");
    va_end(args);
}

/*===========================================================================
  NEW ALLOCATOR INTERFACE
  Now the allocation functions get a context pointer.
  The free function also receives the allocation size,
  and a realloc function is added.
===========================================================================*/

/* Allocation function pointer types */
typedef void *(*bptree_malloc_t)(size_t size, void *ctx);
typedef void (*bptree_free_t)(void *ptr, size_t size, void *ctx);
typedef void *(*bptree_realloc_t)(void *ptr, size_t old_size, size_t new_size, void *ctx);

/**
 * Status codes returned by B+ tree operations.
 */
typedef enum {
    BPTREE_OK,
    BPTREE_DUPLICATE,
    BPTREE_ALLOCATION_ERROR,
    BPTREE_NOT_FOUND,
    BPTREE_ERROR
} bptree_status;

/**
 * Opaque structure representing a B+ tree.
 */
typedef struct bptree bptree;

/**
 * Creates a new B+ tree.
 *
 * @param max_keys Maximum number of keys in a node.
 * @param compare Comparison function to order keys.
 * @param user_data User-provided data for the comparison function.
 * @param alloc_ctx User-defined context pointer for the allocator.
 * @param malloc_fn Custom memory allocation function.
 * @param free_fn Custom memory free function.
 * @param realloc_fn Custom realloc function.
 * @param debug_enabled Enable or disable debug logging.
 * @return Pointer to the newly created B+ tree, or NULL on failure.
 */
bptree *bptree_new(int max_keys,
                   int (*compare)(const void *first, const void *second, const void *user_data),
                   void *user_data, void *alloc_ctx, bptree_malloc_t malloc_fn,
                   bptree_free_t free_fn, bptree_realloc_t realloc_fn, bool debug_enabled);

/**
 * Frees the memory allocated for the B+ tree.
 */
void bptree_free(bptree *tree);

/**
 * Inserts an item into the B+ tree.
 */
bptree_status bptree_put(bptree *tree, void *item);

/**
 * Removes an item from the B+ tree.
 */
bptree_status bptree_remove(bptree *tree, const void *key);

/**
 * Retrieves an item from the B+ tree.
 */
void *bptree_get(const bptree *tree, const void *key);

/**
 * Retrieves a range of items from the B+ tree.
 */
void **bptree_get_range(const bptree *tree, const void *start_key, const void *end_key, int *count);

/**
 * Bulk loads a sorted array of items into a B+ tree.
 */
bptree *bptree_bulk_load(
    int max_keys, int (*compare)(const void *first, const void *second, const void *user_data),
    void *user_data, void *alloc_ctx, bptree_malloc_t malloc_fn, bptree_free_t free_fn,
    bptree_realloc_t realloc_fn, bool debug_enabled, void **sorted_items, int n_items);

/**
 * Structure representing an iterator for traversing the B+ tree.
 */
typedef struct bptree_iterator {
    struct bptree_node *current_leaf;
    int index;
} bptree_iterator;

/**
 * Creates a new iterator for the B+ tree.
 */
bptree_iterator *bptree_iterator_new(const bptree *tree);

/**
 * Advances the iterator to the next item.
 */
void *bptree_iterator_next(bptree_iterator *iter);

/**
 * Frees the memory allocated for the iterator.
 */
void bptree_iterator_free(bptree_iterator *iter, bptree_free_t free_fn, void *alloc_ctx);

/**
 * Structure containing statistics about the B+ tree.
 */
typedef struct bptree_stats {
    int count;
    int height;
    int node_count;
} bptree_stats;

/**
 * Retrieves statistics about the B+ tree.
 */
bptree_stats bptree_get_stats(const bptree *tree);

#ifdef __cplusplus
}
#endif

#ifdef BPTREE_IMPLEMENTATION

#include <assert.h>
#include <stdlib.h>
#include <string.h>

/* Macro for logging if debug is enabled */
#define BPTREE_LOG_DEBUG(tree, ...)     \
    do {                                \
        if ((tree)->debug_enabled) {    \
            bptree_logger(__VA_ARGS__); \
        }                               \
    } while (0)

/*===========================================================================
  Default Allocator Functions
  These wrap the standard library functions. The context is ignored.
===========================================================================*/
static void *default_malloc(const size_t size, void *ctx) {
    (void)ctx;
    return malloc(size);
}

static void default_free(void *ptr, size_t size, void *ctx) {
    (void)size;
    (void)ctx;
    free(ptr);
}

static void *default_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx) {
    (void)old_size;
    (void)ctx;
    return realloc(ptr, new_size);
}

/*===========================================================================
  Internal Structures and Functions
===========================================================================*/

/* Internal structure representing a node in the B+ tree */
typedef struct bptree_node {
    int is_leaf;
    int num_keys;
    void **keys;
    union {
        struct {
            void **items;
            struct bptree_node *next;
        } leaf;
        struct {
            struct bptree_node **children;
        } internal;
    } ptr;
} bptree_node;

/* The main B+ tree structure now includes alloc_ctx and realloc_fn. */
struct bptree {
    int max_keys;
    int min_keys;
    int height;
    int count;
    int (*compare)(const void *first, const void *second, const void *user_data);
    void *udata;
    bptree_node *root;
    void *alloc_ctx; /* Allocator context */
    bptree_malloc_t malloc_fn;
    bptree_free_t free_fn;
    bptree_realloc_t realloc_fn; /* Realloc function */
    bool debug_enabled;
};

/* Performs a binary search on an array of keys. */
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

/* Searches for a key in a leaf node. */
static int leaf_node_search(const bptree *tree, void *const *keys, const int count,
                            const void *key) {
    return binary_search(tree, keys, count, key);
}

/* Searches for a key in an internal node. */
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

/* Creates a new leaf node. */
static bptree_node *create_leaf(const bptree *tree) {
    bptree_node *node = tree->malloc_fn(sizeof(bptree_node), tree->alloc_ctx);
    if (!node) {
        BPTREE_LOG_DEBUG(tree, "Allocation failure (leaf node)");
        return NULL;
    }
    node->is_leaf = 1;
    node->num_keys = 0;
    node->keys = (void **)tree->malloc_fn(tree->max_keys * sizeof(void *), tree->alloc_ctx);
    if (!node->keys) {
        tree->free_fn(node, sizeof(bptree_node), tree->alloc_ctx);
        BPTREE_LOG_DEBUG(tree, "Allocation failure (leaf keys)");
        return NULL;
    }
    node->ptr.leaf.items =
        (void **)tree->malloc_fn(tree->max_keys * sizeof(void *), tree->alloc_ctx);
    if (!node->ptr.leaf.items) {
        tree->free_fn(node->keys, tree->max_keys * sizeof(void *), tree->alloc_ctx);
        tree->free_fn(node, sizeof(bptree_node), tree->alloc_ctx);
        BPTREE_LOG_DEBUG(tree, "Allocation failure (leaf items)");
        return NULL;
    }
    node->ptr.leaf.next = NULL;
    return node;
}

/* Creates a new internal node. */
static bptree_node *create_internal(const bptree *tree) {
    bptree_node *node = tree->malloc_fn(sizeof(bptree_node), tree->alloc_ctx);
    if (!node) {
        BPTREE_LOG_DEBUG(tree, "Allocation failure (internal node)");
        return NULL;
    }
    node->is_leaf = 0;
    node->num_keys = 0;
    node->keys = (void **)tree->malloc_fn(tree->max_keys * sizeof(void *), tree->alloc_ctx);
    if (!node->keys) {
        tree->free_fn(node, sizeof(bptree_node), tree->alloc_ctx);
        BPTREE_LOG_DEBUG(tree, "Allocation failure (internal keys)");
        return NULL;
    }
    node->ptr.internal.children = (bptree_node **)tree->malloc_fn(
        (tree->max_keys + 1) * sizeof(bptree_node *), tree->alloc_ctx);
    if (!node->ptr.internal.children) {
        tree->free_fn(node->keys, tree->max_keys * sizeof(void *), tree->alloc_ctx);
        tree->free_fn(node, sizeof(bptree_node), tree->alloc_ctx);
        BPTREE_LOG_DEBUG(tree, "Allocation failure (internal children)");
        return NULL;
    }
    return node;
}

/* Recursively frees a node and its descendants. */
static void free_node(bptree *tree, bptree_node *node) {
    if (node == NULL) {
        return;
    }
    if (node->is_leaf) {
        tree->free_fn(node->keys, tree->max_keys * sizeof(void *), tree->alloc_ctx);
        tree->free_fn(node->ptr.leaf.items, tree->max_keys * sizeof(void *), tree->alloc_ctx);
    } else {
        for (int i = 0; i <= node->num_keys; i++) {
            free_node(tree, node->ptr.internal.children[i]);
        }
        tree->free_fn(node->keys, tree->max_keys * sizeof(void *), tree->alloc_ctx);
        tree->free_fn(node->ptr.internal.children, (tree->max_keys + 1) * sizeof(bptree_node *),
                      tree->alloc_ctx);
    }
    tree->free_fn(node, sizeof(bptree_node), tree->alloc_ctx);
}

/* Structure for internal result handling during insertion. */
typedef struct {
    void *promoted_key;
    bptree_node *new_child;
    bptree_status status;
} insert_result;

/* Splits an internal node and promotes a key. */
static insert_result split_internal(const bptree *tree, bptree_node *node, void *new_key,
                                    bptree_node *new_child, const int pos) {
    insert_result res = {NULL, NULL, BPTREE_ERROR};
    const int total = node->num_keys + 1;
    const int split = total / 2;
    void **all_keys = tree->malloc_fn(total * sizeof(void *), tree->alloc_ctx);
    bptree_node **all_children =
        tree->malloc_fn((total + 1) * sizeof(bptree_node *), tree->alloc_ctx);
    if (!all_keys || !all_children) {
        if (all_keys) {
            tree->free_fn(all_keys, total * sizeof(void *), tree->alloc_ctx);
        }
        if (all_children) {
            tree->free_fn(all_children, (total + 1) * sizeof(bptree_node *), tree->alloc_ctx);
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
        tree->free_fn(all_keys, total * sizeof(void *), tree->alloc_ctx);
        tree->free_fn(all_children, (total + 1) * sizeof(bptree_node *), tree->alloc_ctx);
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
    tree->free_fn(all_keys, total * sizeof(void *), tree->alloc_ctx);
    tree->free_fn(all_children, (total + 1) * sizeof(bptree_node *), tree->alloc_ctx);
    return res;
}

/* Recursively inserts an item into the B+ tree. */
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
        void **temp_keys = tree->malloc_fn(total * sizeof(void *), tree->alloc_ctx);
        void **temp_items = tree->malloc_fn(total * sizeof(void *), tree->alloc_ctx);
        if (!temp_keys || !temp_items) {
            if (temp_keys) {
                tree->free_fn(temp_keys, total * sizeof(void *), tree->alloc_ctx);
            }
            if (temp_items) {
                tree->free_fn(temp_items, total * sizeof(void *), tree->alloc_ctx);
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
            tree->free_fn(temp_keys, total * sizeof(void *), tree->alloc_ctx);
            tree->free_fn(temp_items, total * sizeof(void *), tree->alloc_ctx);
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
        tree->free_fn(temp_keys, total * sizeof(void *), tree->alloc_ctx);
        tree->free_fn(temp_items, total * sizeof(void *), tree->alloc_ctx);
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

typedef struct {
    bptree_node *node;
    int pos;
} delete_stack_item;

inline bptree_status bptree_remove(bptree *tree, const void *key) {
    if (tree == NULL || tree->root == NULL) {
        return BPTREE_ERROR;
    }
    const int INITIAL_STACK_CAPACITY = 16;
    int stack_capacity = INITIAL_STACK_CAPACITY;
    int depth = 0;
    delete_stack_item *stack =
        tree->malloc_fn(stack_capacity * sizeof(delete_stack_item), tree->alloc_ctx);
    if (stack == NULL) {
        return BPTREE_ALLOCATION_ERROR;
    }
    bptree_node *node = tree->root;
    while (!node->is_leaf) {
        if (depth >= stack_capacity) {
            const int new_capacity = stack_capacity * 2;
            delete_stack_item *new_stack =
                tree->malloc_fn(new_capacity * sizeof(delete_stack_item), tree->alloc_ctx);
            if (new_stack == NULL) {
                tree->free_fn(stack, stack_capacity * sizeof(delete_stack_item), tree->alloc_ctx);
                return BPTREE_ALLOCATION_ERROR;
            }
            memcpy(new_stack, stack, depth * sizeof(delete_stack_item));
            tree->free_fn(stack, stack_capacity * sizeof(delete_stack_item), tree->alloc_ctx);
            stack = new_stack;
            stack_capacity = new_capacity;
        }
        stack[depth].node = node;
        stack[depth].pos = internal_node_search(tree, node->keys, node->num_keys, key);
        depth++;
        node = node->ptr.internal.children[stack[depth - 1].pos];
    }
    const int pos = leaf_node_search(tree, node->keys, node->num_keys, key);
    if (pos >= node->num_keys || tree->compare(key, node->keys[pos], tree->udata) != 0) {
        tree->free_fn(stack, stack_capacity * sizeof(delete_stack_item), tree->alloc_ctx);
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
        tree->free_fn(old_root->keys, tree->max_keys * sizeof(void *), tree->alloc_ctx);
        tree->free_fn(old_root->ptr.internal.children, (tree->max_keys + 1) * sizeof(bptree_node *),
                      tree->alloc_ctx);
        tree->free_fn(old_root, sizeof(bptree_node), tree->alloc_ctx);
        tree->height--;
    }
    tree->count--;
    tree->free_fn(stack, stack_capacity * sizeof(delete_stack_item), tree->alloc_ctx);
    return BPTREE_OK;
}

inline bptree *bptree_new(
    int max_keys, int (*compare)(const void *first, const void *second, const void *user_data),
    void *user_data, void *alloc_ctx, bptree_malloc_t malloc_fn, bptree_free_t free_fn,
    bptree_realloc_t realloc_fn, const bool debug_enabled) {
    if (max_keys < 3) {
        max_keys = 3;
    }
    if (!malloc_fn) {
        malloc_fn = default_malloc;
    }
    if (!free_fn) {
        free_fn = default_free;
    }
    if (!realloc_fn) {
        realloc_fn = default_realloc;
    }
    bptree *tree = malloc_fn(sizeof(bptree), alloc_ctx);
    if (!tree) {
        return NULL;
    }
    tree->max_keys = max_keys;
    tree->min_keys = (max_keys + 1) / 2;
    tree->height = 1;
    tree->count = 0;
    tree->compare = compare;
    tree->udata = user_data;
    tree->alloc_ctx = alloc_ctx;
    tree->malloc_fn = malloc_fn;
    tree->free_fn = free_fn;
    tree->realloc_fn = realloc_fn;
    tree->debug_enabled = debug_enabled;
    BPTREE_LOG_DEBUG(tree, "B+ tree created (max_keys=%d)", tree->max_keys);
    tree->root = create_leaf(tree);
    if (!tree->root) {
        tree->free_fn(tree, sizeof(bptree), alloc_ctx);
        return NULL;
    }
    return tree;
}

inline void bptree_free(bptree *tree) {
    if (tree == NULL) {
        return;
    }
    free_node(tree, tree->root);
    tree->free_fn(tree, sizeof(bptree), tree->alloc_ctx);
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
    void **results = tree->malloc_fn(capacity * sizeof(void *), tree->alloc_ctx);
    if (results == NULL) {
        return NULL;
    }
    while (node) {
        for (int i = 0; i < node->num_keys; i++) {
            if (tree->compare(node->keys[i], start_key, tree->udata) >= 0 &&
                tree->compare(node->keys[i], end_key, tree->udata) <= 0) {
                if (*count >= capacity) {
                    capacity *= 2;
                    void **temp = tree->realloc_fn(results, (*count) * sizeof(void *),
                                                   capacity * sizeof(void *), tree->alloc_ctx);
                    if (temp == NULL) {
                        tree->free_fn(results, (*count) * sizeof(void *), tree->alloc_ctx);
                        return NULL;
                    }
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

bptree *bptree_bulk_load(
    int max_keys, int (*compare)(const void *first, const void *second, const void *user_data),
    void *user_data, void *alloc_ctx, bptree_malloc_t malloc_fn, bptree_free_t free_fn,
    bptree_realloc_t realloc_fn, bool debug_enabled, void **sorted_items, int n_items) {
    if (n_items <= 0 || !sorted_items) {
        return NULL;
    }
    bptree *tree = bptree_new(max_keys, compare, user_data, alloc_ctx, malloc_fn, free_fn,
                              realloc_fn, debug_enabled);
    if (!tree) {
        return NULL;
    }
    int items_per_leaf = tree->max_keys;
    int n_leaves = (n_items + items_per_leaf - 1) / items_per_leaf;
    bptree_node **leaves = tree->malloc_fn(n_leaves * sizeof(bptree_node *), tree->alloc_ctx);
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
            tree->free_fn(leaves, n_leaves * sizeof(bptree_node *), tree->alloc_ctx);
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
    while (level_count > 1) {
        int group_size = tree->max_keys;
        int parent_count = (level_count + group_size - 1) / group_size;
        bptree_node **parent_level =
            tree->malloc_fn(parent_count * sizeof(bptree_node *), tree->alloc_ctx);
        if (!parent_level) {
            for (int j = 0; j < level_count; j++) {
                free_node(tree, current_level[j]);
            }
            if (current_level != leaves) {
                tree->free_fn(current_level, level_count * sizeof(bptree_node *), tree->alloc_ctx);
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
                tree->free_fn(parent_level, parent_count * sizeof(bptree_node *), tree->alloc_ctx);
                if (current_level != leaves) {
                    tree->free_fn(current_level, level_count * sizeof(bptree_node *),
                                  tree->alloc_ctx);
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
            tree->free_fn(current_level, level_count * sizeof(bptree_node *), tree->alloc_ctx);
        }
        current_level = parent_level;
        level_count = parent_count;
    }
    bptree_node *old_root = tree->root;
    tree->root = current_level[0];
    free_node(tree, old_root);
    if (current_level != leaves) {
        tree->free_fn(current_level, sizeof(bptree_node *), tree->alloc_ctx);
    }
    tree->free_fn(leaves, n_leaves * sizeof(bptree_node *), tree->alloc_ctx);
    if (!tree->root->is_leaf && tree->root->num_keys == 0) {
        bptree_node *temp = tree->root;
        tree->root = temp->ptr.internal.children[0];
        tree->free_fn(temp->keys, tree->max_keys * sizeof(void *), tree->alloc_ctx);
        tree->free_fn(temp->ptr.internal.children, (tree->max_keys + 1) * sizeof(bptree_node *),
                      tree->alloc_ctx);
        tree->free_fn(temp, sizeof(bptree_node), tree->alloc_ctx);
        tree->height--;
    }
    tree->count = n_items;
    return tree;
}

bptree_iterator *bptree_iterator_new(const bptree *tree) {
    if (!tree || !tree->root) {
        return NULL;
    }
    bptree_iterator *iter = tree->malloc_fn(sizeof(bptree_iterator), tree->alloc_ctx);
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

void bptree_iterator_free(bptree_iterator *iter, bptree_free_t free_fn, void *alloc_ctx) {
    if (iter && free_fn) {
        free_fn(iter, sizeof(bptree_iterator), alloc_ctx);
    }
}

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
