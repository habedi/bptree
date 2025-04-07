/**
 * @file bptree.h
 * @brief A single-header B+ tree implementation in pure C.
 *
 * This is a generic B+ tree implementation that can be used
 * for key-value storage with different data types. It is implemented as a single
 * header file with no external dependencies other than the C standard library.
 *
 * Usage:
 * 1. Include this header in your source files.
 * 2. In ONE source file, define BPTREE_IMPLEMENTATION before including
 *    to generate the implementation.
 *
 * Example:
 *   #define BPTREE_IMPLEMENTATION
 *   #include "bptree.h"
 *
 * @note Thread-safety: This library is not explicitly thread-safe.
 *       The caller must handle synchronization if used in a multithreaded environment.
 *       Memory management: The tree does not manage the memory of the values it stores.
 *       So, the user is responsible for allocating and freeing the values.
 *       The tree does not copy the items; it only stores pointers to them.
 */

#ifndef BPTREE_H
#define BPTREE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdalign.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ============================
   Compile-Time Key/Value Types
   ============================
   For fixed‑size string keys, define BPTREE_KEY_TYPE_STRING and BPTREE_KEY_SIZE.
   Otherwise, define one of:
      BPTREE_KEY_TYPE_INT, BPTREE_KEY_TYPE_FLOAT, or BPTREE_KEY_TYPE_DOUBLE.
   If none is defined, it defaults to int.
*/
#if defined(BPTREE_KEY_TYPE_STRING)
#ifndef BPTREE_KEY_SIZE
#error "BPTREE_KEY_SIZE must be defined for string key type"
#endif
typedef struct {
    char data[BPTREE_KEY_SIZE];
} bptree_key_t;
#else
#if !defined(BPTREE_KEY_TYPE_INT) && !defined(BPTREE_KEY_TYPE_FLOAT) && \
    !defined(BPTREE_KEY_TYPE_DOUBLE)
#define BPTREE_KEY_TYPE_INT
#endif
#ifdef BPTREE_KEY_TYPE_INT
typedef int bptree_key_t;
#endif
#ifdef BPTREE_KEY_TYPE_FLOAT
typedef float bptree_key_t;
#endif
#ifdef BPTREE_KEY_TYPE_DOUBLE
typedef double bptree_key_t;
#endif
#endif

#ifndef BPTREE_VALUE_TYPE
#define BPTREE_VALUE_TYPE void *
#endif
typedef BPTREE_VALUE_TYPE bptree_value_t;

/* Automatic comparator based on key type */
#ifdef BPTREE_KEY_TYPE_INT
static inline int bptree_default_compare(const bptree_key_t *a, const bptree_key_t *b) {
    return (*a < *b) ? -1 : ((*a > *b) ? 1 : 0);
}
#endif
#ifdef BPTREE_KEY_TYPE_FLOAT
static inline int bptree_default_compare(const bptree_key_t *a, const bptree_key_t *b) {
    /* Note: This comparator uses direct comparisons.
       It does not handle NaN or precision issues; for robust handling, provide a custom comparator.
     */
    return (*a < *b) ? -1 : ((*a > *b) ? 1 : 0);
}
#endif
#ifdef BPTREE_KEY_TYPE_DOUBLE
static inline int bptree_default_compare(const bptree_key_t *a, const bptree_key_t *b) {
    /* Note: This comparator uses direct comparisons.
       It does not handle NaN or precision issues; for robust handling, provide a custom comparator.
     */
    return (*a < *b) ? -1 : ((*a > *b) ? 1 : 0);
}
#endif
#ifdef BPTREE_KEY_TYPE_STRING
static inline int bptree_default_compare(const bptree_key_t *a, const bptree_key_t *b) {
    /* Use strcmp to compare C strings respecting null terminators.
       Users must guarantee that the key data is null-terminated. */
    return strcmp(a->data, b->data);
}
#endif

/* ============================
   Public API Status Codes
   ============================
*/
typedef enum {
    BPTREE_OK = 0,
    BPTREE_DUPLICATE_KEY,
    BPTREE_KEY_NOT_FOUND,
    BPTREE_ALLOCATION_FAILURE,
    BPTREE_INVALID_ARGUMENT,
    BPTREE_BULK_LOAD_NOT_SORTED,
    BPTREE_BULK_LOAD_DUPLICATE,
    BPTREE_INTERNAL_ERROR
} bptree_status;

/* ============================
   Public API Types & Structures
   ============================
*/

/* Forward declaration for node */
typedef struct bptree_node bptree_node;

/* Node structure – storage is allocated dynamically.
   The flexible array member is declared last.
*/
struct bptree_node {
    bool is_leaf;
    int num_keys;
    bptree_node *next;  // For leaf nodes only
    char data[];        // Keys followed by padding then values or child pointers
};

/* bptree structure now stores runtime configuration parameters. */
typedef struct bptree {
    int count;             /* Total number of keys stored */
    int height;            /* Tree height */
    bool enable_debug;     /* Debug flag */
    int max_keys;          /* Maximum keys per node */
    int min_leaf_keys;     /* ceil(max_keys/2) */
    int min_internal_keys; /* min_leaf_keys - 1 */
    int (*compare)(const bptree_key_t *, const bptree_key_t *);
    bptree_node *root;
} bptree;

/* Structure for tree statistics */
typedef struct bptree_stats {
    int count;
    int height;
    int node_count;
} bptree_stats;

/* Iterator structure */
typedef struct bptree_iterator {
    bptree_node *current_leaf;
    int index;
    bptree *tree;
} bptree_iterator;

/* ============================
   Public API Functions
   ============================
*/
static bptree *bptree_create(int max_keys,
                             int (*compare)(const bptree_key_t *, const bptree_key_t *),
                             bool enable_debug);
static void bptree_free(bptree *tree);
static bptree_status bptree_put(bptree *tree, const bptree_key_t *key, bptree_value_t value);
static bptree_status bptree_upsert(bptree *tree, const bptree_key_t *key, bptree_value_t value);
static bptree_value_t bptree_get(const bptree *tree, const bptree_key_t *key);
static bptree_status bptree_remove(bptree *tree, const bptree_key_t *key);
static bptree_status bptree_bulk_load(bptree *tree, const bptree_key_t keys[],
                                      const bptree_value_t values[], int n_items);
/**
 * @brief Retrieve an array of values whose keys fall within the specified range.
 *
 * @param tree The B+ tree.
 * @param start The start key.
 * @param end The end key.
 * @param n_results Pointer to store the number of results.
 * @return Pointer to a malloc’ed array of values. Caller must free this array.
 */
static bptree_value_t *bptree_get_range(const bptree *tree, const bptree_key_t *start,
                                        const bptree_key_t *end, int *n_results);
static bptree_stats bptree_get_stats(const bptree *tree);
static bool bptree_check_invariants(const bptree *tree);
static void bptree_assert_invariants(const bptree *tree);
static bptree_iterator *bptree_iterator_new(const bptree *tree);
static bptree_value_t bptree_iterator_next(bptree_iterator *iter);
static void bptree_iterator_destroy(bptree_iterator *iter);

#ifdef __cplusplus
}
#endif

/* ============================
   Implementation
   ============================
*/
#ifdef BPTREE_IMPLEMENTATION

#define INLINE inline

/* Debug helper that prints the current timestamp */
static void bptree_debug_print(bool enable, const char *fmt, ...) {
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

/* Compute the size of the keys area including padding.
   We pad the keys area so that the next area (values or child pointers)
   starts at an address that is a multiple of alignof(bptree_value_t).
*/
static size_t bptree_keys_area_size(int max_keys) {
    size_t keys_size = max_keys * sizeof(bptree_key_t);
    size_t alignment = alignof(bptree_value_t);
    size_t pad = (alignment - (keys_size % alignment)) % alignment;
    return keys_size + pad;
}

/* --------------------------
   Node Helpers
----------------------------*/
/* Return pointer to keys stored in node */
static bptree_key_t *bptree_node_keys(bptree_node *node) { return (bptree_key_t *)node->data; }

/* Return a pointer to values (or child pointers) stored in node,
   using the padded keys area size as the offset.
*/
static bptree_value_t *bptree_node_values(bptree_node *node, int max_keys) {
    size_t offset = bptree_keys_area_size(max_keys);
    return (bptree_value_t *)(node->data + offset);
}

/* For internal nodes, return pointer to child pointers */
static bptree_node **bptree_node_children(bptree_node *node, int max_keys) {
    size_t offset = bptree_keys_area_size(max_keys);
    return (bptree_node **)(node->data + offset);
}

/* --------------------------
   Node Allocation
----------------------------*/
static bptree_node *bptree_node_alloc(bptree *tree, bool is_leaf) {
    int max_keys = tree->max_keys;
    size_t offset = bptree_keys_area_size(max_keys);
    size_t size;
    if (is_leaf) {
        size = sizeof(bptree_node) + offset + max_keys * sizeof(bptree_value_t);
    } else {
        size = sizeof(bptree_node) + offset + (max_keys + 1) * sizeof(bptree_node *);
    }
    size_t alignment = alignof(bptree_node);
    /* Round size up to the nearest multiple of alignment */
    size_t alloc_size = (size + alignment - 1) / alignment * alignment;
    bptree_node *node = aligned_alloc(alignment, alloc_size);
    if (node) {
        node->is_leaf = is_leaf;
        node->num_keys = 0;
        if (is_leaf) node->next = NULL;
    }
    return node;
}

static bptree_key_t bptree_find_smallest_key(bptree_node *node, int max_keys) {
    while (!node->is_leaf) node = bptree_node_children(node, max_keys)[0];
    return bptree_node_keys(node)[0];
}

/* --------------------------
   Tree Creation & Freeing
----------------------------*/
static bptree *bptree_create(int max_keys,
                             int (*compare)(const bptree_key_t *, const bptree_key_t *),
                             bool enable_debug) {
    if (max_keys < 3) return NULL;
    bptree *tree = malloc(sizeof(bptree));
    if (!tree) return NULL;
    tree->count = 0;
    tree->height = 1;
    tree->enable_debug = enable_debug;
    tree->max_keys = max_keys;
    tree->min_leaf_keys = (max_keys + 1) / 2;
    tree->min_internal_keys = tree->min_leaf_keys - 1;
    tree->compare = (compare) ? compare : bptree_default_compare;
    tree->root = bptree_node_alloc(tree, true);
    if (!tree->root) {
        free(tree);
        return NULL;
    }
    bptree_debug_print(tree->enable_debug, "Tree created (max_keys=%d)\n", tree->max_keys);
    return tree;
}

static void bptree_free_node(bptree_node *node, bptree *tree) {
    if (!node) return;
    if (!node->is_leaf) {
        bptree_node **children = bptree_node_children(node, tree->max_keys);
        for (int i = 0; i <= node->num_keys; i++) {
            bptree_free_node(children[i], tree);
        }
    }
    free(node);
}

static void bptree_free(bptree *tree) {
    if (!tree) return;
    bptree_free_node(tree->root, tree);
    free(tree);
}

/* --------------------------
   Binary Search Helper
----------------------------*/
static int bptree_node_search(const bptree *tree, bptree_node *node, const bptree_key_t *key) {
    int low = 0, high = node->num_keys - 1;
    bptree_key_t *keys = bptree_node_keys(node);
    while (low <= high) {
        int mid = low + (high - low) / 2;  // Use safe midpoint calculation to avoid overflow
        int cmp = tree->compare(key, &keys[mid]);
        if (cmp == 0) return node->is_leaf ? mid : (mid + 1);
        if (cmp < 0)
            high = mid - 1;
        else
            low = mid + 1;
    }
    return low;
}

/* --------------------------
   Insertion (Recursive)
----------------------------*/
static bptree_status bptree_insert_internal(bptree *tree, bptree_node *node,
                                            const bptree_key_t *key, bptree_value_t value,
                                            bptree_key_t *promoted_key, bptree_node **new_child) {
    int pos = bptree_node_search(tree, node, key);
    bptree_key_t *keys = bptree_node_keys(node);
    if (node->is_leaf) {
        bptree_value_t *values = bptree_node_values(node, tree->max_keys);
        if (pos < node->num_keys && tree->compare(key, &keys[pos]) == 0)
            return BPTREE_DUPLICATE_KEY;
        for (int i = node->num_keys; i > pos; i--) {
            keys[i] = keys[i - 1];
            values[i] = values[i - 1];
        }
        keys[pos] = *key;
        values[pos] = value;
        node->num_keys++;
        if (node->num_keys >= tree->max_keys) {
            int split = (tree->max_keys + 1) / 2;
            bptree_node *new_leaf = bptree_node_alloc(tree, true);
            if (!new_leaf) return BPTREE_ALLOCATION_FAILURE;
            bptree_key_t *new_keys = bptree_node_keys(new_leaf);
            bptree_value_t *new_values = bptree_node_values(new_leaf, tree->max_keys);
            new_leaf->num_keys = node->num_keys - split;
            memcpy(new_keys, &keys[split], new_leaf->num_keys * sizeof(bptree_key_t));
            memcpy(new_values, bptree_node_values(node, tree->max_keys) + split,
                   new_leaf->num_keys * sizeof(bptree_value_t));
            node->num_keys = split;
            new_leaf->next = node->next;
            node->next = new_leaf;
            *promoted_key = new_keys[0];
            *new_child = new_leaf;
            bptree_debug_print(tree->enable_debug, "Leaf split: promoted key set\n");
        }
        return BPTREE_OK;
    }
    bptree_key_t child_promoted;
    bptree_node *child_new = NULL;
    bptree_status status =
        bptree_insert_internal(tree, bptree_node_children(node, tree->max_keys)[pos], key, value,
                               &child_promoted, &child_new);
    if (status == BPTREE_DUPLICATE_KEY) return status;
    if (child_new != NULL) {
        for (int i = node->num_keys; i > pos; i--) {
            keys[i] = keys[i - 1];
            bptree_node_children(node, tree->max_keys)[i + 1] =
                bptree_node_children(node, tree->max_keys)[i];
        }
        keys[pos] = child_promoted;
        bptree_node_children(node, tree->max_keys)[pos + 1] = child_new;
        node->num_keys++;
        if (node->num_keys >= tree->max_keys) {
            int split = (tree->max_keys + 1) / 2;
            bptree_node *new_internal = bptree_node_alloc(tree, false);
            if (!new_internal) return BPTREE_ALLOCATION_FAILURE;
            bptree_key_t *new_keys = bptree_node_keys(new_internal);
            bptree_node **new_children = bptree_node_children(new_internal, tree->max_keys);
            new_internal->num_keys = node->num_keys - split - 1;
            memcpy(new_keys, &keys[split + 1], new_internal->num_keys * sizeof(bptree_key_t));
            memcpy(new_children, bptree_node_children(node, tree->max_keys) + split + 1,
                   (new_internal->num_keys + 1) * sizeof(bptree_node *));
            *promoted_key = keys[split];
            node->num_keys = split;
            *new_child = new_internal;
            bptree_debug_print(tree->enable_debug, "Internal node split: promoted key set\n");
        }
    }
    return status;
}

static bptree_status bptree_put(bptree *tree, const bptree_key_t *key, bptree_value_t value) {
    bptree_key_t promoted_key;
    bptree_node *new_child = NULL;
    bptree_status status =
        bptree_insert_internal(tree, tree->root, key, value, &promoted_key, &new_child);
    if (status == BPTREE_DUPLICATE_KEY) return status;
    if (new_child != NULL) {
        bptree_node *new_root = bptree_node_alloc(tree, false);
        if (!new_root) return BPTREE_ALLOCATION_FAILURE;
        bptree_key_t *root_keys = bptree_node_keys(new_root);
        bptree_node **root_children = bptree_node_children(new_root, tree->max_keys);
        root_keys[0] = promoted_key;
        root_children[0] = tree->root;
        root_children[1] = new_child;
        new_root->num_keys = 1;
        tree->root = new_root;
        tree->height++;
        bptree_debug_print(tree->enable_debug, "Root split: new root created\n");
    }
    tree->count++;
    return BPTREE_OK;
}

static bptree_status bptree_upsert(bptree *tree, const bptree_key_t *key, bptree_value_t value) {
    bptree_node *node = tree->root;
    while (!node->is_leaf) {
        int pos = bptree_node_search(tree, node, key);
        node = bptree_node_children(node, tree->max_keys)[pos];
    }
    bptree_key_t *keys = bptree_node_keys(node);
    bptree_value_t *values = bptree_node_values(node, tree->max_keys);
    int pos = bptree_node_search(tree, node, key);
    if (pos < node->num_keys && tree->compare(key, &keys[pos]) == 0) {
        values[pos] = value;
        bptree_debug_print(tree->enable_debug, "Upsert: updated existing key\n");
        return BPTREE_OK;
    }
    bptree_debug_print(tree->enable_debug, "Upsert: inserting new key\n");
    return bptree_put(tree, key, value);
}

static bptree_value_t bptree_get(const bptree *tree, const bptree_key_t *key) {
    bptree_node *node = tree->root;
    while (!node->is_leaf) {
        int pos = bptree_node_search(tree, node, key);
        node = bptree_node_children(node, tree->max_keys)[pos];
    }
    int pos = bptree_node_search(tree, node, key);
    bptree_key_t *keys = bptree_node_keys(node);
    bptree_value_t *values = bptree_node_values(node, tree->max_keys);
    if (pos < node->num_keys && tree->compare(key, &keys[pos]) == 0) return values[pos];
    return NULL;
}

static bptree_status bptree_remove(bptree *tree, const bptree_key_t *key) {
    if (!tree || !tree->root) return BPTREE_INVALID_ARGUMENT;
#define BPTREE_MAX_HEIGHT 64
    bptree_node *node_stack[BPTREE_MAX_HEIGHT];
    int index_stack[BPTREE_MAX_HEIGHT];
    int depth = 0;
    bptree_node *node = tree->root;
    while (!node->is_leaf) {
        int pos = bptree_node_search(tree, node, key);
        node_stack[depth] = node;
        index_stack[depth] = pos;
        depth++;
        node = bptree_node_children(node, tree->max_keys)[pos];
    }
    node_stack[depth] = node;
    int pos = bptree_node_search(tree, node, key);
    bptree_key_t *keys = bptree_node_keys(node);
    bptree_value_t *values = bptree_node_values(node, tree->max_keys);
    if (pos >= node->num_keys || tree->compare(key, &keys[pos]) != 0) return BPTREE_KEY_NOT_FOUND;
    for (int i = pos; i < node->num_keys - 1; i++) {
        keys[i] = keys[i + 1];
        values[i] = values[i + 1];
    }
    node->num_keys--;
    tree->count--;
    bptree_debug_print(tree->enable_debug, "Key removed from leaf\n");
    for (int level = depth; level > 0; level--) {
        bptree_node *child = node_stack[level];
        bptree_node *parent = node_stack[level - 1];
        int child_index = index_stack[level - 1];
        int min_keys = child->is_leaf ? tree->min_leaf_keys : tree->min_internal_keys;
        if (child == tree->root) min_keys = 1;
        if (child->num_keys >= min_keys) break;
        bptree_key_t *parent_keys = bptree_node_keys(parent);
        bptree_node **parent_children = bptree_node_children(parent, tree->max_keys);
        if (child_index > 0) {
            bptree_node *left = parent_children[child_index - 1];
            if (left->num_keys > (left->is_leaf ? tree->min_leaf_keys : tree->min_internal_keys)) {
                if (child->is_leaf) {
                    bptree_key_t *child_keys = bptree_node_keys(child);
                    bptree_value_t *child_values = bptree_node_values(child, tree->max_keys);
                    bptree_key_t *left_keys = bptree_node_keys(left);
                    bptree_value_t *left_values = bptree_node_values(left, tree->max_keys);
                    memmove(&child_keys[1], &child_keys[0], child->num_keys * sizeof(bptree_key_t));
                    memmove(&child_values[1], &child_values[0],
                            child->num_keys * sizeof(bptree_value_t));
                    child_keys[0] = left_keys[left->num_keys - 1];
                    child_values[0] = left_values[left->num_keys - 1];
                    left->num_keys--;
                    child->num_keys++;
                    parent_keys[child_index - 1] = bptree_node_keys(child)[0];
                    bptree_debug_print(tree->enable_debug,
                                       "Rebalanced: borrowed from left sibling\n");
                } else {
                    bptree_key_t *child_keys = bptree_node_keys(child);
                    bptree_node **child_children = bptree_node_children(child, tree->max_keys);
                    memmove(&child_keys[1], &child_keys[0], child->num_keys * sizeof(bptree_key_t));
                    memmove(&child_children[1], &child_children[0],
                            (child->num_keys + 1) * sizeof(bptree_node *));
                    child_keys[0] = parent_keys[child_index - 1];
                    child_children[0] = bptree_node_children(left, tree->max_keys)[left->num_keys];
                    child->num_keys++;
                    parent_keys[child_index - 1] = bptree_node_keys(left)[left->num_keys - 1];
                    left->num_keys--;
                    bptree_debug_print(tree->enable_debug,
                                       "Rebalanced internal: borrowed from left sibling\n");
                }
                break;
            }
        }
        if (child_index < parent->num_keys) {
            bptree_node *right = parent_children[child_index + 1];
            if (right->num_keys >
                (right->is_leaf ? tree->min_leaf_keys : tree->min_internal_keys)) {
                if (child->is_leaf) {
                    bptree_key_t *child_keys = bptree_node_keys(child);
                    bptree_value_t *child_values = bptree_node_values(child, tree->max_keys);
                    bptree_key_t *right_keys = bptree_node_keys(right);
                    bptree_value_t *right_values = bptree_node_values(right, tree->max_keys);
                    child_keys[child->num_keys] = right_keys[0];
                    child_values[child->num_keys] = right_values[0];
                    child->num_keys++;
                    for (int i = 0; i < right->num_keys - 1; i++) {
                        right_keys[i] = right_keys[i + 1];
                        right_values[i] = right_values[i + 1];
                    }
                    right->num_keys--;
                    parent_keys[child_index] = right_keys[0];
                    bptree_debug_print(tree->enable_debug,
                                       "Rebalanced: borrowed from right sibling\n");
                } else {
                    bptree_key_t *child_keys = bptree_node_keys(child);
                    bptree_node **child_children = bptree_node_children(child, tree->max_keys);
                    child_keys[child->num_keys] = parent_keys[child_index];
                    child_children[child->num_keys + 1] =
                        bptree_node_children(right, tree->max_keys)[0];
                    child->num_keys++;
                    parent_keys[child_index] = bptree_node_keys(right)[0];
                    bptree_key_t *right_keys = bptree_node_keys(right);
                    bptree_node **right_children = bptree_node_children(right, tree->max_keys);
                    for (int i = 0; i < right->num_keys - 1; i++) {
                        right_keys[i] = right_keys[i + 1];
                        right_children[i] = right_children[i + 1];
                    }
                    right_children[right->num_keys - 1] = right_children[right->num_keys];
                    right->num_keys--;
                    bptree_debug_print(tree->enable_debug,
                                       "Rebalanced internal: borrowed from right sibling\n");
                }
                break;
            }
        }
        if (child_index > 0) {
            bptree_node *left = parent_children[child_index - 1];
            if (child->is_leaf) {
                bptree_key_t *left_keys = bptree_node_keys(left);
                bptree_value_t *left_values = bptree_node_values(left, tree->max_keys);
                bptree_key_t *child_keys = bptree_node_keys(child);
                bptree_value_t *child_values = bptree_node_values(child, tree->max_keys);
                memcpy(&left_keys[left->num_keys], child_keys,
                       child->num_keys * sizeof(bptree_key_t));
                memcpy(&left_values[left->num_keys], child_values,
                       child->num_keys * sizeof(bptree_value_t));
                left->num_keys += child->num_keys;
                left->next = child->next;
                bptree_debug_print(tree->enable_debug, "Merged with left sibling\n");
            } else {
                bptree_key_t *left_keys = bptree_node_keys(left);
                bptree_node **left_children = bptree_node_children(left, tree->max_keys);
                left_keys[left->num_keys] = parent_keys[child_index - 1];
                left->num_keys++;
                bptree_key_t *child_keys = bptree_node_keys(child);
                bptree_node **child_children = bptree_node_children(child, tree->max_keys);
                memcpy(&left_keys[left->num_keys], child_keys,
                       child->num_keys * sizeof(bptree_key_t));
                memcpy(&left_children[left->num_keys], child_children,
                       (child->num_keys + 1) * sizeof(bptree_node *));
                left->num_keys += child->num_keys;
                bptree_debug_print(tree->enable_debug, "Merged internal node with left sibling\n");
            }
            for (int i = child_index - 1; i < parent->num_keys - 1; i++)
                parent_keys[i] = parent_keys[i + 1];
            for (int i = child_index; i < parent->num_keys; i++)
                parent_children[i] = parent_children[i + 1];
            parent->num_keys--;
            free(child);
        } else {
            bptree_node *right = parent_children[child_index + 1];
            if (child->is_leaf) {
                bptree_key_t *child_keys = bptree_node_keys(child);
                bptree_value_t *child_values = bptree_node_values(child, tree->max_keys);
                bptree_key_t *right_keys = bptree_node_keys(right);
                bptree_value_t *right_values = bptree_node_values(right, tree->max_keys);
                memcpy(&child_keys[child->num_keys], right_keys,
                       right->num_keys * sizeof(bptree_key_t));
                memcpy(&child_values[child->num_keys], right_values,
                       right->num_keys * sizeof(bptree_value_t));
                child->num_keys += right->num_keys;
                child->next = right->next;
                bptree_debug_print(tree->enable_debug, "Merged with right sibling\n");
            } else {
                bptree_key_t *child_keys = bptree_node_keys(child);
                bptree_node **child_children = bptree_node_children(child, tree->max_keys);
                child_keys[child->num_keys] = parent_keys[child_index];
                child->num_keys++;
                bptree_key_t *right_keys = bptree_node_keys(right);
                bptree_node **right_children = bptree_node_children(right, tree->max_keys);
                memcpy(&child_keys[child->num_keys], right_keys,
                       right->num_keys * sizeof(bptree_key_t));
                memcpy(&child_children[child->num_keys], right_children,
                       (right->num_keys + 1) * sizeof(bptree_node *));
                child->num_keys += right->num_keys;
                bptree_debug_print(tree->enable_debug, "Merged internal node with right sibling\n");
            }
            for (int i = child_index; i < parent->num_keys - 1; i++)
                parent_keys[i] = parent_keys[i + 1];
            for (int i = child_index + 1; i < parent->num_keys; i++)
                parent_children[i] = parent_children[i + 1];
            parent->num_keys--;
            free(right);
        }
    }
    if (!tree->root->is_leaf && tree->root->num_keys == 0) {
        bptree_node *old_root = tree->root;
        bptree_node **root_children = bptree_node_children(tree->root, tree->max_keys);
        tree->root = root_children[0];
        free(old_root);
        tree->height--;
        bptree_debug_print(tree->enable_debug, "Root collapsed\n");
    }
#undef BPTREE_MAX_HEIGHT
    return BPTREE_OK;
}

/* --------------------------
   Bulk Load
----------------------------
   Note: This function fills leaf nodes sequentially.
   Consequently, if the total number of items is small (or not a multiple of max_keys),
   the last (or sole) leaf node may contain fewer than min_leaf_keys.
   This is acceptable when the leaf is also the root.
*/
static bptree_status bptree_bulk_load(bptree *tree, const bptree_key_t keys[],
                                      const bptree_value_t values[], int n_items) {
    if (!tree || n_items <= 0) return BPTREE_INVALID_ARGUMENT;
    if (tree->root) {
        bptree_free_node(tree->root, tree);
        tree->root = NULL;
    }
    for (int i = 1; i < n_items; i++) {
        int cmp = tree->compare(&keys[i - 1], &keys[i]);
        if (cmp > 0) return BPTREE_BULK_LOAD_NOT_SORTED;
        if (cmp == 0) return BPTREE_BULK_LOAD_DUPLICATE;
    }
    bptree_debug_print(tree->enable_debug, "Bulk load: %d items\n", n_items);
    int num_leaves = (n_items + tree->max_keys - 1) / tree->max_keys;
    bptree_node **level = (bptree_node **)malloc(num_leaves * sizeof(bptree_node *));
    if (!level) return BPTREE_ALLOCATION_FAILURE;
    memset(level, 0, num_leaves * sizeof(bptree_node *));
    int idx = 0;
    for (int i = 0; i < num_leaves; i++) {
        bptree_node *leaf = bptree_node_alloc(tree, true);
        if (!leaf) {
            free(level);
            return BPTREE_ALLOCATION_FAILURE;
        }
        int count = ((n_items - idx) > tree->max_keys) ? tree->max_keys : (n_items - idx);
        memcpy(bptree_node_keys(leaf), &keys[idx], count * sizeof(bptree_key_t));
        memcpy(bptree_node_values(leaf, tree->max_keys), &values[idx],
               count * sizeof(bptree_value_t));
        leaf->num_keys = count;
        idx += count;
        if (i > 0) level[i - 1]->next = leaf;
        level[i] = leaf;
        bptree_debug_print(tree->enable_debug, "Leaf %d built with %d keys\n", i, count);
    }
    int level_nodes = num_leaves;
    int current_height = 1;
    while (level_nodes > 1) {
        int parent_nodes = (level_nodes + tree->max_keys) / (tree->max_keys + 1);
        bptree_node **parent_level = (bptree_node **)malloc(parent_nodes * sizeof(bptree_node *));
        if (!parent_level) {
            free(level);
            return BPTREE_ALLOCATION_FAILURE;
        }
        int j = 0;
        for (int i = 0; i < level_nodes;) {
            bptree_node *internal = bptree_node_alloc(tree, false);
            if (!internal) {
                free(parent_level);
                free(level);
                return BPTREE_ALLOCATION_FAILURE;
            }
            int children_count = 0;
            bptree_node **children = bptree_node_children(internal, tree->max_keys);
            bptree_key_t *internal_keys = bptree_node_keys(internal);
            while (i < level_nodes && children_count < (tree->max_keys + 1)) {
                bptree_node *child_node = level[i];
                children[children_count] = child_node;
                if (children_count > 0)
                    internal_keys[children_count - 1] =
                        bptree_find_smallest_key(child_node, tree->max_keys);
                children_count++;
                i++;
            }
            internal->num_keys = children_count - 1;
            parent_level[j++] = internal;
            bptree_debug_print(tree->enable_debug, "Internal node built with %d children\n",
                               children_count);
        }
        free(level);
        level = parent_level;
        level_nodes = parent_nodes;
        current_height++;
    }
    tree->root = level[0];
    tree->height = current_height;
    tree->count = n_items;
    free(level);
    bptree_debug_print(tree->enable_debug, "Bulk load completed: tree height = %d\n", tree->height);
    return BPTREE_OK;
}

static bptree_value_t *bptree_get_range(const bptree *tree, const bptree_key_t *start,
                                        const bptree_key_t *end, int *n_results) {
    if (!tree || !tree->root || !start || !end || !n_results) return NULL;
    bptree_node *node = tree->root;
    while (!node->is_leaf) {
        int pos = bptree_node_search(tree, node, start);
        node = bptree_node_children(node, tree->max_keys)[pos];
    }
    int count = 0;
    bptree_node *curr = node;
    while (curr) {
        bptree_key_t *keys = bptree_node_keys(curr);
        for (int i = 0; i < curr->num_keys; i++) {
            if (tree->compare(&keys[i], start) < 0) continue;
            if (tree->compare(&keys[i], end) > 0) goto done;
            count++;
        }
        curr = curr->next;
    }
done:;
    bptree_value_t *results = malloc(count * sizeof(bptree_value_t));
    if (!results) return NULL;
    int index = 0;
    curr = node;
    while (curr) {
        bptree_key_t *keys = bptree_node_keys(curr);
        bptree_value_t *values = bptree_node_values(curr, tree->max_keys);
        for (int i = 0; i < curr->num_keys; i++) {
            if (tree->compare(&keys[i], start) < 0) continue;
            if (tree->compare(&keys[i], end) > 0) goto done2;
            results[index++] = values[i];
        }
        curr = curr->next;
    }
done2:
    *n_results = index;
    return results;
}

/* --------------------------
   Statistics & Invariants
----------------------------*/
static int bptree_count_nodes(const bptree_node *node, const bptree *tree) {
    if (!node) return 0;
    if (node->is_leaf) return 1;
    int cnt = 1;
    /* Cast away const for internal pointer access; node content is read-only here */
    bptree_node **children = bptree_node_children((bptree_node *)node, tree->max_keys);
    for (int i = 0; i <= node->num_keys; i++) {
        cnt += bptree_count_nodes(children[i], tree);
    }
    return cnt;
}

static bptree_stats bptree_get_stats(const bptree *tree) {
    bptree_stats stats;
    stats.count = tree->count;
    stats.height = tree->height;
    stats.node_count = bptree_count_nodes(tree->root, tree);
    return stats;
}

static bool bptree_check_invariants_node(bptree_node *node, bptree *tree, int depth,
                                         int *leaf_depth) {
    bptree_key_t *keys = bptree_node_keys(node);
    for (int i = 1; i < node->num_keys; i++) {
        if (tree->compare(&keys[i - 1], &keys[i]) > 0) {
            fprintf(stderr, "Invariant violation: keys not sorted at depth %d\n", depth);
            return false;
        }
    }
    if (node->is_leaf) {
        if (*leaf_depth == -1)
            *leaf_depth = depth;
        else if (depth != *leaf_depth) {
            fprintf(stderr, "Invariant violation: leaf at depth %d (expected %d)\n", depth,
                    *leaf_depth);
            return false;
        }
        return true;
    }
    bptree_node **children = bptree_node_children(node, tree->max_keys);
    for (int i = 1; i <= node->num_keys; i++) {
        bptree_key_t smallest = bptree_find_smallest_key(children[i], tree->max_keys);
        if (tree->compare(&keys[i - 1], &smallest) != 0) {
            fprintf(stderr,
                    "Invariant violation: parent's key[%d] != smallest key of child at depth %d\n",
                    i - 1, depth + 1);
            return false;
        }
        if (!bptree_check_invariants_node(children[i], tree, depth + 1, leaf_depth)) return false;
    }
    if (!bptree_check_invariants_node(children[0], tree, depth + 1, leaf_depth)) return false;
    return true;
}

static bool bptree_check_invariants(const bptree *tree) {
    if (!tree || !tree->root) return false;
    int leaf_depth = -1;
    return bptree_check_invariants_node(tree->root, (bptree *)tree, 0, &leaf_depth);
}

static void bptree_assert_invariants(const bptree *tree) {
    if (!bptree_check_invariants(tree)) {
        fprintf(stderr, "B+ tree invariants violated!\n");
        abort();
    }
}

/* --------------------------
   Iterator
----------------------------*/
static bptree_iterator *bptree_iterator_new(const bptree *tree) {
    bptree_iterator *iter = malloc(sizeof(bptree_iterator));
    if (!iter) return NULL;
    bptree_node *node = tree->root;
    while (!node->is_leaf) node = bptree_node_children(node, tree->max_keys)[0];
    iter->current_leaf = node;
    iter->index = 0;
    iter->tree = (bptree *)tree;
    return iter;
}

static bptree_value_t bptree_iterator_next(bptree_iterator *iter) {
    if (!iter || !iter->current_leaf) return NULL;
    if (iter->index < iter->current_leaf->num_keys)
        return bptree_node_values(iter->current_leaf, iter->tree->max_keys)[iter->index++];
    iter->current_leaf = iter->current_leaf->next;
    iter->index = 0;
    if (!iter->current_leaf) return NULL;
    return bptree_node_values(iter->current_leaf, iter->tree->max_keys)[iter->index++];
}

static void bptree_iterator_destroy(bptree_iterator *iter) {
    if (iter) free(iter);
}

#endif /* BPTREE_IMPLEMENTATION */

#endif /* BPTREE_H */
