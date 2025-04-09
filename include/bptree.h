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

#include <stdalign.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#if defined(BPTREE_KEY_TYPE_STRING)
#ifndef BPTREE_KEY_SIZE
#error "BPTREE_KEY_SIZE must be defined for fixed-size string keys"
#endif
typedef struct {
    char data[BPTREE_KEY_SIZE];
} bptree_key_t;
#elif defined(BPTREE_KEY_TYPE_FLOAT)
#ifndef BPTREE_FLOAT_TYPE
#define BPTREE_FLOAT_TYPE double
#endif
typedef BPTREE_FLOAT_TYPE bptree_key_t;
#elif defined(BPTREE_KEY_TYPE_INT)
#ifndef BPTREE_INT_TYPE
#define BPTREE_INT_TYPE long int
#endif
typedef BPTREE_INT_TYPE bptree_key_t;
#else
#ifndef BPTREE_INT_TYPE
#define BPTREE_INT_TYPE long int
#endif
typedef BPTREE_INT_TYPE bptree_key_t;
#endif

#ifndef BPTREE_VALUE_TYPE
#define BPTREE_VALUE_TYPE void *
#endif
typedef BPTREE_VALUE_TYPE bptree_value_t;

typedef void *(*bptree_alloc_func)(size_t size, void *ctx);
typedef void (*bptree_free_func)(void *ptr, size_t size, void *ctx);
typedef void *(*bptree_realloc_func)(void *ptr, size_t old_size, size_t new_size, void *ctx);
typedef struct bptree_config {
    int max_keys;
    int (*compare)(const bptree_key_t *, const bptree_key_t *);
    bool enable_debug;
    bptree_alloc_func alloc;
    bptree_free_func free;
    bptree_realloc_func realloc;
    void *allocator_ctx;
} bptree_config;

typedef enum {
    BPTREE_OK = 0,
    BPTREE_DUPLICATE_KEY,
    BPTREE_KEY_NOT_FOUND,
    BPTREE_ALLOCATION_FAILURE,
    BPTREE_INVALID_ARGUMENT,
    BPTREE_BULK_LOAD_NOT_SORTED,
    BPTREE_BULK_LOAD_DUPLICATE,
    BPTREE_ITERATOR_END,
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
    bptree_alloc_func alloc;
    bptree_free_func free_func;
    bptree_realloc_func realloc;
    void *allocator_ctx;
} bptree;

typedef struct bptree_stats {
    int count;
    int height;
    int node_count;
} bptree_stats;

typedef struct bptree_iterator {
    bptree_node *current_leaf;
    int index;
    bptree *tree;
} bptree_iterator;

BPTREE_API bptree *bptree_create(int max_keys,
                                 int (*compare)(const bptree_key_t *, const bptree_key_t *),
                                 bool enable_debug);
BPTREE_API bptree *bptree_create_config(const bptree_config *config);
BPTREE_API void bptree_free(bptree *tree);
BPTREE_API bptree_status bptree_insert(bptree *tree, const bptree_key_t *key, bptree_value_t value);
BPTREE_API bptree_status bptree_upsert(bptree *tree, const bptree_key_t *key, bptree_value_t value);
BPTREE_API bptree_status bptree_get_value(const bptree *tree, const bptree_key_t *key,
                                          bptree_value_t *out_value);
BPTREE_API bptree_status bptree_remove(bptree *tree, const bptree_key_t *key);
BPTREE_API bptree_status bptree_bulk_load(bptree *tree, const bptree_key_t keys[],
                                          const bptree_value_t values[], int n_items);
BPTREE_API bptree_status bptree_get_range(const bptree *tree, const bptree_key_t *start,
                                          const bptree_key_t *end, bptree_value_t **out_values,
                                          int *n_results);
BPTREE_API void bptree_free_range_results(bptree *tree, bptree_value_t *results, int n_results);
BPTREE_API bptree_stats bptree_get_stats(const bptree *tree);
BPTREE_API bool bptree_check_invariants(const bptree *tree);
BPTREE_API void bptree_assert_invariants(const bptree *tree);
BPTREE_API bptree_iterator *bptree_iterator_new(const bptree *tree);
BPTREE_API bool bptree_iterator_has_next(const bptree_iterator *iter);
BPTREE_API bptree_status bptree_iterator_next(bptree_iterator *iter, bptree_value_t *out_value);
BPTREE_API void bptree_iterator_free(bptree_iterator *iter);
BPTREE_API bool bptree_contains(const bptree *tree, const bptree_key_t *key);
BPTREE_API int bptree_size(const bptree *tree);

static bptree_key_t *bptree_node_keys(bptree_node *node);
static bptree_value_t *bptree_node_values(bptree_node *node, int max_keys);
static bptree_node **bptree_node_children(bptree_node *node, int max_keys);
static int bptree_default_compare_numeric(const bptree_key_t *a, const bptree_key_t *b);
static void bptree_free_node(bptree_node *node, bptree *tree);
static int bptree_count_nodes(const bptree_node *node, const bptree *tree);
static bptree_key_t bptree_find_smallest_key(bptree_node *node, int max_keys);
static bptree_key_t bptree_find_largest_key(bptree_node *node, int max_keys);
static bool bptree_check_invariants_node(bptree_node *node, bptree *tree, int depth,
                                         int *leaf_depth);

#ifdef BPTREE_IMPLEMENTATION

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

#define BPTREE_MAX_ALIGN(a, b) ((a) > (b) ? (a) : (b))
static size_t bptree_keys_area_size(int max_keys) {
    size_t keys_size = max_keys * sizeof(bptree_key_t);
    size_t val_align = alignof(bptree_value_t);
    size_t child_align = alignof(bptree_node *);
    size_t required_alignment = BPTREE_MAX_ALIGN(val_align, child_align);
    size_t pad = (required_alignment - (keys_size % required_alignment)) % required_alignment;
    return keys_size + pad;
}

static void *bptree_default_alloc(size_t size, void *ctx) {
    (void)ctx;
    return malloc(size);
}
static void bptree_default_free(void *ptr, size_t size, void *ctx) {
    (void)size;
    (void)ctx;
    free(ptr);
}
static void *bptree_default_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx) {
    (void)old_size;
    (void)ctx;
    return realloc(ptr, new_size);
}

static void *bptree_alloc_mem(bptree *tree, size_t size, size_t alignment) {
    size_t alloc_size = (size + alignment - 1) & ~(alignment - 1);
    if (tree->alloc) return tree->alloc(alloc_size, tree->allocator_ctx);
    return aligned_alloc(alignment, alloc_size);
}

static void bptree_free_mem(bptree *tree, void *ptr, size_t size) {
    if (tree->free_func)
        tree->free_func(ptr, size, tree->allocator_ctx);
    else
        free(ptr);
}

static void *bptree_realloc_mem(bptree *tree, void *ptr, size_t old_size, size_t new_size,
                                size_t alignment) {
    size_t new_alloc_size = (new_size + alignment - 1) & ~(alignment - 1);
    if (tree->realloc) return tree->realloc(ptr, old_size, new_alloc_size, tree->allocator_ctx);
    void *new_ptr = bptree_alloc_mem(tree, new_size, alignment);
    if (new_ptr) {
        memcpy(new_ptr, ptr, old_size < new_size ? old_size : new_size);
        bptree_free_mem(tree, ptr, old_size);
    }
    return new_ptr;
}

static bptree_key_t *bptree_node_keys(bptree_node *node) { return (bptree_key_t *)node->data; }

static bptree_value_t *bptree_node_values(bptree_node *node, int max_keys) {
    size_t offset = bptree_keys_area_size(max_keys);
    return (bptree_value_t *)(node->data + offset);
}

static bptree_node **bptree_node_children(bptree_node *node, int max_keys) {
    size_t offset = bptree_keys_area_size(max_keys);
    return (bptree_node **)(node->data + offset);
}

static int bptree_default_compare_numeric(const bptree_key_t *a, const bptree_key_t *b) {
    return (*a < *b) ? -1 : ((*a > *b) ? 1 : 0);
}

static size_t bptree_node_alloc_size(bptree *tree, bool is_leaf) {
    int max_keys = tree->max_keys;
    size_t offset = bptree_keys_area_size(max_keys);
    size_t data_size = is_leaf ? offset + ((size_t)max_keys * sizeof(bptree_value_t))
                               : offset + (((size_t)max_keys + 1) * sizeof(bptree_node *));
    size_t required_size = sizeof(bptree_node) + data_size;
    size_t alignment = alignof(bptree_node);
    return (required_size + alignment - 1) & ~(alignment - 1);
}

static bptree_node *bptree_node_alloc(bptree *tree, bool is_leaf) {
    size_t alloc_size = bptree_node_alloc_size(tree, is_leaf);
    bptree_node *node = bptree_alloc_mem(tree, alloc_size, alignof(bptree_node));
    if (node) {
        node->is_leaf = is_leaf;
        node->num_keys = 0;
        if (is_leaf) node->next = NULL;
    } else {
        bptree_debug_print(tree->enable_debug, "ERROR: allocation failed.\n");
    }
    return node;
}

static bptree_key_t bptree_find_smallest_key(bptree_node *node, int max_keys) {
    while (!node->is_leaf) node = bptree_node_children(node, max_keys)[0];
    return bptree_node_keys(node)[0];
}

static bptree_key_t bptree_find_largest_key(bptree_node *node, int max_keys) {
    while (!node->is_leaf) {
        node = bptree_node_children(node, max_keys)[node->num_keys];
    }
    /* Optionally add an assertion that node->num_keys > 0 */
    return (node->num_keys > 0) ? bptree_node_keys(node)[node->num_keys - 1] : (bptree_key_t){0};
}

static void bptree_free_node(bptree_node *node, bptree *tree) {
    if (!node) return;
    if (!node->is_leaf) {
        bptree_node **children = bptree_node_children(node, tree->max_keys);
        for (int i = 0; i <= node->num_keys; i++) {
            bptree_free_node(children[i], tree);
        }
    }
    size_t node_size = bptree_node_alloc_size(tree, node->is_leaf);
    bptree_free_mem(tree, node, node_size);
}

static int bptree_count_nodes(const bptree_node *node, const bptree *tree) {
    if (!node) return 0;
    if (node->is_leaf) return 1;
    int cnt = 1;
    bptree_node **children = bptree_node_children((bptree_node *)node, tree->max_keys);
    for (int i = 0; i <= node->num_keys; i++) {
        cnt += bptree_count_nodes(children[i], tree);
    }
    return cnt;
}

static bool bptree_check_invariants_node(bptree_node *node, bptree *tree, int depth,
                                         int *leaf_depth) {
    if (!node) return false;

    bptree_key_t *keys = bptree_node_keys(node);

    for (int i = 1; i < node->num_keys; i++) {
        if (tree->compare(&keys[i - 1], &keys[i]) >= 0) {
            fprintf(stderr,
                    "Invariant violation: Keys not strictly sorted at depth %d (node %p, key %d vs "
                    "%d)\n",
                    depth, (void *)node, i - 1, i);
            return false;
        }
    }

    if (node->is_leaf) {
        if (*leaf_depth == -1)
            *leaf_depth = depth;
        else if (depth != *leaf_depth) {
            fprintf(stderr, "Invariant violation: Leaf at depth %d (node %p), expected %d\n", depth,
                    (void *)node, *leaf_depth);
            return false;
        }
        bool is_root = (tree->root == node);
        if (!is_root && (node->num_keys < tree->min_leaf_keys || node->num_keys > tree->max_keys)) {
            fprintf(stderr,
                    "Invariant violation: Leaf node key count %d out of range [%d, %d] at depth %d "
                    "(node %p)\n",
                    node->num_keys, tree->min_leaf_keys, tree->max_keys, depth, (void *)node);
            return false;
        }
        if (is_root && node->num_keys > tree->max_keys) {
            fprintf(stderr,
                    "Invariant violation: Root leaf node key count %d exceeds max %d (node %p)\n",
                    node->num_keys, tree->max_keys, (void *)node);
            return false;
        }
        return true;
    }

    bool is_root = (tree->root == node);
    if (!is_root && (node->num_keys < tree->min_internal_keys || node->num_keys > tree->max_keys)) {
        fprintf(stderr,
                "Invariant violation: Internal node key count %d out of range [%d, %d] at depth %d "
                "(node %p)\n",
                node->num_keys, tree->min_internal_keys, tree->max_keys, depth, (void *)node);
        return false;
    }
    if (is_root && (node->num_keys < 1 || node->num_keys > tree->max_keys) && tree->count > 0) {
        fprintf(stderr,
                "Invariant violation: Root internal node key count %d out of range [1, %d] (tree "
                "count %d) (node %p)\n",
                node->num_keys, tree->max_keys, tree->count, (void *)node);
        return false;
    }

    bptree_node **children = bptree_node_children(node, tree->max_keys);
    if (node->num_keys > 0) {
        bptree_node *child0 = children[0];
        if (!child0) {
            fprintf(stderr, "Invariant violation: Internal node child 0 is NULL (node %p)\n",
                    (void *)node);
            return false;
        }
        bptree_key_t max_in_child0 = bptree_find_largest_key(child0, tree->max_keys);
        if (child0->num_keys > 0 && tree->compare(&max_in_child0, &keys[0]) >= 0) {
            fprintf(stderr,
                    "Invariant violation: Max key in child 0 >= key[0] at depth %d (node %p)\n",
                    depth, (void *)node);
            return false;
        }
        if (!bptree_check_invariants_node(child0, tree, depth + 1, leaf_depth)) return false;
    }

    for (int i = 1; i <= node->num_keys; i++) {
        bptree_node *child_i = children[i];
        if (!child_i) {
            fprintf(stderr, "Invariant violation: Internal node child %d is NULL (node %p)\n", i,
                    (void *)node);
            return false;
        }
        bptree_key_t min_in_child_i = bptree_find_smallest_key(child_i, tree->max_keys);
        if (child_i->num_keys > 0 && tree->compare(&keys[i - 1], &min_in_child_i) != 0) {
            fprintf(stderr,
                    "Invariant violation: Parent key[%d] != smallest key in child %d at depth %d "
                    "(node %p)\n",
                    i - 1, i, depth, (void *)node);
            return false;
        }
        if (i < node->num_keys) {
            bptree_key_t max_in_child_i = bptree_find_largest_key(child_i, tree->max_keys);
            if (child_i->num_keys > 0 && tree->compare(&max_in_child_i, &keys[i]) >= 0) {
                fprintf(
                    stderr,
                    "Invariant violation: Max key in child %d >= key[%d] at depth %d (node %p)\n",
                    i, i, depth, (void *)node);
                return false;
            }
        }
        if (!bptree_check_invariants_node(child_i, tree, depth + 1, leaf_depth)) return false;
    }

    return true;
}

BPTREE_API inline bptree *bptree_create_config(const bptree_config *config) {
    if (!config || config->max_keys < 3) return NULL;
    bptree *tree = config->alloc ? config->alloc(sizeof(bptree), config->allocator_ctx)
                                 : malloc(sizeof(bptree));
    if (!tree) return NULL;
    tree->count = 0;
    tree->height = 1;
    tree->enable_debug = config->enable_debug;
    tree->max_keys = config->max_keys;
    tree->min_leaf_keys = (config->max_keys + 1) / 2;
    if (config->max_keys % 2 == 0)
        tree->min_internal_keys = (config->max_keys / 2) - 1;
    else
        tree->min_internal_keys = ((config->max_keys + 2) / 2) - 1;
#ifdef BPTREE_KEY_TYPE_STRING
    tree->compare = config->compare ? config->compare : string_key_compare;
#else
    tree->compare = config->compare ? config->compare : bptree_default_compare_numeric;
#endif
    tree->alloc = config->alloc ? config->alloc : bptree_default_alloc;
    tree->free_func = config->free ? config->free : bptree_default_free;
    tree->realloc = config->realloc ? config->realloc : bptree_default_realloc;
    tree->allocator_ctx = config->allocator_ctx;
    tree->root = bptree_node_alloc(tree, true);
    if (!tree->root) {
        tree->free_func(tree, sizeof(bptree), tree->allocator_ctx);
        return NULL;
    }
    bptree_debug_print(tree->enable_debug, "Tree created (max_keys=%d)\n", tree->max_keys);
    return tree;
}

BPTREE_API inline bptree *bptree_create(int max_keys,
                                        int (*compare)(const bptree_key_t *, const bptree_key_t *),
                                        bool enable_debug) {
    bptree_config config = {max_keys, compare, enable_debug, NULL, NULL, NULL, NULL};
    return bptree_create_config(&config);
}

BPTREE_API inline void bptree_free(bptree *tree) {
    if (!tree) return;
    bptree_free_node(tree->root, tree);
    bptree_free_mem(tree, tree, sizeof(bptree));
}

static int bptree_node_search(const bptree *tree, bptree_node *node, const bptree_key_t *key) {
    int low = 0, high = node->num_keys - 1;
    bptree_key_t *keys = bptree_node_keys(node);
    while (low <= high) {
        int mid = low + (high - low) / 2;
        int cmp = tree->compare(key, &keys[mid]);
        if (cmp == 0) return node->is_leaf ? mid : (mid + 1);
        if (cmp < 0)
            high = mid - 1;
        else
            low = mid + 1;
    }
    return low;
}

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
            bptree_node_values(node, tree->max_keys)[i] = values[i - 1];
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
        /* Updated shifting: first shift keys */
        for (int i = node->num_keys; i > pos; i--) {
            keys[i] = keys[i - 1];
        }
        /* Then shift child pointers; there are node->num_keys+1 children */
        bptree_node **children = bptree_node_children(node, tree->max_keys);
        for (int i = node->num_keys + 1; i > pos + 1; i--) {
            children[i] = children[i - 1];
        }
        keys[pos] = child_promoted;
        children[pos + 1] = child_new;
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
            bptree_debug_print(tree->enable_debug, "Internal split: promoted key set\n");
        }
    }
    return status;
}

BPTREE_API inline bptree_status bptree_insert(bptree *tree, const bptree_key_t *key,
                                              bptree_value_t value) {
    bptree_key_t promoted_key;
    bptree_node *new_child = NULL;
    bptree_status status =
        bptree_insert_internal(tree, tree->root, key, value, &promoted_key, &new_child);
    if (status != BPTREE_OK) return status;
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

BPTREE_API inline bptree_status bptree_upsert(bptree *tree, const bptree_key_t *key,
                                              bptree_value_t value) {
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
    return bptree_insert(tree, key, value);
}

BPTREE_API inline bptree_status bptree_get_value(const bptree *tree, const bptree_key_t *key,
                                                 bptree_value_t *out_value) {
    if (!tree || !tree->root || !key || !out_value) return BPTREE_INVALID_ARGUMENT;
    bptree_node *node = tree->root;
    while (!node->is_leaf) {
        int pos = bptree_node_search(tree, node, key);
        node = bptree_node_children(node, tree->max_keys)[pos];
    }
    int pos = bptree_node_search(tree, node, key);
    bptree_key_t *keys = bptree_node_keys(node);
    if (pos < node->num_keys && tree->compare(key, &keys[pos]) == 0) {
        *out_value = bptree_node_values(node, tree->max_keys)[pos];
        return BPTREE_OK;
    }
    return BPTREE_KEY_NOT_FOUND;
}

static void bptree_redistribute_from_right(bptree *tree, bptree_node *parent,
                                           bptree_node *underfull_node, bptree_node *right_sibling,
                                           int underfull_node_idx) {
    bptree_key_t *parent_keys = bptree_node_keys(parent);
    bptree_key_t *underfull_keys = bptree_node_keys(underfull_node);
    bptree_key_t *right_keys = bptree_node_keys(right_sibling);

    if (underfull_node->is_leaf) {
        // --- Leaf Case ---
        bptree_value_t *underfull_values = bptree_node_values(underfull_node, tree->max_keys);
        bptree_value_t *right_values = bptree_node_values(right_sibling, tree->max_keys);

        // Borrow key/value from right sibling's start
        underfull_keys[underfull_node->num_keys] = right_keys[0];
        underfull_values[underfull_node->num_keys] = right_values[0];
        underfull_node->num_keys++;

        // Shift keys/values left in right sibling
        right_sibling->num_keys--;  // Decrement count first
        memmove(right_keys, right_keys + 1, right_sibling->num_keys * sizeof(bptree_key_t));
        memmove(bptree_node_values(right_sibling, tree->max_keys),
                bptree_node_values(right_sibling, tree->max_keys) + 1,
                right_sibling->num_keys * sizeof(bptree_value_t));

        // Update parent separator key: It should be the NEW smallest key in the right sibling.
        // After the shift, this is simply right_keys[0].
        parent_keys[underfull_node_idx] = right_keys[0];

        bptree_debug_print(tree->enable_debug, "Redistributed from right leaf\n");

    } else {
        // --- Internal Node Case ---
        bptree_node **underfull_children = bptree_node_children(underfull_node, tree->max_keys);
        bptree_node **right_children = bptree_node_children(right_sibling, tree->max_keys);

        // The key right_keys[0] (before shift) will become the new parent separator.
        bptree_key_t new_parent_separator = right_keys[0];

        // Move separator from parent down to end of underfull node
        underfull_keys[underfull_node->num_keys] = parent_keys[underfull_node_idx];
        underfull_node->num_keys++;

        // Move first child pointer from right sibling to end of underfull node's children
        underfull_children[underfull_node->num_keys] = right_children[0];

        // Shift keys and children left in the right sibling
        right_sibling->num_keys--;  // Decrement count first
        memmove(right_keys, right_keys + 1, right_sibling->num_keys * sizeof(bptree_key_t));
        // Number of children remaining is num_keys + 1
        memmove(right_children, right_children + 1,
                (right_sibling->num_keys + 1) * sizeof(bptree_node *));

        // Update the parent separator key with the key identified BEFORE the shift in right
        // sibling.
        parent_keys[underfull_node_idx] = new_parent_separator;

        bptree_debug_print(tree->enable_debug, "Redistributed from right internal\n");
    }
}

static void bptree_redistribute_from_left(bptree *tree, bptree_node *parent,
                                          bptree_node *underfull_node, bptree_node *left_sibling,
                                          int underfull_node_idx) {
    bptree_key_t *parent_keys = bptree_node_keys(parent);
    bptree_key_t *underfull_keys = bptree_node_keys(underfull_node);
    bptree_key_t *left_keys = bptree_node_keys(left_sibling);

    if (underfull_node->is_leaf) {
        // --- Leaf Case ---
        bptree_value_t *underfull_values = bptree_node_values(underfull_node, tree->max_keys);
        bptree_value_t *left_values = bptree_node_values(left_sibling, tree->max_keys);

        // Make space in underfull node at the beginning
        memmove(underfull_keys + 1, underfull_keys,
                underfull_node->num_keys * sizeof(bptree_key_t));
        memmove(underfull_values + 1, underfull_values,
                underfull_node->num_keys * sizeof(bptree_value_t));

        // Move last key/value from left sibling to start of underfull node
        underfull_keys[0] = left_keys[left_sibling->num_keys - 1];
        underfull_values[0] = left_values[left_sibling->num_keys - 1];
        underfull_node->num_keys++;
        left_sibling->num_keys--;

        // Update parent separator key: It's the new smallest key in the underfull node.
        // Simplified: parent_keys[underfull_node_idx - 1] = underfull_keys[0];
        // Original: parent_keys[underfull_node_idx - 1] = bptree_find_smallest_key(underfull_node,
        // tree->max_keys); Let's use the simplified version for leaves.
        parent_keys[underfull_node_idx - 1] = underfull_keys[0];

        bptree_debug_print(tree->enable_debug, "Redistributed from left leaf\n");

    } else {
        // --- Internal Node Case ---
        bptree_node **underfull_children = bptree_node_children(underfull_node, tree->max_keys);
        bptree_node **left_children = bptree_node_children(left_sibling, tree->max_keys);

        // Make space in underfull node keys and children at the beginning
        memmove(underfull_keys + 1, underfull_keys,
                underfull_node->num_keys * sizeof(bptree_key_t));
        memmove(underfull_children + 1, underfull_children,
                (underfull_node->num_keys + 1) * sizeof(bptree_node *));

        // Move separator from parent down to start of underfull node keys
        underfull_keys[0] = parent_keys[underfull_node_idx - 1];
        underfull_node->num_keys++;  // Increment key count first

        // Move last child pointer from left sibling to start of underfull node children
        underfull_children[0] = left_children[left_sibling->num_keys];
        // Note: Left sibling keeps num_keys keys, but loses the (num_keys+1)th child pointer.

        // Update parent key: It becomes the smallest key in the subtree moved from the left
        // sibling. This smallest key is found by traversing underfull_children[0]. The call to
        // find_smallest_key(underfull_node,...) correctly does this traversal.
        bptree_key_t new_parent_separator =
            bptree_find_smallest_key(underfull_node, tree->max_keys);

        // Update left sibling's key count
        left_sibling->num_keys--;

        // Assign the correctly calculated new parent separator
        parent_keys[underfull_node_idx - 1] = new_parent_separator;

        bptree_debug_print(tree->enable_debug, "Redistributed from left internal\n");
    }
}

static bptree_node *bptree_merge_nodes(bptree *tree, bptree_node *left_node,
                                       bptree_node *right_node, int left_node_idx,
                                       const bptree_key_t *separator) {
    bptree_key_t *left_keys = bptree_node_keys(left_node);
    bptree_key_t *right_keys = bptree_node_keys(right_node);
    bptree_debug_print(tree->enable_debug, "Merging nodes (left_idx=%d)\n", left_node_idx);
    if (left_node->is_leaf) {
        bptree_value_t *left_values = bptree_node_values(left_node, tree->max_keys);
        bptree_value_t *right_values = bptree_node_values(right_node, tree->max_keys);
        memcpy(left_keys + left_node->num_keys, right_keys,
               right_node->num_keys * sizeof(bptree_key_t));
        memcpy(left_values + left_node->num_keys, right_values,
               right_node->num_keys * sizeof(bptree_value_t));
        left_node->num_keys += right_node->num_keys;
        left_node->next = right_node->next;
    } else {
        bptree_node **left_children = bptree_node_children(left_node, tree->max_keys);
        bptree_node **right_children = bptree_node_children(right_node, tree->max_keys);
        /* Incorporate the parent's separator key into the merged node */
        left_keys[left_node->num_keys] = *separator;
        left_node->num_keys++;
        memcpy(left_keys + left_node->num_keys, right_keys,
               right_node->num_keys * sizeof(bptree_key_t));
        memcpy(left_children + left_node->num_keys, right_children,
               (right_node->num_keys + 1) * sizeof(bptree_node *));
        left_node->num_keys += right_node->num_keys;
    }
    size_t right_node_size = bptree_node_alloc_size(tree, right_node->is_leaf);
    bptree_free_mem(tree, right_node, right_node_size);
    return left_node;
}

BPTREE_API inline bptree_status bptree_remove(bptree *tree, const bptree_key_t *key) {
#define BPTREE_MAX_HEIGHT 64
    bptree_node *node_stack[BPTREE_MAX_HEIGHT];
    int index_stack[BPTREE_MAX_HEIGHT];
    int depth = 0;

    if (!tree || !tree->root || !key) return BPTREE_INVALID_ARGUMENT;

    bptree_node *node = tree->root;
    /* Traverse down to the leaf using standard bptree_node_search.
       (No extra adjustment is applied here.)
    */
    while (!node->is_leaf) {
        if (depth >= BPTREE_MAX_HEIGHT - 1) {
            fprintf(stderr, "BPTree Error: Tree height exceeds max supported (%d)\n",
                    BPTREE_MAX_HEIGHT);
            return BPTREE_INTERNAL_ERROR;
        }
        int pos = bptree_node_search(tree, node, key);
        node_stack[depth] = node;
        index_stack[depth] = pos;
        depth++;
        node = bptree_node_children(node, tree->max_keys)[pos];
    }

    int key_pos = bptree_node_search(tree, node, key);
    bptree_key_t *keys = bptree_node_keys(node);
    if (key_pos >= node->num_keys || tree->compare(key, &keys[key_pos]) != 0)
        return BPTREE_KEY_NOT_FOUND;
    bptree_value_t *values = bptree_node_values(node, tree->max_keys);
    memmove(&keys[key_pos], &keys[key_pos + 1],
            (node->num_keys - 1 - key_pos) * sizeof(bptree_key_t));
    memmove(&values[key_pos], &values[key_pos + 1],
            (node->num_keys - 1 - key_pos) * sizeof(bptree_value_t));
    node->num_keys--;
    tree->count--;
    bptree_debug_print(tree->enable_debug, "Key removed from leaf, count=%d\n", tree->count);

    /* Upward rebalancing */
    bptree_node *current_child = node;
    for (int d = depth - 1; d >= 0; d--) {
        bptree_node *parent = node_stack[d];
        int child_idx = index_stack[d];
        int min_keys = current_child->is_leaf ? tree->min_leaf_keys : tree->min_internal_keys;
        // Rebalance if current_child is not the root and underflowed
        if (current_child != tree->root && current_child->num_keys < min_keys) {
            bptree_debug_print(
                tree->enable_debug,
                "Underflow detected at depth %d (node index %d); num_keys=%d, min_keys=%d\n", d + 1,
                child_idx, current_child->num_keys, min_keys);
            bptree_node **parent_children = bptree_node_children(parent, tree->max_keys);
            bptree_key_t *parent_keys = bptree_node_keys(parent);
            if (child_idx < parent->num_keys) {
                // Attempt to fix underflow with right sibling.
                bptree_node *right_sibling = parent_children[child_idx + 1];
                int right_min_keys =
                    right_sibling->is_leaf ? tree->min_leaf_keys : tree->min_internal_keys;
                if (right_sibling->num_keys > right_min_keys) {
                    bptree_debug_print(tree->enable_debug,
                                       "Attempting redistribute from right sibling (idx %d)\n",
                                       child_idx + 1);
                    bptree_redistribute_from_right(tree, parent, current_child, right_sibling,
                                                   child_idx);
                } else {
                    bptree_debug_print(tree->enable_debug,
                                       "Attempting merge with right sibling (idx %d)\n",
                                       child_idx + 1);
                    current_child = bptree_merge_nodes(tree, current_child, right_sibling,
                                                       child_idx, &parent_keys[child_idx]);
                    memmove(&parent_keys[child_idx], &parent_keys[child_idx + 1],
                            (parent->num_keys - 1 - child_idx) * sizeof(bptree_key_t));
                    memmove(&parent_children[child_idx + 1], &parent_children[child_idx + 2],
                            (parent->num_keys - child_idx) * sizeof(bptree_node *));
                    parent->num_keys--;
                    // Update the separator key at index child_idx if applicable.
                    if (child_idx < parent->num_keys) {
                        parent_keys[child_idx] = bptree_find_smallest_key(
                            parent_children[child_idx + 1], tree->max_keys);
                    }
                    index_stack[d] = child_idx;
                }
            } else if (child_idx > 0) {
                // Attempt to fix underflow with left sibling.
                bptree_node *left_sibling = parent_children[child_idx - 1];
                int left_min_keys =
                    left_sibling->is_leaf ? tree->min_leaf_keys : tree->min_internal_keys;
                if (left_sibling->num_keys > left_min_keys) {
                    bptree_debug_print(tree->enable_debug,
                                       "Attempting redistribute from left sibling (idx %d)\n",
                                       child_idx - 1);
                    bptree_redistribute_from_left(tree, parent, current_child, left_sibling,
                                                  child_idx);
                } else {
                    bptree_debug_print(tree->enable_debug,
                                       "Attempting merge with left sibling (idx %d)\n",
                                       child_idx - 1);
                    current_child = bptree_merge_nodes(tree, left_sibling, current_child,
                                                       child_idx - 1, &parent_keys[child_idx - 1]);
                    memmove(&parent_keys[child_idx - 1], &parent_keys[child_idx],
                            (parent->num_keys - child_idx) * sizeof(bptree_key_t));
                    memmove(&parent_children[child_idx], &parent_children[child_idx + 1],
                            (parent->num_keys - child_idx) * sizeof(bptree_node *));
                    parent->num_keys--;
                    if ((child_idx - 1) < parent->num_keys) {
                        parent_keys[child_idx - 1] =
                            bptree_find_smallest_key(parent_children[child_idx], tree->max_keys);
                    }
                    index_stack[d] = child_idx - 1;
                }
            }
        } else {
            bptree_debug_print(tree->enable_debug,
                               "Node at depth %d is balanced or is root; stopping upward check.\n",
                               d + 1);
            break;
        }
        current_child = parent;
    }

    /* Collapse the root if necessary */
    if (tree->root && !tree->root->is_leaf && tree->root->num_keys == 0 && tree->count > 0) {
        bptree_node *old_root = tree->root;
        tree->root = bptree_node_children(old_root, tree->max_keys)[0];
        tree->height--;
        size_t old_root_size = bptree_node_alloc_size(tree, old_root->is_leaf);
        bptree_free_mem(tree, old_root, old_root_size);
        bptree_debug_print(tree->enable_debug, "Collapsed root; new height=%d\n", tree->height);
    } else if (tree->count == 0 && tree->root) {
        if (!tree->root->is_leaf || tree->root->num_keys != 0) {
            bptree_free_node(tree->root, tree);
            tree->root = bptree_node_alloc(tree, true);
            if (!tree->root) return BPTREE_ALLOCATION_FAILURE;
            tree->height = 1;
            bptree_debug_print(tree->enable_debug, "Tree emptied; created new empty leaf root.\n");
        }
    }

#undef BPTREE_MAX_HEIGHT
    return BPTREE_OK;
}

BPTREE_API inline bptree_status bptree_bulk_load(bptree *tree, const bptree_key_t keys[],
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
    bptree_node **level =
        bptree_alloc_mem(tree, num_leaves * sizeof(bptree_node *), alignof(bptree_node *));
    if (!level) return BPTREE_ALLOCATION_FAILURE;
    memset(level, 0, num_leaves * sizeof(bptree_node *));

    int idx = 0;
    for (int i = 0; i < num_leaves; i++) {
        bptree_node *leaf = bptree_node_alloc(tree, true);
        if (!leaf) {
            for (int j = 0; j < i; j++) {
                size_t node_size = bptree_node_alloc_size(tree, level[j]->is_leaf);
                bptree_free_mem(tree, level[j], node_size);
            }
            bptree_free_mem(tree, level, num_leaves * sizeof(bptree_node *));
            return BPTREE_ALLOCATION_FAILURE;
        }
        int count = ((n_items - idx) < tree->max_keys) ? (n_items - idx) : tree->max_keys;
        if (count <= 0) {
            bptree_free_mem(tree, leaf, bptree_node_alloc_size(tree, true));
            continue;
        }
        memcpy(bptree_node_keys(leaf), &keys[idx], count * sizeof(bptree_key_t));
        memcpy(bptree_node_values(leaf, tree->max_keys), &values[idx],
               count * sizeof(bptree_value_t));
        leaf->num_keys = count;
        idx += count;
        if (i > 0) level[i - 1]->next = leaf;
        level[i] = leaf;
        bptree_debug_print(tree->enable_debug, "Leaf %d built with %d keys\n", i, count);
    }

    /* --- Ensure last leaf not underfilled --- */
    if (num_leaves > 1) {
        bptree_node *last = level[num_leaves - 1];
        if (last->num_keys < tree->min_leaf_keys) {
            bptree_node *prev = level[num_leaves - 2];
            int total = prev->num_keys + last->num_keys;
            if (total <= tree->max_keys) {
                memcpy(bptree_node_keys(prev) + prev->num_keys, bptree_node_keys(last),
                       last->num_keys * sizeof(bptree_key_t));
                memcpy(bptree_node_values(prev, tree->max_keys) + prev->num_keys,
                       bptree_node_values(last, tree->max_keys),
                       last->num_keys * sizeof(bptree_value_t));
                prev->num_keys = total;
                prev->next = NULL;
                num_leaves--;
                bptree_debug_print(tree->enable_debug,
                                   "Merged last leaf into previous leaf, new leaf count = %d\n",
                                   num_leaves);
            } else {
                int new_count1 = (total + 1) / 2;
                int new_count2 = total - new_count1;
                size_t tmp_keys_size = total * sizeof(bptree_key_t);
                size_t tmp_vals_size = total * sizeof(bptree_value_t);
                bptree_key_t *tmp_keys =
                    bptree_alloc_mem(tree, tmp_keys_size, alignof(bptree_key_t));
                bptree_value_t *tmp_vals =
                    bptree_alloc_mem(tree, tmp_vals_size, alignof(bptree_value_t));
                if (!tmp_keys || !tmp_vals) {
                    if (tmp_keys) bptree_free_mem(tree, tmp_keys, tmp_keys_size);
                    if (tmp_vals) bptree_free_mem(tree, tmp_vals, tmp_vals_size);
                    return BPTREE_ALLOCATION_FAILURE;
                }
                memcpy(tmp_keys, bptree_node_keys(prev), prev->num_keys * sizeof(bptree_key_t));
                memcpy(tmp_keys + prev->num_keys, bptree_node_keys(last),
                       last->num_keys * sizeof(bptree_key_t));
                memcpy(tmp_vals, bptree_node_values(prev, tree->max_keys),
                       prev->num_keys * sizeof(bptree_value_t));
                memcpy(tmp_vals + prev->num_keys, bptree_node_values(last, tree->max_keys),
                       last->num_keys * sizeof(bptree_value_t));
                memcpy(bptree_node_keys(prev), tmp_keys, new_count1 * sizeof(bptree_key_t));
                memcpy(bptree_node_values(prev, tree->max_keys), tmp_vals,
                       new_count1 * sizeof(bptree_value_t));
                prev->num_keys = new_count1;
                memcpy(bptree_node_keys(last), tmp_keys + new_count1,
                       new_count2 * sizeof(bptree_key_t));
                memcpy(bptree_node_values(last, tree->max_keys), tmp_vals + new_count1,
                       new_count2 * sizeof(bptree_value_t));
                last->num_keys = new_count2;
                bptree_free_mem(tree, tmp_keys, tmp_keys_size);
                bptree_free_mem(tree, tmp_vals, tmp_vals_size);
                bptree_debug_print(tree->enable_debug,
                                   "Redistributed keys between last two leaves\n");
            }
        }
    }

    /* --- Build Internal Levels --- */
    int level_nodes = num_leaves;
    int current_height = 1;
    const int group_capacity = tree->max_keys + 1;
    const int min_children = (tree->min_internal_keys >= 1) ? tree->min_internal_keys + 1 : 2;

    while (level_nodes > 1) {
        int num_parents = (level_nodes + group_capacity - 1) / group_capacity;
        bptree_node **parent_level =
            bptree_alloc_mem(tree, num_parents * sizeof(bptree_node *), alignof(bptree_node *));
        if (!parent_level) {
            for (int i = 0; i < level_nodes; i++) bptree_free_node(level[i], tree);
            bptree_free_mem(tree, level, level_nodes * sizeof(bptree_node *));
            return BPTREE_ALLOCATION_FAILURE;
        }
        memset(parent_level, 0, num_parents * sizeof(bptree_node *));

        int parent_idx = 0;
        int child_idx = 0;
        while (child_idx < level_nodes) {
            bptree_node *internal = bptree_node_alloc(tree, false);
            if (!internal) {
                for (int k = 0; k < parent_idx; k++) bptree_free_node(parent_level[k], tree);
                bptree_free_mem(tree, parent_level, num_parents * sizeof(bptree_node *));
                for (int l = 0; l < level_nodes; l++) bptree_free_node(level[l], tree);
                bptree_free_mem(tree, level, level_nodes * sizeof(bptree_node *));
                return BPTREE_ALLOCATION_FAILURE;
            }
            bptree_node **children = bptree_node_children(internal, tree->max_keys);
            bptree_key_t *internal_keys = bptree_node_keys(internal);
            int children_count = 0;
            while (child_idx < level_nodes && children_count < group_capacity) {
                children[children_count] = level[child_idx];
                children_count++;
                child_idx++;
            }
            if (child_idx >= level_nodes && children_count == 1 && parent_idx > 0) {
                bptree_node *prev = parent_level[parent_idx - 1];
                int prev_child_count = prev->num_keys + 1;
                if (prev_child_count > min_children) {
                    bptree_node **prev_children = bptree_node_children(prev, tree->max_keys);
                    bptree_node *borrowed = prev_children[prev->num_keys];
                    memmove(children + 1, children, children_count * sizeof(bptree_node *));
                    children[0] = borrowed;
                    children_count++;
                    prev->num_keys--;
                    bptree_debug_print(
                        tree->enable_debug,
                        "Borrowed child from previous group: new children_count=%d\n",
                        children_count);
                }
            }
            bool merged = false;
            if (children_count == 1 && parent_idx > 0) {
                bptree_node *prev = parent_level[parent_idx - 1];
                bptree_node **prev_children = bptree_node_children(prev, tree->max_keys);
                int prev_child_count = prev->num_keys + 1;
                if (prev_child_count < group_capacity) {
                    prev_children[prev_child_count] = children[0];
                    bptree_key_t *prev_keys = bptree_node_keys(prev);
                    prev_keys[prev->num_keys] =
                        bptree_find_smallest_key(children[0], tree->max_keys);
                    prev->num_keys++;
                    size_t internal_size = bptree_node_alloc_size(tree, false);
                    bptree_free_mem(tree, internal, internal_size);
                    merged = true;
                    bptree_debug_print(
                        tree->enable_debug,
                        "Merged solitary child into previous parent (parent_idx %d)\n",
                        parent_idx - 1);
                    continue;
                }
            }
            if (!merged) {
                internal->num_keys = children_count - 1;
                for (int k = 1; k < children_count; k++) {
                    internal_keys[k - 1] = bptree_find_smallest_key(children[k], tree->max_keys);
                }
                parent_level[parent_idx++] = internal;
                bptree_debug_print(tree->enable_debug,
                                   "Internal node built with %d children (%d keys)\n",
                                   children_count, internal->num_keys);
            }
        }
        bptree_free_mem(tree, level, level_nodes * sizeof(bptree_node *));
        level = parent_level;
        level_nodes = parent_idx;
        current_height++;
    }
    tree->root = level[0];
    if (tree->root && !tree->root->is_leaf && tree->root->num_keys == 0 && tree->count > 0) {
        bptree_node *old_root = tree->root;
        if (bptree_node_children(old_root, tree->max_keys)[0] != NULL) {
            tree->root = bptree_node_children(old_root, tree->max_keys)[0];
            current_height--;
            size_t old_root_size = bptree_node_alloc_size(tree, old_root->is_leaf);
            bptree_free_mem(tree, old_root, old_root_size);
            bptree_debug_print(tree->enable_debug, "Collapsed solitary internal root\n");
        } else {
            bptree_debug_print(tree->enable_debug,
                               "Error: Internal root with 0 keys and no child found.\n");
        }
    }
    tree->height = current_height;
    tree->count = n_items;
    bptree_free_mem(tree, level, level_nodes * sizeof(bptree_node *));
    bptree_debug_print(tree->enable_debug, "Bulk load completed: tree height = %d\n", tree->height);
    return BPTREE_OK;
}

BPTREE_API inline bptree_status bptree_get_range(const bptree *tree, const bptree_key_t *start,
                                                 const bptree_key_t *end,
                                                 bptree_value_t **out_values, int *n_results) {
    if (!n_results || !tree || !tree->root || !start || !end) return BPTREE_INVALID_ARGUMENT;
    *n_results = 0;
    if (tree->compare(start, end) > 0) return BPTREE_INVALID_ARGUMENT;
    bptree_node *node = tree->root;
    while (!node->is_leaf) {
        int pos = bptree_node_search(tree, node, start);
        node = bptree_node_children(node, tree->max_keys)[pos];
    }
    int count = 0;
    bptree_node *current_leaf = node;
    bool past_end = false;
    while (current_leaf && !past_end) {
        bptree_key_t *keys = bptree_node_keys(current_leaf);
        for (int i = 0; i < current_leaf->num_keys; i++) {
            if (tree->compare(&keys[i], start) >= 0) {
                if (tree->compare(&keys[i], end) > 0) {
                    past_end = true;
                    break;
                }
                count++;
            }
        }
        if (!past_end) current_leaf = current_leaf->next;
    }
    if (count == 0) {
        *out_values = NULL;
        *n_results = 0;
        return BPTREE_OK;
    }
    size_t alloc_size = (size_t)count * sizeof(bptree_value_t);
    bptree_value_t *results = bptree_alloc_mem((bptree *)tree, alloc_size, alignof(bptree_value_t));
    if (!results) {
        *n_results = 0;
        *out_values = NULL;
        return BPTREE_ALLOCATION_FAILURE;
    }
    int index = 0;
    current_leaf = node;
    past_end = false;
    while (current_leaf && !past_end && index < count) {
        bptree_key_t *keys = bptree_node_keys(current_leaf);
        bptree_value_t *values = bptree_node_values(current_leaf, tree->max_keys);
        for (int i = 0; i < current_leaf->num_keys; i++) {
            if (tree->compare(&keys[i], start) >= 0) {
                if (tree->compare(&keys[i], end) > 0) {
                    past_end = true;
                    break;
                }
                if (index < count)
                    results[index++] = values[i];
                else {
                    fprintf(stderr, "BPTree Error: Range query index exceeded count.\n");
                    past_end = true;
                    break;
                }
            }
        }
        if (!past_end) current_leaf = current_leaf->next;
    }
    *n_results = index;
    *out_values = results;
    return BPTREE_OK;
}

BPTREE_API inline void bptree_free_range_results(bptree *tree, bptree_value_t *results,
                                                 int n_results) {
    if (!tree || !results || n_results < 0) return;
    size_t alloc_size = (size_t)n_results * sizeof(bptree_value_t);
    bptree_free_mem(tree, results, alloc_size);
}

BPTREE_API inline bptree_stats bptree_get_stats(const bptree *tree) {
    bptree_stats stats;
    stats.count = tree->count;
    stats.height = tree->height;
    stats.node_count = bptree_count_nodes(tree->root, tree);
    return stats;
}

BPTREE_API inline bool bptree_check_invariants(const bptree *tree) {
    if (!tree || !tree->root) return false;
    int leaf_depth = -1;
    return bptree_check_invariants_node(tree->root, (bptree *)tree, 0, &leaf_depth);
}

BPTREE_API inline void bptree_assert_invariants(const bptree *tree) {
    if (!bptree_check_invariants(tree)) {
        fprintf(stderr, "B+ tree invariants violated!\n");
        abort();
    }
}

BPTREE_API inline bptree_iterator *bptree_iterator_new(const bptree *tree) {
    bptree_iterator *iter =
        bptree_alloc_mem((bptree *)tree, sizeof(bptree_iterator), alignof(bptree_iterator));
    if (!iter) return NULL;
    bptree_node *node = tree->root;
    while (!node->is_leaf) node = bptree_node_children(node, tree->max_keys)[0];
    iter->current_leaf = node;
    iter->index = 0;
    iter->tree = (bptree *)tree;
    return iter;
}

BPTREE_API inline bool bptree_iterator_has_next(const bptree_iterator *iter) {
    if (!iter || !iter->current_leaf) return false;
    return (iter->index < iter->current_leaf->num_keys) || (iter->current_leaf->next != NULL);
}

BPTREE_API inline bptree_status bptree_iterator_next(bptree_iterator *iter,
                                                     bptree_value_t *out_value) {
    if (!iter || !out_value) return BPTREE_INVALID_ARGUMENT;
    if (!iter->current_leaf) return BPTREE_ITERATOR_END;
    if (iter->index < iter->current_leaf->num_keys) {
        *out_value = bptree_node_values(iter->current_leaf, iter->tree->max_keys)[iter->index++];
        return BPTREE_OK;
    }
    iter->current_leaf = iter->current_leaf->next;
    iter->index = 0;
    if (!iter->current_leaf) return BPTREE_ITERATOR_END;
    *out_value = bptree_node_values(iter->current_leaf, iter->tree->max_keys)[iter->index++];
    return BPTREE_OK;
}

BPTREE_API inline void bptree_iterator_free(bptree_iterator *iter) {
    if (iter) bptree_free_mem(iter->tree, iter, sizeof(bptree_iterator));
}

BPTREE_API inline bool bptree_contains(const bptree *tree, const bptree_key_t *key) {
    bptree_value_t value;
    return (bptree_get_value(tree, key, &value) == BPTREE_OK);
}

BPTREE_API inline int bptree_size(const bptree *tree) { return tree ? tree->count : 0; }

#endif

#ifdef __cplusplus
}
#endif

#endif
