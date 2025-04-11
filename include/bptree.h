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

static void bptree_debug_print(const bool enable, const char *fmt, ...) {
    if (!enable) return;
    char time_buf[64];
    const time_t now = time(NULL);
    const struct tm *tm_info = localtime(&now);
    strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", tm_info);
    printf("[%s] [BPTREE DEBUG] ", time_buf);
    va_list args;
    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
}

#define BPTREE_MAX_ALIGN(a, b) ((a) > (b) ? (a) : (b))

static size_t bptree_keys_area_size(const int max_keys) {
    const size_t keys_size = (size_t)(max_keys + 1) * sizeof(bptree_key_t);
    const size_t req_align = BPTREE_MAX_ALIGN(alignof(bptree_value_t), alignof(bptree_node *));
    const size_t pad = (req_align - (keys_size % req_align)) % req_align;
    return keys_size + pad;
}

static bptree_key_t *bptree_node_keys(const bptree_node *node) { return (bptree_key_t *)node->data; }

static bptree_value_t *bptree_node_values(bptree_node *node, const int max_keys) {
    const size_t offset = bptree_keys_area_size(max_keys);
    return (bptree_value_t *)(node->data + offset);
}

static bptree_node **bptree_node_children(bptree_node *node, const int max_keys) {
    const size_t offset = bptree_keys_area_size(max_keys);
    return (bptree_node **)(node->data + offset);
}

#ifdef BPTREE_KEY_TYPE_STRING
static inline int bptree_default_compare(const bptree_key_t *a, const bptree_key_t *b) {
    return memcmp(a->data, b->data, BPTREE_KEY_SIZE);
}
#else
static int bptree_default_compare(const bptree_key_t *a, const bptree_key_t *b) {
    return (*a < *b) ? -1 : ((*a > *b) ? 1 : 0);
}
#endif

static bptree_key_t bptree_find_smallest_key(bptree_node *node, const int max_keys) {
    assert(node != NULL);
    while (!node->is_leaf) {
        assert(node->num_keys >= 0);
        node = bptree_node_children(node, max_keys)[0];
        assert(node != NULL);
    }
    assert(node->num_keys > 0);
    return bptree_node_keys(node)[0];
}

static bptree_key_t bptree_find_largest_key(bptree_node *node, const int max_keys) {
    assert(node != NULL);
    while (!node->is_leaf) {
        assert(node->num_keys >= 0);
        node = bptree_node_children(node, max_keys)[node->num_keys];
        assert(node != NULL);
    }
    assert(node->num_keys > 0);
    return bptree_node_keys(node)[node->num_keys - 1];
}

static int bptree_count_nodes(const bptree_node *node, const bptree *tree) {
    if (!node) return 0;
    if (node->is_leaf) return 1;
    int count = 1;
    bptree_node **children = bptree_node_children((bptree_node *)node, tree->max_keys);
    for (int i = 0; i <= node->num_keys; i++) {
        count += bptree_count_nodes(children[i], tree);
    }
    return count;
}

static bool bptree_check_invariants_node(bptree_node *node, const bptree *tree, const int depth,
                                         int *leaf_depth) {
    if (!node) return false;
    const bptree_key_t *keys = bptree_node_keys(node);
    const bool is_root = (tree->root == node);
    for (int i = 1; i < node->num_keys; i++) {
        if (tree->compare(&keys[i - 1], &keys[i]) >= 0) {
            bptree_debug_print(tree->enable_debug, "Invariant Fail: Keys not sorted in node %p\n",
                               (void *)node);
            return false;
        }
    }
    if (node->is_leaf) {
        if (*leaf_depth == -1) {
            *leaf_depth = depth;
        } else if (depth != *leaf_depth) {
            bptree_debug_print(tree->enable_debug,
                               "Invariant Fail: Leaf depth mismatch (%d != %d) for node %p\n",
                               depth, *leaf_depth, (void *)node);
            return false;
        }
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
        return true;
    } else {
        if (!is_root &&
            (node->num_keys < tree->min_internal_keys || node->num_keys > tree->max_keys)) {
            bptree_debug_print(
                tree->enable_debug,
                "Invariant Fail: Internal node %p key count out of range [%d, %d] (%d keys)\n",
                (void *)node, tree->min_internal_keys, tree->max_keys, node->num_keys);
            return false;
        }
        if (is_root && tree->count > 0 && node->num_keys < 1) {
            bptree_debug_print(
                tree->enable_debug,
                "Invariant Fail: Internal root node %p has < 1 key (%d keys) in non-empty tree\n",
                (void *)node, node->num_keys);
            return false;
        }
        if (is_root && node->num_keys > tree->max_keys) {
            bptree_debug_print(tree->enable_debug,
                               "Invariant Fail: Internal root node %p has > max_keys (%d > %d)\n",
                               (void *)node, node->num_keys, tree->max_keys);
            return false;
        }
        bptree_node **children = bptree_node_children(node, tree->max_keys);
        if (node->num_keys >= 0) {
            if (!children[0]) {
                bptree_debug_print(tree->enable_debug,
                                   "Invariant Fail: Internal node %p missing child[0]\n",
                                   (void *)node);
                return false;
            }
            if (node->num_keys > 0 && (children[0]->num_keys > 0 || !children[0]->is_leaf)) {
                const bptree_key_t max_in_child0 = bptree_find_largest_key(children[0], tree->max_keys);
                if (tree->compare(&max_in_child0, &keys[0]) >= 0) {
                    bptree_debug_print(tree->enable_debug,
                                       "Invariant Fail: max(child[0]) >= key[0] in node %p -- "
                                       "MaxChild=%lld Key=%lld\n",
                                       (void *)node, (long long)max_in_child0, (long long)keys[0]);
                    return false;
                }
            }
            if (!bptree_check_invariants_node(children[0], tree, depth + 1, leaf_depth))
                return false;
            for (int i = 1; i <= node->num_keys; i++) {
                if (!children[i]) {
                    bptree_debug_print(tree->enable_debug,
                                       "Invariant Fail: Internal node %p missing child[%d]\n",
                                       (void *)node, i);
                    return false;
                }
                if (children[i]->num_keys > 0 || !children[i]->is_leaf) {
                    bptree_key_t min_in_child =
                        bptree_find_smallest_key(children[i], tree->max_keys);
                    if (tree->compare(&keys[i - 1], &min_in_child) > 0) {
                        bptree_debug_print(
                            tree->enable_debug,
                            "Invariant Fail: key[%d] > min(child[%d]) -- Node Addr: %p, Node Keys: "
                            "%d, Parent Key[%d]: %lld, Found Min: %lld, Child[%d] Addr: %p\n",
                            i - 1, i, (void *)node, node->num_keys, i - 1, (long long)keys[i - 1],
                            (long long)min_in_child, i, (void *)children[i]);
                        return false;
                    }
                    if (i < node->num_keys) {
                        bptree_key_t max_in_child =
                            bptree_find_largest_key(children[i], tree->max_keys);
                        if (tree->compare(&max_in_child, &keys[i]) >= 0) {
                            bptree_debug_print(tree->enable_debug,
                                               "Invariant Fail: max(child[%d]) >= key[%d] in node "
                                               "%p -- MaxChild=%lld Key=%lld\n",
                                               i, i, (void *)node, (long long)max_in_child,
                                               (long long)keys[i]);
                            return false;
                        }
                    }
                } else if (children[i]->is_leaf && children[i]->num_keys == 0 && tree->count > 0) {
                    bptree_debug_print(tree->enable_debug,
                                       "Invariant Fail: Internal node %p points to empty leaf "
                                       "child[%d] %p in non-empty tree\n",
                                       (void *)node, i, (void *)children[i]);
                    return false;
                }
                if (!bptree_check_invariants_node(children[i], tree, depth + 1, leaf_depth))
                    return false;
            }
        } else {
            if (!is_root || tree->count > 0) {
                bptree_debug_print(tree->enable_debug,
                                   "Invariant Fail: Internal node %p has < 0 keys (%d)\n",
                                   (void *)node, node->num_keys);
                return false;
            }
        }
        return true;
    }
}

static size_t bptree_node_alloc_size(const bptree *tree, const bool is_leaf) {
    const int max_keys = tree->max_keys;
    const size_t keys_area_sz = bptree_keys_area_size(max_keys);
    size_t data_payload_size;
    if (is_leaf) {
        data_payload_size = (size_t)(max_keys + 1) * sizeof(bptree_value_t);
    } else {
        data_payload_size = (size_t)(max_keys + 2) * sizeof(bptree_node *);
    }
    const size_t total_data_size = keys_area_sz + data_payload_size;
    return sizeof(bptree_node) + total_data_size;
}

static bptree_node *bptree_node_alloc(const bptree *tree, const bool is_leaf) {
    size_t max_align = alignof(bptree_node);
    max_align = BPTREE_MAX_ALIGN(max_align, alignof(bptree_key_t));
    if (is_leaf) {
        max_align = BPTREE_MAX_ALIGN(max_align, alignof(bptree_value_t));
    } else {
        max_align = BPTREE_MAX_ALIGN(max_align, alignof(bptree_node *));
    }
    size_t size = bptree_node_alloc_size(tree, is_leaf);
    size = (size + max_align - 1) & ~(max_align - 1);
    bptree_node *node = aligned_alloc(max_align, size);
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

static void bptree_rebalance_up(bptree *tree, bptree_node **node_stack, const int *index_stack,
                                const int depth) {
    for (int d = depth - 1; d >= 0; d--) {
        bptree_node *parent = node_stack[d];
        const int child_idx = index_stack[d];
        bptree_node **children = bptree_node_children(parent, tree->max_keys);
        bptree_node *child = children[child_idx];
        const int min_keys = child->is_leaf ? tree->min_leaf_keys : tree->min_internal_keys;
        if (child->num_keys >= min_keys) {
            bptree_debug_print(tree->enable_debug,
                               "Rebalance unnecessary at depth %d, child %d has %d keys (min %d)\n",
                               d, child_idx, child->num_keys, min_keys);
            break;
        }
        bptree_debug_print(tree->enable_debug,
                           "Rebalance needed at depth %d for child %d (%d keys < min %d)\n", d,
                           child_idx, child->num_keys, min_keys);
        if (child_idx > 0) {
            bptree_node *left_sibling = children[child_idx - 1];
            const int left_min = left_sibling->is_leaf ? tree->min_leaf_keys : tree->min_internal_keys;
            if (left_sibling->num_keys > left_min) {
                bptree_debug_print(tree->enable_debug,
                                   "Attempting borrow from left sibling (idx %d)\n", child_idx - 1);
                bptree_key_t *parent_keys = bptree_node_keys(parent);
                if (child->is_leaf) {
                    bptree_key_t *child_keys = bptree_node_keys(child);
                    bptree_value_t *child_vals = bptree_node_values(child, tree->max_keys);
                    const bptree_key_t *left_keys = bptree_node_keys(left_sibling);
                    const bptree_value_t *left_vals = bptree_node_values(left_sibling, tree->max_keys);
                    memmove(&child_keys[1], &child_keys[0], child->num_keys * sizeof(bptree_key_t));
                    memmove(&child_vals[1], &child_vals[0],
                            child->num_keys * sizeof(bptree_value_t));
                    child_keys[0] = left_keys[left_sibling->num_keys - 1];
                    child_vals[0] = left_vals[left_sibling->num_keys - 1];
                    child->num_keys++;
                    left_sibling->num_keys--;
                    parent_keys[child_idx - 1] = child_keys[0];
                    bptree_debug_print(tree->enable_debug,
                                       "Borrowed leaf key from left. Parent key updated.\n");
                    break;
                } else {
                    bptree_key_t *child_keys = bptree_node_keys(child);
                    bptree_node **child_children = bptree_node_children(child, tree->max_keys);
                    bptree_key_t *left_keys = bptree_node_keys(left_sibling);
                    bptree_node **left_children =
                        bptree_node_children(left_sibling, tree->max_keys);
                    memmove(&child_keys[1], &child_keys[0], child->num_keys * sizeof(bptree_key_t));
                    memmove(&child_children[1], &child_children[0],
                            (child->num_keys + 1) * sizeof(bptree_node *));
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
        if (child_idx < parent->num_keys) {
            bptree_node *right_sibling = children[child_idx + 1];
            const int right_min = right_sibling->is_leaf ? tree->min_leaf_keys : tree->min_internal_keys;
            if (right_sibling->num_keys > right_min) {
                bptree_debug_print(tree->enable_debug,
                                   "Attempting borrow from right sibling (idx %d)\n",
                                   child_idx + 1);
                bptree_key_t *parent_keys = bptree_node_keys(parent);
                if (child->is_leaf) {
                    bptree_key_t *child_keys = bptree_node_keys(child);
                    bptree_value_t *child_vals = bptree_node_values(child, tree->max_keys);
                    bptree_key_t *right_keys = bptree_node_keys(right_sibling);
                    bptree_value_t *right_vals = bptree_node_values(right_sibling, tree->max_keys);
                    child_keys[child->num_keys] = right_keys[0];
                    child_vals[child->num_keys] = right_vals[0];
                    child->num_keys++;
                    right_sibling->num_keys--;
                    memmove(&right_keys[0], &right_keys[1],
                            right_sibling->num_keys * sizeof(bptree_key_t));
                    memmove(&right_vals[0], &right_vals[1],
                            right_sibling->num_keys * sizeof(bptree_value_t));
                    parent_keys[child_idx] = right_keys[0];
                    bptree_debug_print(tree->enable_debug,
                                       "Borrowed leaf key from right. Parent key updated.\n");
                    break;
                } else {
                    bptree_key_t *child_keys = bptree_node_keys(child);
                    bptree_node **child_children = bptree_node_children(child, tree->max_keys);
                    bptree_key_t *right_keys = bptree_node_keys(right_sibling);
                    bptree_node **right_children =
                        bptree_node_children(right_sibling, tree->max_keys);
                    child_keys[child->num_keys] = parent_keys[child_idx];
                    child_children[child->num_keys + 1] = right_children[0];
                    parent_keys[child_idx] = right_keys[0];
                    child->num_keys++;
                    right_sibling->num_keys--;
                    memmove(&right_keys[0], &right_keys[1],
                            right_sibling->num_keys * sizeof(bptree_key_t));
                    memmove(&right_children[0], &right_children[1],
                            (right_sibling->num_keys + 1) * sizeof(bptree_node *));
                    bptree_debug_print(
                        tree->enable_debug,
                        "Borrowed internal key/child from right. Parent key updated.\n");
                    break;
                }
            }
        }
        bptree_debug_print(tree->enable_debug, "Borrow failed, attempting merge\n");
        if (child_idx > 0) {
            bptree_node *left_sibling = children[child_idx - 1];
            bptree_debug_print(tree->enable_debug, "Merging child %d into left sibling %d\n",
                               child_idx, child_idx - 1);
            if (child->is_leaf) {
                bptree_key_t *left_keys = bptree_node_keys(left_sibling);
                bptree_value_t *left_vals = bptree_node_values(left_sibling, tree->max_keys);
                const bptree_key_t *child_keys = bptree_node_keys(child);
                const bptree_value_t *child_vals = bptree_node_values(child, tree->max_keys);
                const int combined_keys = left_sibling->num_keys + child->num_keys;
                if (combined_keys > tree->max_keys) {
                    fprintf(stderr,
                            "[BPTree FATAL] Merge-Left (Leaf) Buffer Overflow PREVENTED! "
                            "Combined keys %d > max_keys %d. Invariants likely violated.\n",
                            combined_keys, tree->max_keys);
                    abort();
                }
                memcpy(left_keys + left_sibling->num_keys, child_keys,
                       child->num_keys * sizeof(bptree_key_t));
                memcpy(left_vals + left_sibling->num_keys, child_vals,
                       child->num_keys * sizeof(bptree_value_t));
                left_sibling->num_keys = combined_keys;
                left_sibling->next = child->next;
                free(child);
                children[child_idx] = NULL;
            } else {
                bptree_key_t *left_keys = bptree_node_keys(left_sibling);
                bptree_node **left_children = bptree_node_children(left_sibling, tree->max_keys);
                bptree_key_t *child_keys = bptree_node_keys(child);
                bptree_node **child_children = bptree_node_children(child, tree->max_keys);
                bptree_key_t *parent_keys = bptree_node_keys(parent);
                const int combined_keys = left_sibling->num_keys + 1 + child->num_keys;
                const int combined_children = (left_sibling->num_keys + 1) + (child->num_keys + 1);
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
                left_keys[left_sibling->num_keys] = parent_keys[child_idx - 1];
                memcpy(left_keys + left_sibling->num_keys + 1, child_keys,
                       child->num_keys * sizeof(bptree_key_t));
                memcpy(left_children + left_sibling->num_keys + 1, child_children,
                       (child->num_keys + 1) * sizeof(bptree_node *));
                left_sibling->num_keys = combined_keys;
                free(child);
                children[child_idx] = NULL;
            }
            bptree_key_t *parent_keys = bptree_node_keys(parent);
            memmove(&parent_keys[child_idx - 1], &parent_keys[child_idx],
                    (parent->num_keys - child_idx) * sizeof(bptree_key_t));
            memmove(&children[child_idx], &children[child_idx + 1],
                    (parent->num_keys - child_idx) * sizeof(bptree_node *));
            parent->num_keys--;
            bptree_debug_print(tree->enable_debug, "Merge with left complete. Parent updated.\n");
        } else {
            bptree_node *right_sibling = children[child_idx + 1];
            bptree_debug_print(tree->enable_debug, "Merging right sibling %d into child %d\n",
                               child_idx + 1, child_idx);
            if (child->is_leaf) {
                bptree_key_t *child_keys = bptree_node_keys(child);
                bptree_value_t *child_vals = bptree_node_values(child, tree->max_keys);
                const bptree_key_t *right_keys = bptree_node_keys(right_sibling);
                const bptree_value_t *right_vals = bptree_node_values(right_sibling, tree->max_keys);
                const int combined_keys = child->num_keys + right_sibling->num_keys;
                if (combined_keys > tree->max_keys) {
                    fprintf(stderr,
                            "[BPTree FATAL] Merge-Right (Leaf) Buffer Overflow PREVENTED! "
                            "Combined keys %d > max_keys %d. Invariants likely violated.\n",
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
                bptree_key_t *child_keys = bptree_node_keys(child);
                bptree_node **child_children = bptree_node_children(child, tree->max_keys);
                const bptree_key_t *right_keys = bptree_node_keys(right_sibling);
                bptree_node **right_children = bptree_node_children(right_sibling, tree->max_keys);
                const bptree_key_t *parent_keys = bptree_node_keys(parent);
                const int combined_keys = child->num_keys + 1 + right_sibling->num_keys;
                const int combined_children = (child->num_keys + 1) + (right_sibling->num_keys + 1);
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
                child_keys[child->num_keys] = parent_keys[child_idx];
                memcpy(child_keys + child->num_keys + 1, right_keys,
                       right_sibling->num_keys * sizeof(bptree_key_t));
                memcpy(child_children + child->num_keys + 1, right_children,
                       (right_sibling->num_keys + 1) * sizeof(bptree_node *));
                child->num_keys = combined_keys;
                free(right_sibling);
                children[child_idx + 1] = NULL;
            }
            bptree_key_t *parent_keys = bptree_node_keys(parent);
            memmove(&parent_keys[child_idx], &parent_keys[child_idx + 1],
                    (parent->num_keys - child_idx - 1) * sizeof(bptree_key_t));
            memmove(&children[child_idx + 1], &children[child_idx + 2],
                    (parent->num_keys - child_idx - 1) * sizeof(bptree_node *));
            parent->num_keys--;
            bptree_debug_print(tree->enable_debug, "Merge with right complete. Parent updated.\n");
        }
    }
    if (!tree->root->is_leaf && tree->root->num_keys == 0 && tree->count > 0) {
        bptree_debug_print(tree->enable_debug,
                           "Root node is internal and empty, shrinking height.\n");
        bptree_node *old_root = tree->root;
        tree->root = bptree_node_children(old_root, tree->max_keys)[0];
        tree->height--;
        free(old_root);
    } else if (tree->count == 0 && tree->root && tree->root->num_keys != 0) {
        bptree_debug_print(tree->enable_debug, "Tree empty, ensuring root node is empty.\n");
        tree->root->num_keys = 0;
    }
}

static int bptree_node_search(const bptree *tree, const bptree_node *node, const bptree_key_t *key) {
    int low = 0, high = node->num_keys;
    const bptree_key_t *keys = bptree_node_keys(node);
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

static bptree_status bptree_insert_internal(bptree *tree, bptree_node *node,
                                            const bptree_key_t *key, const bptree_value_t value,
                                            bptree_key_t *promoted_key, bptree_node **new_child) {
    const int pos = bptree_node_search(tree, node, key);
    if (node->is_leaf) {
        bptree_key_t *keys = bptree_node_keys(node);
        bptree_value_t *values = bptree_node_values(node, tree->max_keys);
        if (pos < node->num_keys && tree->compare(key, &keys[pos]) == 0) {
            bptree_debug_print(tree->enable_debug, "Insert failed: Duplicate key found.\n");
            return BPTREE_DUPLICATE_KEY;
        }
        memmove(&keys[pos + 1], &keys[pos], (node->num_keys - pos) * sizeof(bptree_key_t));
        memmove(&values[pos + 1], &values[pos], (node->num_keys - pos) * sizeof(bptree_value_t));
        keys[pos] = *key;
        values[pos] = value;
        node->num_keys++;
        *new_child = NULL;
        bptree_debug_print(tree->enable_debug, "Inserted key in leaf. Node keys: %d\n",
                           node->num_keys);
        if (node->num_keys > tree->max_keys) {
            bptree_debug_print(tree->enable_debug, "Leaf node overflow (%d > %d), splitting.\n",
                               node->num_keys, tree->max_keys);
            const int total_keys = node->num_keys;
            const int split_idx = (total_keys + 1) / 2;
            const int new_node_keys = total_keys - split_idx;
            bptree_node *new_leaf = bptree_node_alloc(tree, true);
            if (!new_leaf) {
                node->num_keys--;
                bptree_debug_print(tree->enable_debug, "Leaf split allocation failed!\n");
                return BPTREE_ALLOCATION_FAILURE;
            }
            bptree_key_t *new_keys = bptree_node_keys(new_leaf);
            bptree_value_t *new_values = bptree_node_values(new_leaf, tree->max_keys);
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
        bptree_node **children = bptree_node_children(node, tree->max_keys);
        bptree_key_t child_promoted_key;
        bptree_node *child_new_node = NULL;
        const bptree_status status = bptree_insert_internal(tree, children[pos], key, value,
                                                      &child_promoted_key, &child_new_node);
        if (status != BPTREE_OK || child_new_node == NULL) {return status;}
        bptree_debug_print(tree->enable_debug,
                           "Child split propagated. Inserting promoted key into internal node.\n");
        bptree_key_t *keys = bptree_node_keys(node);
        memmove(&keys[pos + 1], &keys[pos], (node->num_keys - pos) * sizeof(bptree_key_t));
        memmove(&children[pos + 2], &children[pos + 1],
                (node->num_keys - pos) * sizeof(bptree_node *));
        keys[pos] = child_promoted_key;
        children[pos + 1] = child_new_node;
        node->num_keys++;
        bptree_debug_print(tree->enable_debug, "Internal node keys: %d\n", node->num_keys);
        if (node->num_keys > tree->max_keys) {
            bptree_debug_print(tree->enable_debug, "Internal node overflow (%d > %d), splitting.\n",
                               node->num_keys, tree->max_keys);
            const int total_keys = node->num_keys;
            const int split_idx = total_keys / 2;
            const int new_node_keys = total_keys - split_idx - 1;
            bptree_node *new_internal = bptree_node_alloc(tree, false);
            if (!new_internal) {
                node->num_keys--;
                bptree_debug_print(tree->enable_debug, "Internal split allocation failed!\n");
                return BPTREE_ALLOCATION_FAILURE;
            }
            bptree_key_t *new_keys = bptree_node_keys(new_internal);
            bptree_node **new_children = bptree_node_children(new_internal, tree->max_keys);
            *promoted_key = keys[split_idx];
            *new_child = new_internal;
            memcpy(new_keys, &keys[split_idx + 1], new_node_keys * sizeof(bptree_key_t));
            memcpy(new_children, &children[split_idx + 1],
                   (new_node_keys + 1) * sizeof(bptree_node *));
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

BPTREE_API bptree_status bptree_put(bptree *tree, const bptree_key_t *key, bptree_value_t value) {
    if (!tree || !key) return BPTREE_INVALID_ARGUMENT;
    if (!tree->root) return BPTREE_INTERNAL_ERROR;
    bptree_key_t promoted_key;
    bptree_node *new_node = NULL;
    const bptree_status status =
        bptree_insert_internal(tree, tree->root, key, value, &promoted_key, &new_node);
    if (status == BPTREE_OK) {
        if (new_node != NULL) {
            bptree_debug_print(tree->enable_debug, "Root split occurred. Creating new root.\n");
            bptree_node *new_root = bptree_node_alloc(tree, false);
            if (!new_root) {
                bptree_free_node(new_node, tree);
                return BPTREE_ALLOCATION_FAILURE;
            }
            bptree_key_t *root_keys = bptree_node_keys(new_root);
            bptree_node **root_children = bptree_node_children(new_root, tree->max_keys);
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

BPTREE_API bptree_status bptree_get(const bptree *tree, const bptree_key_t *key,
                                    bptree_value_t *out_value) {
    if (!tree || !tree->root || !key || !out_value) return BPTREE_INVALID_ARGUMENT;
    if (tree->count == 0) return BPTREE_KEY_NOT_FOUND;
    bptree_node *node = tree->root;
    while (!node->is_leaf) {
        const int pos = bptree_node_search(tree, node, key);
        node = bptree_node_children(node, tree->max_keys)[pos];
        if (!node) return BPTREE_INTERNAL_ERROR;
    }
    int pos = bptree_node_search(tree, node, key);
    const bptree_key_t *keys = bptree_node_keys(node);
    if (pos < node->num_keys && tree->compare(key, &keys[pos]) == 0) {
        *out_value = bptree_node_values(node, tree->max_keys)[pos];
        return BPTREE_OK;
    }
    return BPTREE_KEY_NOT_FOUND;
}

BPTREE_API bptree_status bptree_remove(bptree *tree, const bptree_key_t *key) {
#define BPTREE_MAX_HEIGHT_REMOVE 64
    bptree_node *node_stack[BPTREE_MAX_HEIGHT_REMOVE];
    int index_stack[BPTREE_MAX_HEIGHT_REMOVE];
    int depth = 0;
    if (!tree || !tree->root || !key) return BPTREE_INVALID_ARGUMENT;
    if (tree->count == 0) return BPTREE_KEY_NOT_FOUND;
    bptree_node *node = tree->root;
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
    bptree_key_t *keys = bptree_node_keys(node);
    if (pos >= node->num_keys || tree->compare(key, &keys[pos]) != 0) {
        return BPTREE_KEY_NOT_FOUND;
    }
    const bptree_key_t deleted_key_copy = keys[pos];
    bptree_value_t *values = bptree_node_values(node, tree->max_keys);
    memmove(&keys[pos], &keys[pos + 1], (node->num_keys - pos - 1) * sizeof(bptree_key_t));
    memmove(&values[pos], &values[pos + 1], (node->num_keys - pos - 1) * sizeof(bptree_value_t));
    node->num_keys--;
    tree->count--;
    bptree_debug_print(tree->enable_debug, "Removed key from leaf. Node keys: %d, Tree count: %d\n",
                       node->num_keys, tree->count);
    if (pos == 0 && depth > 0 && node->num_keys > 0) {
        const int parent_child_idx = index_stack[depth - 1];
        if (parent_child_idx > 0) {
            const int separator_idx = parent_child_idx - 1;
            bptree_node *parent = node_stack[depth - 1];
            bptree_key_t *parent_keys = bptree_node_keys(parent);
            if (separator_idx < parent->num_keys &&
                tree->compare(&parent_keys[separator_idx], &deleted_key_copy) == 0) {
                bptree_debug_print(
                    tree->enable_debug,
                    "Updating parent separator key [%d] after deleting smallest leaf key.\n",
                    separator_idx);
                parent_keys[separator_idx] = keys[0];
            }
        }
    }
    const bool root_is_leaf = (depth == 0);
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

BPTREE_API bptree_status bptree_get_range(const bptree *tree, const bptree_key_t *start,
                                          const bptree_key_t *end, bptree_value_t **out_values,
                                          int *n_results) {
    if (!tree || !tree->root || !start || !end || !out_values || !n_results) {
        return BPTREE_INVALID_ARGUMENT;
    }
    *out_values = NULL;
    *n_results = 0;
    if (tree->compare(start, end) > 0) {        return BPTREE_INVALID_ARGUMENT;    }
    if (tree->count == 0) {        return BPTREE_OK;    }
    bptree_node *node = tree->root;
    while (!node->is_leaf) {
        const int pos = bptree_node_search(tree, node, start);
        node = bptree_node_children(node, tree->max_keys)[pos];
        if (!node) return BPTREE_INTERNAL_ERROR;
    }
    int count = 0;
    bptree_node *current_node = node;
    bool past_end = false;
    while (current_node && !past_end) {
        const bptree_key_t *keys = bptree_node_keys(current_node);
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
    while (current_node && !past_end && index < count) {
        const bptree_key_t *keys = bptree_node_keys(current_node);
        const bptree_value_t *values = bptree_node_values(current_node, tree->max_keys);
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

BPTREE_API void bptree_free_range_results(bptree_value_t *results) { free(results); }

BPTREE_API bptree_stats bptree_get_stats(const bptree *tree) {
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

BPTREE_API bool bptree_check_invariants(const bptree *tree) {
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

BPTREE_API bool bptree_contains(const bptree *tree, const bptree_key_t *key) {
    bptree_value_t dummy_value;
    return (bptree_get(tree, key, &dummy_value) == BPTREE_OK);
}

BPTREE_API bptree *bptree_create(const int max_keys,
                                 int (*compare)(const bptree_key_t *, const bptree_key_t *),
                                 const bool enable_debug) {
    if (max_keys < 3) {
        fprintf(stderr, "[BPTREE CREATE] Error: max_keys must be at least 3.\n");
        return NULL;
    }
    bptree *tree = malloc(sizeof(bptree));
    if (!tree) {
        fprintf(stderr, "[BPTREE CREATE] Error: Failed to allocate memory for tree structure.\n");
        return NULL;
    }
    tree->count = 0;
    tree->height = 1;
    tree->enable_debug = enable_debug;
    tree->max_keys = max_keys;
    tree->min_internal_keys = ((max_keys + 1) / 2) - 1;
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

BPTREE_API void bptree_free(bptree *tree) {
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
