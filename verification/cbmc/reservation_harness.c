#include <assert.h>
#include <stdint.h>
#include <stdlib.h>

static int fail_countdown = -1;
static int live_allocs = 0;

static void* cbmc_aligned_alloc(size_t alignment, size_t size) {
    assert(alignment > 0);
    assert((alignment & (alignment - 1)) == 0);
    assert(size % alignment == 0);
    if (fail_countdown == 0) return NULL;
    if (fail_countdown > 0) fail_countdown--;
    void* ptr = malloc(size);
    if (ptr) live_allocs++;
    return ptr;
}

static void cbmc_free(void* ptr) {
    if (ptr) {
        live_allocs--;
        free(ptr);
    }
}

#define aligned_alloc cbmc_aligned_alloc
#define free cbmc_free
#define BPTREE_IMPLEMENTATION
#include "bptree.h"
#undef aligned_alloc
#undef free

int nondet_int(void);

int main(void) {
    bptree tree_storage = {.count = 0,
                           .height = 1,
                           .enable_debug = false,
                           .max_keys = 3,
                           .min_leaf_keys = 2,
                           .min_internal_keys = 1,
                           .compare = bptree_default_compare,
                           .root = NULL};
    bptree* tree = &tree_storage;

    int height = nondet_int();
    int splits = nondet_int();
    __CPROVER_assume(height >= 1 && height <= 8);
    __CPROVER_assume(splits >= 0 && splits <= height);
    tree->height = height;

    int fail_at = nondet_int();
    __CPROVER_assume(fail_at >= -1 && fail_at <= 9);
    fail_countdown = fail_at;

    // Exact preallocation and rollback loop from bptree_put (lines 1304-1318).
    const bool split_root = splits == tree->height;
    const int needed = splits + (split_root ? 1 : 0);
    bptree_node* reserved_nodes = NULL;
    bool failed = false;
    for (int i = needed - 1; i >= 0; --i) {
        bptree_node* node = bptree_node_alloc(tree, i == 0);
        if (!node) {
            while (reserved_nodes) {
                bptree_node* next = reserved_nodes->next;
                cbmc_free(reserved_nodes);
                reserved_nodes = next;
            }
            failed = true;
            break;
        }
        node->next = reserved_nodes;
        reserved_nodes = node;
    }
    fail_countdown = -1;

    if (failed) {
        // Allocation failure must leave zero leaked nodes.
        assert(reserved_nodes == NULL);
        assert(live_allocs == 0);
    } else {
        // Success must allocate exact count in leaf-to-root order (head is leaf, rest internal).
        assert(live_allocs == needed);
        int count = 0;
        bptree_node* cur = reserved_nodes;
        while (cur) {
            if (count == 0) {
                assert(cur->is_leaf == true);
            } else {
                assert(cur->is_leaf == false);
            }
            bptree_node* next = cur->next;
            cbmc_free(cur);
            cur = next;
            count++;
        }
        assert(count == needed);
        assert(live_allocs == 0);
    }

    return 0;
}
