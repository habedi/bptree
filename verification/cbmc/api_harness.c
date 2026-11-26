#include <assert.h>
#include <stdint.h>
#include <stdlib.h>

static void* cbmc_aligned_alloc(size_t alignment, size_t size) {
    assert(alignment > 0);
    assert((alignment & (alignment - 1)) == 0);
    assert(size % alignment == 0);
    return malloc(size);
}

#define aligned_alloc cbmc_aligned_alloc
#define BPTREE_IMPLEMENTATION
#include "bptree.h"
#undef aligned_alloc

int nondet_int(void);
int64_t nondet_int64(void);

int main(void) {
    // 1. Null-pointer safety across public API.
    bptree_key_t k = nondet_int64();
    bptree_value_t out_val = NULL;
    bptree_value_t* range_vals = NULL;
    int range_count = -1;

    assert(bptree_count(NULL) == 0);
    assert(bptree_height(NULL) == 0);
    assert(bptree_max_keys(NULL) == 0);
    assert(!bptree_check_invariants(NULL));
    assert(!bptree_contains(NULL, &k));
    assert(bptree_put(NULL, &k, NULL) == BPTREE_INVALID_ARGUMENT);
    assert(bptree_get(NULL, &k, &out_val) == BPTREE_INVALID_ARGUMENT);
    assert(bptree_remove(NULL, &k) == BPTREE_INVALID_ARGUMENT);
    assert(bptree_get_range(NULL, &k, &k, &range_vals, &range_count) == BPTREE_INVALID_ARGUMENT);

    bptree_stats null_stats = bptree_get_stats(NULL);
    assert(null_stats.count == 0 && null_stats.height == 0 && null_stats.node_count == 0);
    bptree_clear(NULL);
    bptree_free(NULL);

    // 2. Empty tree creation and iterator contracts.
    bptree* tree = bptree_create(3, NULL, false);
    if (tree) {
        assert(bptree_count(tree) == 0);
        assert(bptree_height(tree) == 1);
        assert(bptree_max_keys(tree) == 3);
        assert(bptree_check_invariants(tree));
        assert(!bptree_contains(tree, &k));
        assert(bptree_remove(tree, &k) == BPTREE_KEY_NOT_FOUND);

        // Range query with start > end must be rejected.
        int64_t start_k = nondet_int64();
        int64_t end_k = nondet_int64();
        if (start_k > end_k) {
            assert(bptree_get_range(tree, &start_k, &end_k, &range_vals, &range_count) ==
                   BPTREE_INVALID_ARGUMENT);
            assert(range_vals == NULL && range_count == 0);
        } else {
            assert(bptree_get_range(tree, &start_k, &end_k, &range_vals, &range_count) ==
                   BPTREE_OK);
            assert(range_vals == NULL && range_count == 0);
        }

        // Iterator contracts on empty tree.
        bptree_iter it = bptree_iter_begin(tree);
        bptree_iter it2 = bptree_iter_find(tree, &k);
        assert(!bptree_iter_valid(&it));
        assert(!bptree_iter_valid(&it2));
        assert(bptree_iter_equal(&it, &it2));
        bptree_iter_next(&it);
        assert(!bptree_iter_valid(&it));

        bptree_free(tree);
    }

    // 3. Invalid capacity rejection for arbitrary int outside [3, 4096].
    int bad_max = nondet_int();
    __CPROVER_assume(bad_max < 3 || bad_max > 4096);
    assert(bptree_create(bad_max, NULL, false) == NULL);

    return 0;
}
