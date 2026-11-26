#include <assert.h>
#include <stdalign.h>
#include <stdint.h>

#define BPTREE_IMPLEMENTATION
#include "bptree.h"

int nondet_int(void);

int main(void) {
    int max_keys = nondet_int();
    __CPROVER_assume(max_keys >= 3 && max_keys <= 4096);

    // 1. Verify keys area alignment and capacity for all supported max_keys.
    size_t keys_area = bptree_keys_area_size(max_keys);
    assert(keys_area >= (size_t)(max_keys + 1) * sizeof(bptree_key_t));
    assert(keys_area % alignof(bptree_value_t) == 0);
    assert(keys_area % alignof(bptree_node*) == 0);

    // 2. Verify leaf and internal node allocation sizes fit header + keys + payload.
    size_t leaf_sz = bptree_node_alloc_size(&(bptree){.max_keys = max_keys}, true);
    size_t internal_sz = bptree_node_alloc_size(&(bptree){.max_keys = max_keys}, false);
    assert(leaf_sz >= sizeof(bptree_node) + keys_area + (size_t)(max_keys + 1) * sizeof(bptree_value_t));
    assert(internal_sz >=
           sizeof(bptree_node) + keys_area + (size_t)(max_keys + 2) * sizeof(bptree_node*));

    // 3. Verify aligned_alloc size rounding produces a multiple of max_align.
    size_t max_align = alignof(bptree_node);
    if (alignof(bptree_key_t) > max_align) max_align = alignof(bptree_key_t);
    if (alignof(bptree_value_t) > max_align) max_align = alignof(bptree_value_t);
    size_t rounded_leaf = (leaf_sz + max_align - 1) & ~(max_align - 1);
    assert(rounded_leaf >= leaf_sz);
    assert(rounded_leaf % max_align == 0);

    // 4. Verify split and merge occupancy arithmetic for all max_keys in [3, 4096].
    int min_leaf = (max_keys + 1) / 2;
    int min_internal = ((max_keys + 2) / 2) - 1;
    assert(min_leaf >= 1 && min_leaf <= max_keys);
    assert(min_internal >= 1 && min_internal <= max_keys);

    // Leaf split of max_keys + 1 keys.
    int leaf_split_left = (max_keys + 2) / 2;
    int leaf_split_right = (max_keys + 1) - leaf_split_left;
    assert(leaf_split_left >= min_leaf && leaf_split_left <= max_keys);
    assert(leaf_split_right >= min_leaf && leaf_split_right <= max_keys);

    // Internal split of max_keys + 1 keys.
    int int_split_left = (max_keys + 1) / 2;
    int int_split_right = (max_keys + 1) - int_split_left - 1;
    assert(int_split_left >= min_internal && int_split_left <= max_keys);
    assert(int_split_right >= min_internal && int_split_right <= max_keys);

    // Leaf and internal merge upper bounds (never overflow max_keys).
    assert((min_leaf - 1) + min_leaf <= max_keys);
    assert((min_internal - 1) + 1 + min_internal <= max_keys);

    return 0;
}
