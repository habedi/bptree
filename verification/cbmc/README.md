## Bptree Bounded Model Checking with CBMC

### Overview

This directory contains C bounded model checking harnesses for `include/bptree.h` using [CBMC](https://www.cprover.org/cbmc/).
While the Lean 4 subproject (`verification/lean/`) proves algorithmic and structural properties on a pure functional model, these harnesses symbolically verify C-level memory layout, comparator total-order and overflow freedom, API argument validation, and cascading split node-reservation atomicity directly on `include/bptree.h`.

### Verified C Properties

| # | Harness | Properties Checked | Bounds |
|---|---|---|---|
| 1 | `layout_harness.c` | `bptree_keys_area_size` alignment, `bptree_node_alloc_size` capacity, `aligned_alloc` size rounding, and split/merge occupancy arithmetic | All `max_keys ∈ [3, 4096]` |
| 2 | `compare_harness.c` | `bptree_default_compare` reflexivity, antisymmetry, transitivity, trichotomy, and absence of signed overflow | All `int64_t a, b, c` |
| 3 | `api_harness.c` | NULL-pointer safety, `bptree_create` invalid capacity rejection, `bptree_get_range` `start > end` rejection, and empty-tree iterator contracts | All `int` capacities and `int64_t` keys |
| 4 | `reservation_harness.c` | `bptree_put` preallocation count (`splits + split_root`), leaf-to-root node order, and zero leaked allocations on OOM rollback | `height ∈ [1, 8]`, `splits ∈ [0, height]`, any fault step |

### Running CBMC

Run from the repository root inside `nix develop`:

```bash
make cbmc
```
