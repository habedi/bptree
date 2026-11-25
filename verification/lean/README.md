## Bptree Formal Verification in Lean 4

### Overview

This directory contains a Lean 4 formal specification and reference model for the data structure described in `bptree.h`.
It is a verified mathematical reference model in Lean 4 paired with runtime invariant checks in C.
It is not a direct C-to-Lean proof of C code.

### Verification Coverage Summary

The current formalization covers node-level invariants, search semantics, leaf insertion, node splitting bounds, and preallocation split counting.

1. **Machine-Checked in Lean 4 (26 Theorems, Zero `sorry`)**:
   - Capacity and occupancy bounds for `minLeafKeys` and `minInternalKeys`
   - Structural validity (`Valid`) of the empty tree
   - Tree count consistency (`count_eq_toList_length`)
   - Child index bounds (`findChildIdx_le_length`) and leaf search soundness and completeness (`leafLookup_mem`, `leafLookup_of_mem_sorted`)
   - Leaf insertion length, sortedness preservation, lookup-after-insert, and duplicate-key rejection
   - Entry preservation (`splitLeafEntries_append`) and post-split occupancy bounds for both leaf splits and internal splits
   - Upper bounds on the pre-pass cascading split counter (`countContiguousSplits_le_height`, `reservedNodesNeeded_le_height_succ`)

2. **Modelled in Lean 4 and Checked with `#guard` (End-to-End Recursive Proofs Pending)**:
   - Multi-level recursive `lookup`, `insertNode`, `insert`, and `rangeQuery`
   - Full-tree invariant preservation across arbitrary `insert` sequences (`Valid maxKeys t -> Valid maxKeys t'`)

3. **Runtime-Checked in C Only (Not Yet Formalized in Lean 4)**:
   - Deletion (`bptree_remove`) and sibling borrowing or merging (`bptree_rebalance_up`)
   - Iterator state machine (`bptree_iter_begin`, `bptree_iter_next`, `bptree_iter_find`, `bptree_iter_lower_bound`, `bptree_iter_upper_bound`)
   - Leaf `next`-pointer chain continuity, height consistency, and total key count in `bptree_check_invariants`
   - C memory safety, pointer aliasing, and `aligned_alloc` failure atomicity (tested in `test/extras/test_safety.c` under ASan and UBSan)

### Verified Properties and Theorems

| Property | Status | Lean Module | Lean Theorem or Definition | C Counterpart in `bptree.h` |
|---|---|---|---|---|
| Capacity lower and upper bounds | Verified in Lean 4 | `Bptree.Basic` | `minLeafKeys_pos`, `minInternalKeys_pos`, `minLeafKeys_le_maxKeys`, `minInternalKeys_le_maxKeys` | `bptree_create` |
| Empty tree count and height | Verified in Lean 4 | `Bptree.Basic` | `count_empty`, `height_empty` | `bptree_create`, `bptree_count`, `bptree_height` |
| Tree count equals leaf entry count | Verified in Lean 4 | `Bptree.Basic` | `count_eq_toList_length` | `bptree_check_invariants` |
| Empty tree structural validity | Verified in Lean 4 | `Bptree.Invariants` | `uniformDepth_empty`, `valid_empty` | `bptree_create`, `bptree_check_invariants` |
| Empty tree lookup and containment | Verified in Lean 4 | `Bptree.Search` | `lookup_empty`, `contains_empty`, `contains_eq_isSome_lookup` | `bptree_get`, `bptree_contains` |
| Internal child index within bounds | Verified in Lean 4 | `Bptree.Search` | `findChildIdx_le_length` | `bptree_node_search` |
| Leaf lookup soundness and completeness | Verified in Lean 4 | `Bptree.Search` | `leafLookup_mem`, `leafLookup_of_mem_sorted` | `bptree_get`, `bptree_node_search` |
| Leaf duplicate-key rejection | Verified in Lean 4 | `Bptree.Insert` | `insertEntries_duplicate` | `bptree_insert_internal`, `bptree_put` |
| Leaf insertion length and lookup | Verified in Lean 4 | `Bptree.Insert` | `insertEntries_length`, `leafLookup_insertEntries_self` | `bptree_insert_internal` |
| Leaf insertion sortedness preservation | Verified in Lean 4 | `Bptree.Insert` | `insertEntries_preserves_sorted` | `bptree_insert_internal`, `bptree_check_invariants_node` |
| Leaf split entry preservation | Verified in Lean 4 | `Bptree.Insert` | `splitLeafEntries_append` | `bptree_insert_internal` |
| Leaf split occupancy bounds | Verified in Lean 4 | `Bptree.Insert` | `splitLeaf_left_bounds`, `splitLeaf_right_bounds` | `bptree_insert_internal` |
| Internal split occupancy bounds | Verified in Lean 4 | `Bptree.Insert` | `splitInternal_left_bounds`, `splitInternal_right_bounds` | `bptree_insert_internal` |
| Preallocation split counter upper bounds | Verified in Lean 4 | `Bptree.Insert` | `countContiguousSplits_le_height`, `reservedNodesNeeded_le_height_succ` | `bptree_put` |
| Multi-level search, insert, and range query | Modelled (`#guard` tested) | `Bptree.Search`, `Bptree.Insert` | `lookup`, `insertNode`, `insert`, `rangeQuery` | `bptree_get`, `bptree_put`, `bptree_get_range` |
| Full-tree insert invariant preservation | Planned | `Bptree.Insert` | `Valid` | `bptree_put`, `bptree_check_invariants` |
| Full-tree lookup-after-insert | Planned | `Bptree.Insert` | `lookup`, `insert` | `bptree_get`, `bptree_put` |
| Deletion and upward rebalancing | Planned | `Bptree.Remove` (planned) | `bptree_remove` model | `bptree_remove`, `bptree_rebalance_up` |
| Leaf `next`-pointer chain continuity | Runtime-checked in C | `Bptree.Basic` | `leaves` (abstract leaf order) | `bptree_check_invariants` |
| Allocation failure atomicity and memory safety | Tested in C (ASan, UBSan) | `Bptree.Insert` | `reservedNodesNeeded` | `bptree_put`, `test/extras/test_safety.c` |

### Project Structure

- `lean-toolchain`: Pinned Lean 4 version (`leanprover/lean4:v4.35.0`).
- `lakefile.lean`: Lake build configuration for the `Bptree` library, `Tests` driver, and `bptree-lean` executable.
- `Bptree.lean`: Root library module exporting all verified submodules.
- `Bptree/Basic.lean`: Core `BPlusTree` representation, capacity formulas, and basic tree metrics.
- `Bptree/Invariants.lean`: Formal invariant predicates matching `bptree_check_invariants`.
- `Bptree/Search.lean`: Key lookup, containment, range queries, and leaf search proofs.
- `Bptree/Insert.lean`: Leaf and internal splitting, insertion, split reservation counting, and node-level proofs.
- `Tests.lean`: Compile-time `#guard` evaluations and theorem checks.
- `Main.lean`: Executable runner demonstrating insertion, splitting, and range queries.

## Build and Test Commands

Run these commands from the repository root after `nix develop`:

```bash
make lean-build   # Compile the Lean library and executable
make lean-test    # Run #guard and theorem checks in Tests.lean
make lean-lint    # Build with warnings treated as errors (--wfail)
make lean-diff    # Run differential tests comparing C and Lean 4 tree structures
make -C verification/lean run   # Run Main.lean
```
