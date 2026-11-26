## Bptree Formal Verification in Lean 4

### Overview

This directory contains a Lean 4 formal specification and reference model for the data structure described in `bptree.h`.
It is a verified mathematical reference model in Lean 4 paired with runtime invariant checks in C.
It is not a direct C-to-Lean proof of C code.

### Verification Coverage Summary

The current formalization covers node-level invariants, search semantics, leaf insertion, node splitting bounds, and preallocation split counting.

1. **Machine-Checked in Lean 4 (48 Theorems, Zero `sorry`)**:
   - Capacity and occupancy bounds for `minLeafKeys` and `minInternalKeys`
   - Structural validity (`Valid`) of the empty tree
   - Tree count consistency (`count_eq_toList_length`)
   - Child index bounds (`findChildIdx_le_length`) and leaf search soundness and completeness (`leafLookup_mem`, `leafLookup_of_mem_sorted`)
   - Leaf insertion length, sortedness preservation, lookup-after-insert (`leafLookup_insertEntries_self`, `leafLookup_insertEntries_ne`), and duplicate-key rejection
   - Leaf deletion length, sortedness preservation, lookup-after-remove (`leafLookup_removeEntries_self`), and missing-key rejection (`remove_empty`, `removeEntries_not_found`)
   - Entry preservation (`splitLeafEntries_append`) and post-split occupancy bounds for both leaf splits and internal splits
   - Post-merge occupancy and overflow-freedom bounds for both leaf merges and internal merges (`mergeLeaf_bounds`, `mergeInternal_bounds`)
   - Upper bounds on the pre-pass cascading split counter (`countContiguousSplits_le_height`, `reservedNodesNeeded_le_height_succ`)

2. **Differential-Tested Between C and Lean 4 (`make lean-diff`)**:
   - Multi-level `lookup`, `insert`, `remove` (including `rebalance_up`, borrowing, merging, ancestor separator updates, and root height shrinking), and `rangeQuery`
   - Canonical S-expression serialization (`BPlusTree.serialize` vs. `test/extras/test_differential.c`) verified byte-for-byte across `max_keys ∈ {3, 4, 7, 12}` and sequential, duplicate, reverse, and pseudo-random mixed workloads

3. **Runtime-Checked in C Only (Not Yet Formalized in Lean 4)**:
   - Iterator state machine (`bptree_iter_begin`, `bptree_iter_next`, `bptree_iter_find`, `bptree_iter_lower_bound`, `bptree_iter_upper_bound`)
   - Leaf `next`-pointer chain continuity, height consistency, and total key count in `bptree_check_invariants`
   - C memory safety, pointer aliasing, and `aligned_alloc` failure atomicity (tested in `test/extras/test_safety.c` under ASan and UBSan)

## Verified Properties and Theorems

| # | Property | Status | Lean Module | Lean Theorem or Definition | C Counterpart in `bptree.h` |
|---|---|---|---|---|---|
| 1 | Capacity lower and upper bounds | Verified in Lean 4 | `Bptree.Basic` | `minLeafKeys_pos`, `minInternalKeys_pos`, `minLeafKeys_le_maxKeys`, `minInternalKeys_le_maxKeys` | `bptree_create` |
| 2 | Empty tree count and height | Verified in Lean 4 | `Bptree.Basic` | `count_empty`, `height_empty` | `bptree_create`, `bptree_count`, `bptree_height` |
| 3 | Tree count equals leaf entry count | Verified in Lean 4 | `Bptree.Basic` | `count_eq_toList_length` | `bptree_check_invariants` |
| 4 | Empty tree structural validity | Verified in Lean 4 | `Bptree.Invariants` | `uniformDepth_empty`, `valid_empty` | `bptree_create`, `bptree_check_invariants` |
| 5 | Executable invariant checker soundness | Verified in Lean 4 | `Bptree.Invariants` | `checkCapacity_iff`, `checkChildCount_sound`, `checkNonRootOccupancy_sound`, `checkRootOccupancy_sound`, `checkUniformDepth_sound`, `checkInvariants_empty` | `bptree_check_invariants` |
| 6 | Empty tree lookup and containment | Verified in Lean 4 | `Bptree.Search` | `lookup_empty`, `contains_empty`, `contains_eq_isSome_lookup` | `bptree_get`, `bptree_contains` |
| 7 | Internal child index within bounds | Verified in Lean 4 | `Bptree.Search` | `findChildIdx_le_length` | `bptree_node_search` |
| 8 | Leaf lookup soundness and completeness | Verified in Lean 4 | `Bptree.Search` | `leafLookup_mem`, `leafLookup_of_mem_sorted` | `bptree_get`, `bptree_node_search` |
| 9 | Range query and iterator bounds | Verified in Lean 4 | `Bptree.Search` | `rangeQuery_empty`, `mem_rangeQuery_iff`, `rangeQuery_gt`, `lowerBound_empty`, `upperBound_empty`, `lowerBound_mem`, `upperBound_mem`, `upperBound_eq_lowerBound_succ` | `bptree_get_range`, `bptree_iter_lower_bound`, `bptree_iter_upper_bound` |
| 9 | Leaf duplicate-key rejection | Verified in Lean 4 | `Bptree.Insert` | `insertEntries_duplicate` | `bptree_insert_internal`, `bptree_put` |
| 10 | Leaf insertion length and lookup | Verified in Lean 4 | `Bptree.Insert` | `insertEntries_length`, `leafLookup_insertEntries_self`, `leafLookup_insertEntries_ne` | `bptree_insert_internal` |
| 11 | Leaf insertion sortedness preservation | Verified in Lean 4 | `Bptree.Insert` | `insertEntries_preserves_sorted` | `bptree_insert_internal`, `bptree_check_invariants_node` |
| 12 | Leaf split entry preservation | Verified in Lean 4 | `Bptree.Insert` | `splitLeafEntries_append` | `bptree_insert_internal` |
| 13 | Leaf split occupancy bounds | Verified in Lean 4 | `Bptree.Insert` | `splitLeaf_left_bounds`, `splitLeaf_right_bounds` | `bptree_insert_internal` |
| 14 | Internal split occupancy bounds | Verified in Lean 4 | `Bptree.Insert` | `splitInternal_left_bounds`, `splitInternal_right_bounds` | `bptree_insert_internal` |
| 15 | Preallocation split counter upper bounds | Verified in Lean 4 | `Bptree.Insert` | `countContiguousSplits_le_height`, `reservedNodesNeeded_le_height_succ` | `bptree_put` |
| 16 | Empty tree and missing key removal | Verified in Lean 4 | `Bptree.Remove` | `remove_empty`, `removeEntries_not_found` | `bptree_remove` |
| 17 | Leaf removal length, sortedness, and lookup | Verified in Lean 4 | `Bptree.Remove` | `removeEntries_length`, `removeEntries_preserves_sorted`, `leafLookup_removeEntries_self` | `bptree_remove` |
| 18 | Leaf and internal merge overflow freedom | Verified in Lean 4 | `Bptree.Remove` | `mergeLeaf_bounds`, `mergeInternal_bounds` | `bptree_rebalance_up` |
| 19 | Multi-level search, insert, remove, and range | Modelled and differential-tested | `Bptree.Search`, `Bptree.Insert`, `Bptree.Remove` | `lookup`, `insert`, `remove`, `rangeQuery`, `BPlusTree.serialize`, `checkInvariants` | `bptree_get`, `bptree_put`, `bptree_remove`, `bptree_get_range` |
| 20 | Full-tree insert and remove invariant proofs | Planned | `Bptree.Insert`, `Bptree.Remove` | `Valid` | `bptree_put`, `bptree_remove`, `bptree_check_invariants` |
| 21 | Leaf `next`-pointer chain continuity | Runtime-checked in C | `Bptree.Basic` | `leaves` (abstract leaf order) | `bptree_check_invariants` |
| 22 | Allocation failure atomicity and memory safety | Tested in C (ASan, UBSan) | `Bptree.Insert` | `reservedNodesNeeded` | `bptree_put`, `test/extras/test_safety.c` |

## Project Structure

- `lean-toolchain`: Pinned Lean 4 version (`leanprover/lean4:v4.35.0`).
- `lakefile.lean`: Lake build configuration for the `Bptree` library, `Tests` driver, and `bptree-lean` executable.
- `Bptree.lean`: Root library module exporting all verified submodules.
- `Bptree/Basic.lean`: Core `BPlusTree` representation, capacity formulas, serialization, and basic tree metrics.
- `Bptree/Invariants.lean`: Formal invariant predicates matching `bptree_check_invariants`.
- `Bptree/Search.lean`: Key lookup, containment, range queries, and leaf search proofs.
- `Bptree/Insert.lean`: Leaf and internal splitting, insertion, split reservation counting, and node-level proofs.
- `Bptree/Remove.lean`: Leaf removal, ancestor separator updates, sibling borrowing, node merging, and merge-bound proofs.
- `Tests.lean`: Compile-time `#guard` evaluations and theorem checks.
- `Main.lean`: Executable runner for differential testing against `test/extras/test_differential.c`.

## Build and Test Commands

Run these commands from the repository root:

```bash
make lean-build   # Compile the Lean library and executable
make lean-test    # Run #guard and theorem checks in Tests.lean
make lean-lint    # Build with warnings treated as errors (--wfail)
make lean-diff    # Run differential tests comparing C and Lean 4 tree structures
make -C verification/lean run   # Run Main.lean
```
