module

meta import Bptree

open Bptree

-- Occupancy formula checks matching bptree_create
#guard minLeafKeys 3 == 2
#guard minInternalKeys 3 == 1
#guard minLeafKeys 4 == 2
#guard minInternalKeys 4 == 2
#guard minLeafKeys 7 == 4
#guard minInternalKeys 7 == 3

-- Empty tree checks
#guard (empty Nat Nat).count == 0
#guard (empty Nat Nat).height == 1
#guard lookup (empty Nat Nat) 42 == none
#guard contains (empty Nat Nat) 42 == false

-- Leaf insertion, duplicate rejection, and lookup checks
private meta def sampleTree : Except Status (BPlusTree Nat Nat) := do
  let t0 : BPlusTree Nat Nat := empty Nat Nat
  let t1 ← insert 3 t0 10 100
  let t2 ← insert 3 t1 20 200
  let t3 ← insert 3 t2 30 300
  let t4 ← insert 3 t3 40 400
  insert 3 t4 50 500

#guard match sampleTree with
  | .ok t =>
    t.count == 5 &&
    t.height == 2 &&
    lookup t 10 == some 100 &&
    lookup t 30 == some 300 &&
    lookup t 50 == some 500 &&
    lookup t 25 == none &&
    rangeQuery t 20 40 == [200, 300, 400]
  | .error _ => false

-- Duplicate key rejection check
#guard match sampleTree with
  | .ok t =>
    match insert 3 t 30 999 with
    | .error .duplicateKey => true
    | _ => false
  | .error _ => false

-- Preallocation split counter check
#guard match sampleTree with
  | .ok t => reservedNodesNeeded 3 t 60 ≤ t.height + 1
  | .error _ => false

-- Verified theorem instantiations
example : Valid 3 (empty Nat Nat) :=
  valid_empty Nat ⟨by decide, by decide⟩

example : lookup (empty Nat Nat) 7 = none :=
  lookup_empty Nat 7
