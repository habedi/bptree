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

-- Canonical serialization check
#guard (empty Nat Nat).serialize == "(leaf [])"
#guard match sampleTree with
  | .ok t => t.serialize == "(internal [30] [(leaf [10:100,20:200]),(leaf [30:300,40:400,50:500])])"
  | .error _ => false

-- Deletion and rebalancing checks
#guard match remove 3 (empty Nat Nat) 10 with
  | .error .keyNotFound => true
  | _ => false

private meta def sampleAfterRemoves : Except Status (BPlusTree Nat Nat) := do
  let t ← sampleTree
  let t1 ← remove 3 t 10
  let t2 ← remove 3 t1 30
  let t3 ← remove 3 t2 20
  let t4 ← remove 3 t3 40
  remove 3 t4 50

-- Executable invariant checks
#guard checkInvariants 3 (empty Nat Nat) == true
#guard match sampleTree with
  | .ok t => checkInvariants 3 t == true
  | .error _ => false

-- Iterator lowerBound and upperBound checks
#guard lowerBound (empty Nat Nat) 10 == none
#guard upperBound (empty Nat Nat) 10 == none
#guard match sampleTree with
  | .ok t =>
    lowerBound t 5 == some (10, 100) &&
    lowerBound t 25 == some (30, 300) &&
    lowerBound t 30 == some (30, 300) &&
    lowerBound t 51 == none &&
    upperBound t 30 == some (40, 400) &&
    upperBound t 50 == none &&
    iterFind t 30 == some (30, 300) &&
    iterFind t 25 == none &&
    (clear Nat Nat t).serialize == "(leaf [])"
  | .error _ => false

#guard match sampleAfterRemoves with
  | .ok t => t.count == 0 && t.height == 1 && t.serialize == "(leaf [])" && checkInvariants 3 t == true
  | .error _ => false

-- Verified theorem instantiations
example : Valid 3 (empty Nat Nat) :=
  valid_empty Nat ⟨by decide, by decide⟩

example : Valid 3 (empty Nat Nat) :=
  checkInvariants_sound (by decide)

example : lookup (empty Nat Nat) 7 = none :=
  lookup_empty Nat 7

example {t t' : BPlusTree Nat Nat} (h : insert 3 t 42 100 = .ok t') :
    t'.count = t.count + 1 :=
  insert_count h

example {t t' : BPlusTree Nat Nat} (h : remove 3 t 42 = .ok t') :
    t'.count + 1 = t.count :=
  remove_count h

example {t t' : BPlusTree Nat Nat} (hcc : ChildCountValid t)
    (h : insert 3 t 42 100 = .ok t') :
    ChildCountValid t' :=
  insert_preserves_ChildCountValid hcc h

example {t t' : BPlusTree Nat Nat} (hcc : ChildCountValid t)
    (hud : UniformDepth t t.height) (h : insert 3 t 42 100 = .ok t') :
    UniformDepth t' t'.height :=
  insert_preserves_UniformDepth hcc hud h

example {t t' : BPlusTree Nat Nat} (hro : RootOccupancy 3 t)
    (h : insert 3 t 42 100 = .ok t') :
    RootOccupancy 3 t' :=
  insert_preserves_RootOccupancy (by decide) hro h
