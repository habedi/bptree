module

public import Bptree.Invariants

@[expose] public section

namespace Bptree

/-- Look up a key in a leaf entry list. -/
def leafLookup {V : Type} (entries : List (Nat × V)) (k : Nat) : Option V :=
  match entries with
  | [] => none
  | (k', v) :: rest => if k == k' then some v else leafLookup rest k

/-- Compute the child index to descend into for an internal node, matching `bptree_node_search`.
    Advances while `key ≥ keys[i]`. -/
def findChildIdx (keys : List Nat) (k : Nat) : Nat :=
  (keys.takeWhile (· ≤ k)).length

/-- Look up a key in the B+ tree. -/
def lookup {V : Type} (t : BPlusTree Nat V) (k : Nat) : Option V :=
  match t with
  | .leaf entries => leafLookup entries k
  | .internal keys children =>
    let idx := findChildIdx keys k
    match _hc : children[idx]? with
    | some c => lookup c k
    | none => none
  termination_by sizeOf t
  decreasing_by
    simp_wf
    have hmem : c ∈ children := List.mem_of_getElem? _hc
    have hsz := List.sizeOf_lt_of_mem hmem
    omega

/-- Check whether a key is present in the B+ tree, matching `bptree_contains`. -/
def contains {V : Type} (t : BPlusTree Nat V) (k : Nat) : Bool :=
  (lookup t k).isSome

/-- Collect all values whose keys lie in `[startKey, endKey]` in sorted order,
    matching `bptree_get_range`. -/
def rangeQuery {V : Type} (t : BPlusTree Nat V) (startKey endKey : Nat) : List V :=
  ((t.toList).filter (fun p => startKey ≤ p.1 ∧ p.1 ≤ endKey)).map Prod.snd

/-- Lookup on an empty tree always returns `none`. -/
theorem lookup_empty (V : Type) (k : Nat) : lookup (empty Nat V) k = none := by
  simp [empty, lookup, leafLookup]

/-- `contains` on an empty tree always returns `false`. -/
theorem contains_empty (V : Type) (k : Nat) : contains (empty Nat V) k = false := by
  simp [contains, lookup_empty]

/-- `contains` is equivalent to `(lookup t k).isSome`. -/
theorem contains_eq_isSome_lookup {V : Type} (t : BPlusTree Nat V) (k : Nat) :
    contains t k = (lookup t k).isSome := by
  rfl

/-- `findChildIdx` never exceeds `keys.length`. -/
theorem findChildIdx_le_length (keys : List Nat) (k : Nat) :
    findChildIdx keys k ≤ keys.length := by
  unfold findChildIdx
  induction keys with
  | nil => simp
  | cons x xs ih =>
    simp only [List.takeWhile_cons, List.length_cons]
    split_ifs
    · simp; omega
    · simp

/-- If `leafLookup entries k = some v`, then `(k, v)` is a member of `entries`. -/
theorem leafLookup_mem {V : Type} {entries : List (Nat × V)} {k : Nat} {v : V}
    (h : leafLookup entries k = some v) : (k, v) ∈ entries := by
  induction entries with
  | nil =>
    simp [leafLookup] at h
  | cons hd tl ih =>
    rcases hd with ⟨k', v'⟩
    simp only [leafLookup] at h
    by_cases hkk : k == k'
    · simp only [hkk, ↓reduceIte, Option.some.injEq] at h
      have hkeq : k = k' := beq_iff_eq.mp hkk
      subst hkeq h
      simp
    · simp only [hkk, Bool.false_eq_true, ↓reduceIte] at h
      exact List.mem_cons_of_mem (k', v') (ih h)

/-- If `entries` is strictly sorted by key and `(k, v) ∈ entries`, then `leafLookup` finds `v`. -/
theorem leafLookup_of_mem_sorted {V : Type} {entries : List (Nat × V)} {k : Nat} {v : V}
    (hsort : EntriesSorted entries) (hmem : (k, v) ∈ entries) :
    leafLookup entries k = some v := by
  induction entries with
  | nil =>
    simp at hmem
  | cons hd tl ih =>
    rcases hd with ⟨k', v'⟩
    simp only [EntriesSorted, List.map_cons, List.pairwise_cons, List.mem_map, Prod.exists] at hsort
    rcases hsort with ⟨hlt, htl⟩
    rcases List.mem_cons.mp hmem with heq | hmem_tl
    · injection heq with hk hv
      subst hk hv
      simp [leafLookup]
    · have hne : k' < k := hlt k ⟨k, v, hmem_tl, rfl⟩
      have hbeq : (k == k') = false := beq_false_of_ne (Nat.ne_of_gt hne)
      simp only [leafLookup, hbeq, Bool.false_eq_true, ↓reduceIte]
      exact ih htl hmem_tl

end Bptree

end
