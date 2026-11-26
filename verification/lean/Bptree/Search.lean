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

/-- First entry in `t.toList` with key `≥ k`, matching `bptree_iter_lower_bound`. -/
def lowerBound {V : Type} (t : BPlusTree Nat V) (k : Nat) : Option (Nat × V) :=
  t.toList.find? (fun p => decide (k ≤ p.1))

/-- First entry in `t.toList` with key `> k`, matching `bptree_iter_upper_bound`. -/
def upperBound {V : Type} (t : BPlusTree Nat V) (k : Nat) : Option (Nat × V) :=
  t.toList.find? (fun p => decide (k < p.1))

/-- Forward iterator traversal sequence, matching `bptree_iter_begin` and `bptree_iter_next`. -/
def iterList {V : Type} (t : BPlusTree Nat V) : List (Nat × V) :=
  t.toList

/-- Return the `(key, value)` pair if `k` is in `t`, matching `bptree_iter_find`. -/
def iterFind {V : Type} (t : BPlusTree Nat V) (k : Nat) : Option (Nat × V) :=
  (lookup t k).map (fun v => (k, v))

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

/-- `rangeQuery` on an empty tree returns `[]`. -/
theorem rangeQuery_empty (V : Type) (lo hi : Nat) :
    rangeQuery (empty Nat V) lo hi = [] := by
  simp [rangeQuery, empty, BPlusTree.toList, BPlusTree.leaves]

/-- Soundness and completeness of `rangeQuery` with respect to `t.toList` and `[lo, hi]`. -/
theorem mem_rangeQuery_iff {V : Type} (t : BPlusTree Nat V) (lo hi : Nat) (v : V) :
    v ∈ rangeQuery t lo hi ↔ ∃ k, (k, v) ∈ t.toList ∧ lo ≤ k ∧ k ≤ hi := by
  simp only [rangeQuery, List.mem_map, List.mem_filter, decide_eq_true_eq, Prod.exists]
  constructor
  · rintro ⟨k, v', ⟨hmem, hlo, hhi⟩, rfl⟩
    exact ⟨k, hmem, hlo, hhi⟩
  · rintro ⟨k, hmem, hlo, hhi⟩
    exact ⟨k, v, ⟨hmem, hlo, hhi⟩, rfl⟩

/-- An inverted range (`hi < lo`) always produces an empty result list. -/
theorem rangeQuery_gt {V : Type} (t : BPlusTree Nat V) {lo hi : Nat} (hgt : hi < lo) :
    rangeQuery t lo hi = [] := by
  simp only [rangeQuery, List.map_eq_nil_iff, List.filter_eq_nil_iff, decide_eq_true_eq]
  intro p _ hp
  omega

/-- `lowerBound` on an empty tree always returns `none`. -/
theorem lowerBound_empty (V : Type) (k : Nat) :
    lowerBound (empty Nat V) k = none := by
  simp [lowerBound, empty, BPlusTree.toList, BPlusTree.leaves]

/-- `upperBound` on an empty tree always returns `none`. -/
theorem upperBound_empty (V : Type) (k : Nat) :
    upperBound (empty Nat V) k = none := by
  simp [upperBound, empty, BPlusTree.toList, BPlusTree.leaves]

/-- Soundness of `lowerBound`: any returned entry belongs to `t.toList` with key `≥ k`. -/
theorem lowerBound_mem {V : Type} {t : BPlusTree Nat V} {k k' : Nat} {v : V}
    (h : lowerBound t k = some (k', v)) : (k', v) ∈ t.toList ∧ k ≤ k' := by
  have hfind := List.find?_some h
  have hmem := List.mem_of_find?_eq_some h
  simp only [decide_eq_true_eq] at hfind
  exact ⟨hmem, hfind⟩

/-- Soundness of `upperBound`: any returned entry belongs to `t.toList` with key `> k`. -/
theorem upperBound_mem {V : Type} {t : BPlusTree Nat V} {k k' : Nat} {v : V}
    (h : upperBound t k = some (k', v)) : (k', v) ∈ t.toList ∧ k < k' := by
  have hfind := List.find?_some h
  have hmem := List.mem_of_find?_eq_some h
  simp only [decide_eq_true_eq] at hfind
  exact ⟨hmem, hfind⟩

/-- For `Nat` keys, `upperBound t k` is identical to `lowerBound t (k + 1)`. -/
theorem upperBound_eq_lowerBound_succ {V : Type} (t : BPlusTree Nat V) (k : Nat) :
    upperBound t k = lowerBound t (k + 1) := by
  rfl

/-- `iterFind` on an empty tree always returns `none`. -/
theorem iterFind_empty (V : Type) (k : Nat) :
    iterFind (empty Nat V) k = none := by
  simp [iterFind, lookup_empty]

/-- `iterFind t k = some p` iff `p.1 = k` and `lookup t k = some p.2`. -/
theorem iterFind_eq_some_iff {V : Type} (t : BPlusTree Nat V) (k : Nat) (p : Nat × V) :
    iterFind t k = some p ↔ p.1 = k ∧ lookup t k = some p.2 := by
  simp only [iterFind, Option.map_eq_some_iff]
  constructor
  · rintro ⟨v, hv, rfl⟩
    exact ⟨rfl, hv⟩
  · rintro ⟨hk, hv⟩
    exact ⟨p.2, hv, by ext <;> simp [hk]⟩

private theorem mem_toList_of_mem_children {K V : Type} {c : BPlusTree K V}
    {children : List (BPlusTree K V)} {keys : List K} {p : K × V}
    (hc : c ∈ children) (hp : p ∈ c.toList) :
    p ∈ (BPlusTree.internal keys children).toList := by
  simp only [BPlusTree.toList, BPlusTree.leaves, List.mem_flatten] at hp ⊢
  rcases hp with ⟨l, hl_leaves, hp_l⟩
  exact ⟨l, List.mem_flatMap.mpr ⟨c, hc, hl_leaves⟩, hp_l⟩

/-- Full-tree search soundness: if `lookup t k = some v`, then `(k, v) ∈ t.toList`. -/
theorem lookup_mem_toList {V : Type} :
    (t : BPlusTree Nat V) → {k : Nat} → {v : V} →
    lookup t k = some v → (k, v) ∈ t.toList
  | .leaf entries, k, v, h => by
    simp only [lookup] at h
    simpa [BPlusTree.toList, BPlusTree.leaves] using leafLookup_mem h
  | .internal keys children, k, v, h => by
    simp only [lookup] at h
    split at h
    · rename_i c hc
      have hmem : c ∈ children := List.mem_of_getElem? hc
      exact mem_toList_of_mem_children hmem (lookup_mem_toList c h)
    · simp at h
  termination_by t => sizeOf t
  decreasing_by
    simp_wf
    have hsz := List.sizeOf_lt_of_mem hmem
    omega

/-- Full-tree containment soundness: if `contains t k = true`, then some `(k, v) ∈ t.toList`. -/
theorem contains_true_exists_mem_toList {V : Type} {t : BPlusTree Nat V} {k : Nat}
    (h : contains t k = true) : ∃ v, (k, v) ∈ t.toList := by
  simp only [contains, Option.isSome_iff_exists] at h
  rcases h with ⟨v, hv⟩
  exact ⟨v, lookup_mem_toList t hv⟩

/-- If `keys` is sorted and `keys[j] ≤ k`, then `findChildIdx keys k` advances past `j`. -/
theorem findChildIdx_gt_of_le :
    (keys : List Nat) → {k j : Nat} → (hj : j < keys.length) →
    KeysSorted keys → keys[j] ≤ k → j < findChildIdx keys k
  | [], _, _, hj, _, _ => by simp at hj
  | x :: xs, k, 0, _, _, hle => by
    simp only [List.getElem_cons_zero] at hle
    simp only [findChildIdx, List.takeWhile_cons, decide_eq_true hle, ↓reduceIte,
      List.length_cons]
    omega
  | x :: xs, k, j + 1, hj, hsort, hle => by
    simp only [KeysSorted, List.pairwise_cons] at hsort
    simp only [List.length_cons] at hj
    simp only [List.getElem_cons_succ] at hle
    have hx_mem : xs[j] ∈ xs := List.getElem_mem (by omega)
    have hx_lt : x < xs[j] := hsort.1 xs[j] hx_mem
    have hx_le : x ≤ k := by omega
    have ih := findChildIdx_gt_of_le xs (k := k) (j := j) (by omega) hsort.2 hle
    simp only [findChildIdx, List.takeWhile_cons, decide_eq_true hx_le, ↓reduceIte,
      List.length_cons] at ih ⊢
    omega

/-- If `k < keys[j]`, then `findChildIdx keys k ≤ j`. -/
theorem findChildIdx_le_of_lt :
    (keys : List Nat) → {k j : Nat} → (hj : j < keys.length) →
    k < keys[j] → findChildIdx keys k ≤ j
  | [], _, _, hj, _ => by simp at hj
  | x :: xs, k, 0, _, hlt => by
    simp only [List.getElem_cons_zero] at hlt
    have hnle : ¬(decide (x ≤ k) = true) := by simp; omega
    simp only [findChildIdx, List.takeWhile_cons, hnle, Bool.false_eq_true, ↓reduceIte,
      List.length_nil]
    omega
  | x :: xs, k, j + 1, hj, hlt => by
    simp only [List.length_cons] at hj
    simp only [List.getElem_cons_succ] at hlt
    have ih := findChildIdx_le_of_lt xs (k := k) (j := j) (by omega) hlt
    simp only [findChildIdx, List.takeWhile_cons] at ih ⊢
    split_ifs
    · simp only [List.length_cons]; omega
    · simp

/-- If `k` satisfies the separator bounds of child `i`, then `findChildIdx keys k = i`. -/
theorem findChildIdx_eq_of_bounds {keys : List Nat} {k i : Nat}
    (hi_le : i ≤ keys.length) (hsort : KeysSorted keys)
    (hlt : ∀ (j : Nat) (hj : j < keys.length), i ≤ j → k < keys[j])
    (hle : ∀ (j : Nat) (hj : j < keys.length), j < i → keys[j] ≤ k) :
    findChildIdx keys k = i := by
  have hupper : findChildIdx keys k ≤ i := by
    by_cases hi_lt : i < keys.length
    · exact findChildIdx_le_of_lt keys hi_lt (hlt i hi_lt (Nat.le_refl i))
    · have hlen := findChildIdx_le_length keys k
      omega
  by_cases hlt_idx : findChildIdx keys k < i
  · have hgt := findChildIdx_gt_of_le keys (j := findChildIdx keys k) (by omega) hsort
      (hle (findChildIdx keys k) (by omega) hlt_idx)
    omega
  · omega

private theorem mem_children_of_mem_internal_toList {K V : Type} {keys : List K}
    {children : List (BPlusTree K V)} {p : K × V}
    (hp : p ∈ (BPlusTree.internal keys children).toList) :
    ∃ (i : Nat) (hi : i < children.length), p ∈ children[i].toList := by
  simp only [BPlusTree.toList, BPlusTree.leaves, List.mem_flatten, List.mem_flatMap] at hp
  rcases hp with ⟨l, ⟨c, hc_mem, hl_leaves⟩, hp_l⟩
  rcases List.mem_iff_getElem.mp hc_mem with ⟨i, hi, rfl⟩
  refine ⟨i, hi, ?_⟩
  simp only [BPlusTree.toList, List.mem_flatten]
  exact ⟨l, hl_leaves, hp_l⟩

/-- Full-tree search completeness: in an ordered, sorted, child-count-valid tree,
    every `(k, v) ∈ t.toList` is found by `lookup t k`. -/
theorem lookup_of_mem_toList {V : Type} :
    (t : BPlusTree Nat V) → {k : Nat} → {v : V} →
    NodeSorted t → ChildCountValid t → SubtreesOrdered t →
    (k, v) ∈ t.toList → lookup t k = some v
  | .leaf entries, k, v, hsort, _, _, hmem => by
    simp only [NodeSorted] at hsort
    simp only [BPlusTree.toList, BPlusTree.leaves, List.flatten_cons, List.flatten_nil,
      List.append_nil] at hmem
    simp only [lookup]
    exact leafLookup_of_mem_sorted hsort hmem
  | .internal keys children, k, v, hsort, hcc, hord, hmem => by
    simp only [NodeSorted] at hsort
    simp only [ChildCountValid] at hcc
    simp only [SubtreesOrdered] at hord
    rcases mem_children_of_mem_internal_toList hmem with ⟨i, hi, hmem_i⟩
    have hidx : findChildIdx keys k = i :=
      findChildIdx_eq_of_bounds (by omega) hsort.1
        (fun j hj hij => (hord.2 i j hi hj (k, v) hmem_i).1 hij)
        (fun j hj hji => (hord.2 i j hi hj (k, v) hmem_i).2 hji)
    have hc_mem : children[i] ∈ children := List.getElem_mem hi
    have ih := lookup_of_mem_toList children[i] (hsort.2 children[i] hc_mem)
      (hcc.2 children[i] hc_mem) (hord.1 children[i] hc_mem) hmem_i
    simp only [lookup]
    rw [hidx, List.getElem?_eq_getElem hi]
    exact ih
  termination_by t => sizeOf t
  decreasing_by
    simp_wf
    have hsz := List.sizeOf_lt_of_mem hc_mem
    omega

/-- Complete `lookup` ↔ `toList` equivalence for valid trees. -/
theorem lookup_eq_some_iff_mem_toList {V : Type} {t : BPlusTree Nat V} {k : Nat} {v : V}
    (hsort : NodeSorted t) (hcc : ChildCountValid t) (hord : SubtreesOrdered t) :
    lookup t k = some v ↔ (k, v) ∈ t.toList :=
  ⟨lookup_mem_toList t, lookup_of_mem_toList t hsort hcc hord⟩

/-- Complete `contains` ↔ `toList` equivalence for valid trees. -/
theorem contains_eq_true_iff_exists_mem_toList {V : Type} {t : BPlusTree Nat V} {k : Nat}
    (hsort : NodeSorted t) (hcc : ChildCountValid t) (hord : SubtreesOrdered t) :
    contains t k = true ↔ ∃ v, (k, v) ∈ t.toList := by
  constructor
  · exact contains_true_exists_mem_toList
  · rintro ⟨v, hv⟩
    simp only [contains, lookup_of_mem_toList t hsort hcc hord hv, Option.isSome_some]

end Bptree

end
