module

public import Bptree.Basic

@[expose] public section

namespace Bptree

/-- Strictly increasing key list. -/
def KeysSorted (keys : List Nat) : Prop :=
  keys.Pairwise (· < ·)

/-- Strictly increasing key-value entry list by key. -/
def EntriesSorted {V : Type} (entries : List (Nat × V)) : Prop :=
  (entries.map Prod.fst).Pairwise (· < ·)

/-- Every node in the tree has strictly increasing keys. -/
def NodeSorted {V : Type} : BPlusTree Nat V → Prop
  | .leaf entries => EntriesSorted entries
  | .internal keys children => KeysSorted keys ∧ ∀ c ∈ children, NodeSorted c

/-- All leaves reside at the exact same depth `d`. -/
def UniformDepth {K V : Type} : BPlusTree K V → Nat → Prop
  | .leaf _, 1 => True
  | .leaf _, _ => False
  | .internal _ children, d + 1 => children ≠ [] ∧ ∀ c ∈ children, UniformDepth c d
  | .internal _ _, 0 => False

/-- Every internal node has `keys.length + 1` children. -/
def ChildCountValid {K V : Type} : BPlusTree K V → Prop
  | .leaf _ => True
  | .internal keys children => children.length = keys.length + 1 ∧ ∀ c ∈ children, ChildCountValid c

/-- Non-root node occupancy constraints matching `bptree_check_invariants_node`. -/
def NonRootOccupancy {K V : Type} (maxKeys : Nat) : BPlusTree K V → Prop
  | .leaf entries => minLeafKeys maxKeys ≤ entries.length ∧ entries.length ≤ maxKeys
  | .internal keys children =>
    minInternalKeys maxKeys ≤ keys.length ∧
    keys.length ≤ maxKeys ∧
    ∀ c ∈ children, NonRootOccupancy maxKeys c

/-- Root node occupancy constraints matching `bptree_check_invariants_node`. -/
def RootOccupancy {K V : Type} (maxKeys : Nat) : BPlusTree K V → Prop
  | .leaf entries => entries.length ≤ maxKeys
  | .internal keys children =>
    1 ≤ keys.length ∧
    keys.length ≤ maxKeys ∧
    ∀ c ∈ children, NonRootOccupancy maxKeys c

/-- Smallest key in the leftmost leaf of a subtree, if non-empty. -/
def minKey {K V : Type} : BPlusTree K V → Option K
  | .leaf entries => entries.head?.map Prod.fst
  | .internal _ children =>
    match children with
    | [] => none
    | c :: _ => minKey c

/-- Helper to get the last element of a list, structurally recursive. -/
def listLast? {α : Type} : List α → Option α
  | [] => none
  | [x] => some x
  | _ :: xs => listLast? xs

/-- Largest key in the rightmost leaf of a subtree, if non-empty. -/
def maxKey {K V : Type} : BPlusTree K V → Option K
  | .leaf entries => (listLast? entries).map Prod.fst
  | .internal _ children =>
    match children with
    | [] => none
    | c :: cs =>
      match _hl : listLast? cs with
      | none => maxKey c
      | some lastC => maxKey lastC
  termination_by t => sizeOf t
  decreasing_by
    · simp_wf; omega
    · simp_wf
      have hmem : ∀ {α : Type} {xs : List α} {x : α}, listLast? xs = some x → x ∈ xs := by
        intro α xs x h
        induction xs with
        | nil => simp [listLast?] at h
        | cons y ys ih =>
          cases ys with
          | nil =>
            simp [listLast?] at h
            subst h
            simp
          | cons z zs =>
            simp only [listLast?] at h
            exact List.mem_cons_of_mem y (ih h)
      have hmem_c : lastC ∈ cs := hmem _hl
      have hsz := List.sizeOf_lt_of_mem hmem_c
      omega

/-- Separator key correctness matching `bptree_check_invariants_node`:
    for each separator `k = keys[i]`, `max(children[i]) < k` and `k = min(children[i + 1])`. -/
def SeparatorsValid {V : Type} : BPlusTree Nat V → Prop
  | .leaf _ => True
  | .internal keys children =>
    (∀ c ∈ children, SeparatorsValid c) ∧
    (∀ (i : Nat) (hi : i < keys.length) (hl : i < children.length) (hr : i + 1 < children.length),
      (∃ m, maxKey children[i] = some m ∧ m < keys[i]) ∧
      minKey children[i + 1] = some keys[i])

/-- Complete structural validity predicate matching `bptree_check_invariants`. -/
def Valid {V : Type} (maxKeys : Nat) (t : BPlusTree Nat V) : Prop :=
  validCapacity maxKeys ∧
  NodeSorted t ∧
  ChildCountValid t ∧
  RootOccupancy maxKeys t ∧
  UniformDepth t t.height ∧
  SeparatorsValid t

/-- An empty tree has uniform depth 1. -/
theorem uniformDepth_empty (V : Type) : UniformDepth (empty Nat V) 1 := by
  simp [empty, UniformDepth]

/-- An empty tree satisfies all structural invariants for any valid capacity. -/
theorem valid_empty {maxKeys : Nat} (V : Type) (hcap : validCapacity maxKeys) :
    Valid maxKeys (empty Nat V) := by
  refine ⟨hcap, ?_, ?_, ?_, ?_, ?_⟩
  · simp [empty, NodeSorted, EntriesSorted]
  · simp [empty, ChildCountValid]
  · simp [empty, RootOccupancy]
  · simp [empty, BPlusTree.height, UniformDepth]
  · simp [empty, SeparatorsValid]

/-- Executable capacity check matching `bptree_create`. -/
def checkCapacity (maxKeys : Nat) : Bool :=
  decide (3 ≤ maxKeys ∧ maxKeys ≤ 4096)

/-- Executable strict-increasing check for a list of keys. -/
def checkKeysSorted : List Nat → Bool
  | [] => true
  | [_] => true
  | k1 :: k2 :: ks => decide (k1 < k2) && checkKeysSorted (k2 :: ks)

mutual
  /-- Executable check that every node in the tree has strictly increasing keys. -/
  def checkNodeSorted {V : Type} : BPlusTree Nat V → Bool
    | .leaf entries => checkKeysSorted (entries.map Prod.fst)
    | .internal keys children => checkKeysSorted keys && checkNodeSortedList children

  /-- Helper for `checkNodeSorted` over child lists. -/
  def checkNodeSortedList {V : Type} : List (BPlusTree Nat V) → Bool
    | [] => true
    | c :: cs => checkNodeSorted c && checkNodeSortedList cs
end

mutual
  /-- Executable check that every internal node has `keys.length + 1` children. -/
  def checkChildCount {K V : Type} : BPlusTree K V → Bool
    | .leaf _ => true
    | .internal keys children =>
      (children.length == keys.length + 1) && checkChildCountList children

  /-- Helper for `checkChildCount` over child lists. -/
  def checkChildCountList {K V : Type} : List (BPlusTree K V) → Bool
    | [] => true
    | c :: cs => checkChildCount c && checkChildCountList cs
end

mutual
  /-- Executable non-root occupancy check matching `bptree_check_invariants_node`. -/
  def checkNonRootOccupancy {K V : Type} (maxKeys : Nat) : BPlusTree K V → Bool
    | .leaf entries =>
      decide (minLeafKeys maxKeys ≤ entries.length ∧ entries.length ≤ maxKeys)
    | .internal keys children =>
      decide (minInternalKeys maxKeys ≤ keys.length ∧ keys.length ≤ maxKeys) &&
      checkNonRootOccupancyList maxKeys children

  /-- Helper for `checkNonRootOccupancy` over child lists. -/
  def checkNonRootOccupancyList {K V : Type} (maxKeys : Nat) : List (BPlusTree K V) → Bool
    | [] => true
    | c :: cs => checkNonRootOccupancy maxKeys c && checkNonRootOccupancyList maxKeys cs
end

/-- Executable root occupancy check matching `bptree_check_invariants_node`. -/
def checkRootOccupancy {K V : Type} (maxKeys : Nat) : BPlusTree K V → Bool
  | .leaf entries => decide (entries.length ≤ maxKeys)
  | .internal keys children =>
    decide (1 ≤ keys.length ∧ keys.length ≤ maxKeys) &&
    checkNonRootOccupancyList maxKeys children

mutual
  /-- Executable uniform leaf-depth check. -/
  def checkUniformDepth {K V : Type} : BPlusTree K V → Nat → Bool
    | .leaf _, 1 => true
    | .leaf _, _ => false
    | .internal _ children, d + 1 =>
      !children.isEmpty && checkUniformDepthList children d
    | .internal _ _, 0 => false

  /-- Helper for `checkUniformDepth` over child lists. -/
  def checkUniformDepthList {K V : Type} : List (BPlusTree K V) → Nat → Bool
    | [], _ => true
    | c :: cs, d => checkUniformDepth c d && checkUniformDepthList cs d
end

/-- Executable check for separator keys against adjacent child min/max keys. -/
def checkAdjacentSeps {V : Type} : List Nat → List (BPlusTree Nat V) → Bool
  | [], _ => true
  | k :: ks, c0 :: c1 :: cs =>
    (match maxKey c0, minKey c1 with
     | some mx, some mn => decide (mx < k) && (mn == k)
     | _, _ => false) &&
    checkAdjacentSeps ks (c1 :: cs)
  | _, _ => false

mutual
  /-- Executable separator invariant check matching `bptree_check_invariants_node`. -/
  def checkSeparators {V : Type} : BPlusTree Nat V → Bool
    | .leaf _ => true
    | .internal keys children =>
      checkSeparatorsList children && checkAdjacentSeps keys children

  /-- Helper for `checkSeparators` over child lists. -/
  def checkSeparatorsList {V : Type} : List (BPlusTree Nat V) → Bool
    | [] => true
    | c :: cs => checkSeparators c && checkSeparatorsList cs
end

/-- Executable invariant checker matching `bptree_check_invariants` in `bptree.h`. -/
def checkInvariants {V : Type} (maxKeys : Nat) (t : BPlusTree Nat V) : Bool :=
  checkCapacity maxKeys &&
  checkNodeSorted t &&
  checkChildCount t &&
  checkRootOccupancy maxKeys t &&
  checkUniformDepth t t.height &&
  checkSeparators t

/-- `checkCapacity` is equivalent to `validCapacity`. -/
theorem checkCapacity_iff {maxKeys : Nat} :
    checkCapacity maxKeys = true ↔ validCapacity maxKeys := by
  simp [checkCapacity, validCapacity]

/-- Soundness of `checkChildCount` with respect to `ChildCountValid`. -/
theorem checkChildCount_sound {K V : Type} :
    (t : BPlusTree K V) → checkChildCount t = true → ChildCountValid t
  | .leaf _, _ => by simp [ChildCountValid]
  | .internal keys children, h => by
    simp only [checkChildCount, Bool.and_eq_true, beq_iff_eq] at h
    rcases h with ⟨hlen, hlist⟩
    simp only [ChildCountValid]
    refine ⟨hlen, ?_⟩
    have hmem : ∀ c ∈ children, checkChildCount c = true → ChildCountValid c :=
      fun c _ hc => checkChildCount_sound c hc
    clear checkChildCount_sound hlen
    induction children with
    | nil => simp
    | cons c cs ih =>
      simp only [checkChildCountList, Bool.and_eq_true] at hlist
      intro x hx
      rcases List.mem_cons.mp hx with rfl | hx_cs
      · exact hmem x (by simp) hlist.1
      · exact ih hlist.2 (fun y hy hyb => hmem y (by simp [hy]) hyb) x hx_cs

/-- Soundness of `checkNonRootOccupancy` with respect to `NonRootOccupancy`. -/
theorem checkNonRootOccupancy_sound {K V : Type} (maxKeys : Nat) :
    (t : BPlusTree K V) → checkNonRootOccupancy maxKeys t = true → NonRootOccupancy maxKeys t
  | .leaf entries, h => by
    simp only [checkNonRootOccupancy, decide_eq_true_eq] at h
    simpa [NonRootOccupancy] using h
  | .internal keys children, h => by
    simp only [checkNonRootOccupancy, Bool.and_eq_true, decide_eq_true_eq] at h
    rcases h with ⟨⟨hmin, hmax⟩, hlist⟩
    simp only [NonRootOccupancy]
    refine ⟨hmin, hmax, ?_⟩
    have hmem : ∀ c ∈ children,
        checkNonRootOccupancy maxKeys c = true → NonRootOccupancy maxKeys c :=
      fun c _ hc => checkNonRootOccupancy_sound maxKeys c hc
    clear checkNonRootOccupancy_sound hmin hmax
    induction children with
    | nil => simp
    | cons c cs ih =>
      simp only [checkNonRootOccupancyList, Bool.and_eq_true] at hlist
      intro x hx
      rcases List.mem_cons.mp hx with rfl | hx_cs
      · exact hmem x (by simp) hlist.1
      · exact ih hlist.2 (fun y hy hyb => hmem y (by simp [hy]) hyb) x hx_cs

/-- Soundness of `checkRootOccupancy` with respect to `RootOccupancy`. -/
theorem checkRootOccupancy_sound {K V : Type} {maxKeys : Nat} :
    (t : BPlusTree K V) → checkRootOccupancy maxKeys t = true → RootOccupancy maxKeys t
  | .leaf entries, h => by
    simp only [checkRootOccupancy, decide_eq_true_eq] at h
    simpa [RootOccupancy] using h
  | .internal keys children, h => by
    simp only [checkRootOccupancy, Bool.and_eq_true, decide_eq_true_eq] at h
    rcases h with ⟨⟨hmin, hmax⟩, hlist⟩
    simp only [RootOccupancy]
    refine ⟨hmin, hmax, ?_⟩
    clear hmin hmax
    induction children with
    | nil => simp
    | cons c cs ih =>
      simp only [checkNonRootOccupancyList, Bool.and_eq_true] at hlist
      intro x hx
      rcases List.mem_cons.mp hx with rfl | hx_cs
      · exact checkNonRootOccupancy_sound maxKeys x hlist.1
      · exact ih hlist.2 x hx_cs

/-- Soundness of `checkUniformDepth` with respect to `UniformDepth`. -/
theorem checkUniformDepth_sound {K V : Type} :
    (t : BPlusTree K V) → (d : Nat) → checkUniformDepth t d = true → UniformDepth t d
  | .leaf _, 1, _ => by simp [UniformDepth]
  | .leaf _, 0, h => by simp [checkUniformDepth] at h
  | .leaf _, _ + 2, h => by simp [checkUniformDepth] at h
  | .internal _ _, 0, h => by simp [checkUniformDepth] at h
  | .internal _ children, d + 1, h => by
    simp only [checkUniformDepth, Bool.and_eq_true, Bool.not_eq_true',
      List.isEmpty_eq_false_iff] at h
    rcases h with ⟨hne, hlist⟩
    simp only [UniformDepth]
    refine ⟨hne, ?_⟩
    have hmem : ∀ c ∈ children, checkUniformDepth c d = true → UniformDepth c d :=
      fun c _ hc => checkUniformDepth_sound c d hc
    clear checkUniformDepth_sound hne
    induction children with
    | nil => simp
    | cons c cs ih =>
      simp only [checkUniformDepthList, Bool.and_eq_true] at hlist
      intro x hx
      rcases List.mem_cons.mp hx with rfl | hx_cs
      · exact hmem x (by simp) hlist.1
      · exact ih hlist.2 (fun y hy hyb => hmem y (by simp [hy]) hyb) x hx_cs

/-- An empty tree passes `checkInvariants` whenever `checkCapacity maxKeys = true`. -/
theorem checkInvariants_empty {maxKeys : Nat} (V : Type) (h : checkCapacity maxKeys = true) :
    checkInvariants maxKeys (empty Nat V) = true := by
  simp [checkInvariants, h, empty, checkNodeSorted, checkKeysSorted, checkChildCount,
    checkRootOccupancy, BPlusTree.height, checkUniformDepth, checkSeparators]

/-- Soundness of `checkKeysSorted` with respect to `KeysSorted`. -/
theorem checkKeysSorted_sound :
    {keys : List Nat} → checkKeysSorted keys = true → KeysSorted keys
  | [], _ => by simp [KeysSorted]
  | [_], _ => by simp [KeysSorted]
  | k1 :: k2 :: ks, h => by
    simp only [checkKeysSorted, Bool.and_eq_true, decide_eq_true_eq] at h
    have htl : KeysSorted (k2 :: ks) := checkKeysSorted_sound h.2
    simp only [KeysSorted, List.pairwise_cons] at htl ⊢
    refine ⟨?_, htl⟩
    intro x hx
    rcases List.mem_cons.mp hx with rfl | hx_ks
    · exact h.1
    · exact Nat.lt_trans h.1 (htl.1 x hx_ks)

/-- Soundness of `checkNodeSorted` with respect to `NodeSorted`. -/
theorem checkNodeSorted_sound {V : Type} :
    (t : BPlusTree Nat V) → checkNodeSorted t = true → NodeSorted t
  | .leaf entries, h => by
    simp only [checkNodeSorted] at h
    simpa [NodeSorted, EntriesSorted, KeysSorted] using checkKeysSorted_sound h
  | .internal keys children, h => by
    simp only [checkNodeSorted, Bool.and_eq_true] at h
    rcases h with ⟨hkeys, hlist⟩
    simp only [NodeSorted]
    refine ⟨checkKeysSorted_sound hkeys, ?_⟩
    have hmem : ∀ c ∈ children, checkNodeSorted c = true → NodeSorted c :=
      fun c _ hc => checkNodeSorted_sound c hc
    clear checkNodeSorted_sound hkeys
    induction children with
    | nil => simp
    | cons c cs ih =>
      simp only [checkNodeSortedList, Bool.and_eq_true] at hlist
      intro x hx
      rcases List.mem_cons.mp hx with rfl | hx_cs
      · exact hmem x (by simp) hlist.1
      · exact ih hlist.2 (fun y hy hyb => hmem y (by simp [hy]) hyb) x hx_cs

/-- Soundness of `checkAdjacentSeps` with respect to adjacent child min/max keys. -/
theorem checkAdjacentSeps_sound {V : Type} :
    {keys : List Nat} → {children : List (BPlusTree Nat V)} →
    checkAdjacentSeps keys children = true →
    ∀ (i : Nat) (hi : i < keys.length) (hl : i < children.length) (hr : i + 1 < children.length),
      (∃ m, maxKey children[i] = some m ∧ m < keys[i]) ∧
      minKey children[i + 1] = some keys[i]
  | [], _, _, _, hi, _, _ => by simp at hi
  | _ :: _, [], h, _, _, _, _ => by simp [checkAdjacentSeps] at h
  | _ :: _, [_], h, _, _, _, _ => by simp [checkAdjacentSeps] at h
  | k :: ks, c0 :: c1 :: cs, h, 0, _, _, _ => by
    simp only [checkAdjacentSeps, Bool.and_eq_true] at h
    rcases h with ⟨hpair, _⟩
    cases hmx : maxKey c0 with
    | none => simp [hmx] at hpair
    | some mx =>
      cases hmn : minKey c1 with
      | none => simp [hmx, hmn] at hpair
      | some mn =>
        simp only [hmx, hmn, Bool.and_eq_true, decide_eq_true_eq, beq_iff_eq] at hpair
        rcases hpair with ⟨hlt, heq⟩
        subst heq
        exact ⟨⟨mx, hmx, hlt⟩, hmn⟩
  | k :: ks, c0 :: c1 :: cs, h, i + 1, hi, hl, hr => by
    simp only [checkAdjacentSeps, Bool.and_eq_true] at h
    simp only [List.length_cons] at hi hl hr
    have hrec := checkAdjacentSeps_sound h.2 i (by omega) (by simp; omega) (by simp; omega)
    simpa [List.getElem_cons_succ] using hrec

/-- Soundness of `checkSeparators` with respect to `SeparatorsValid`. -/
theorem checkSeparators_sound {V : Type} :
    (t : BPlusTree Nat V) → checkSeparators t = true → SeparatorsValid t
  | .leaf _, _ => by simp [SeparatorsValid]
  | .internal keys children, h => by
    simp only [checkSeparators, Bool.and_eq_true] at h
    rcases h with ⟨hlist, hadj⟩
    simp only [SeparatorsValid]
    refine ⟨?_, checkAdjacentSeps_sound hadj⟩
    have hmem : ∀ c ∈ children, checkSeparators c = true → SeparatorsValid c :=
      fun c _ hc => checkSeparators_sound c hc
    clear checkSeparators_sound hadj
    induction children with
    | nil => simp
    | cons c cs ih =>
      simp only [checkSeparatorsList, Bool.and_eq_true] at hlist
      intro x hx
      rcases List.mem_cons.mp hx with rfl | hx_cs
      · exact hmem x (by simp) hlist.1
      · exact ih hlist.2 (fun y hy hyb => hmem y (by simp [hy]) hyb) x hx_cs

/-- Master soundness theorem: passing `checkInvariants` implies full `Valid` invariants. -/
theorem checkInvariants_sound {V : Type} {maxKeys : Nat} {t : BPlusTree Nat V}
    (h : checkInvariants maxKeys t = true) : Valid maxKeys t := by
  simp only [checkInvariants, Bool.and_eq_true] at h
  rcases h with ⟨⟨⟨⟨⟨hcap, hsort⟩, hch⟩, hocc⟩, hdepth⟩, hsep⟩
  exact ⟨checkCapacity_iff.mp hcap, checkNodeSorted_sound t hsort, checkChildCount_sound t hch,
    checkRootOccupancy_sound t hocc, checkUniformDepth_sound t t.height hdepth,
    checkSeparators_sound t hsep⟩

/-- Every child subtree in an internal node contains only keys strictly bounded by the parent's
    separator keys: all keys in `children[i]` are `< keys[j]` for `i ≤ j` and `≥ keys[j]` for
    `j < i`. -/
def SubtreesOrdered {V : Type} : BPlusTree Nat V → Prop
  | .leaf _ => True
  | .internal keys children =>
    (∀ c ∈ children, SubtreesOrdered c) ∧
    (∀ (i j : Nat) (hi : i < children.length) (hj : j < keys.length) (p : Nat × V),
      p ∈ children[i].toList →
      (i ≤ j → p.1 < keys[j]) ∧ (j < i → keys[j] ≤ p.1))

/-- An empty tree satisfies `SubtreesOrdered`. -/
theorem subtreesOrdered_empty (V : Type) : SubtreesOrdered (empty Nat V) := by
  simp [empty, SubtreesOrdered]

end Bptree

end
