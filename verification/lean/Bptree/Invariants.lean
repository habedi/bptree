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

end Bptree

end
