module

public import Mathlib.Data.Nat.Basic
public import Mathlib.Tactic

@[expose] public section

namespace Bptree

/-- Minimum number of keys in a non-root leaf node, matching `bptree_create`. -/
def minLeafKeys (maxKeys : Nat) : Nat :=
  (maxKeys + 1) / 2

/-- Minimum number of keys in a non-root internal node, matching `bptree_create`. -/
def minInternalKeys (maxKeys : Nat) : Nat :=
  (maxKeys + 2) / 2 - 1

/-- Supported node capacity range, matching `bptree_create`. -/
def validCapacity (maxKeys : Nat) : Prop :=
  3 ≤ maxKeys ∧ maxKeys ≤ 4096

/-- Status codes matching `bptree_status` in `bptree.h`. -/
inductive Status where
  | ok
  | duplicateKey
  | keyNotFound
  | allocationFailure
  | invalidArgument
  | internalError
  deriving DecidableEq, Repr

/-- Pure functional representation of a B+ tree. -/
inductive BPlusTree (K V : Type) where
  | leaf : List (K × V) → BPlusTree K V
  | internal : List K → List (BPlusTree K V) → BPlusTree K V
  deriving Repr

mutual
  /-- Structural equality check for `BPlusTree`. -/
  def BPlusTree.beq {K V : Type} [BEq K] [BEq V] : BPlusTree K V → BPlusTree K V → Bool
    | .leaf a, .leaf b => a == b
    | .internal ka ca, .internal kb cb => ka == kb && BPlusTree.beqList ca cb
    | _, _ => false

  /-- Structural equality check for lists of `BPlusTree`. -/
  def BPlusTree.beqList {K V : Type} [BEq K] [BEq V] :
      List (BPlusTree K V) → List (BPlusTree K V) → Bool
    | [], [] => true
    | a :: as, b :: bs => BPlusTree.beq a b && BPlusTree.beqList as bs
    | _, _ => false
end

instance {K V : Type} [BEq K] [BEq V] : BEq (BPlusTree K V) where
  beq := BPlusTree.beq

/-- Construct an empty B+ tree (a single empty leaf). -/
def empty (K V : Type) : BPlusTree K V :=
  .leaf []

/-- Clear all entries from a B+ tree, matching `bptree_clear`. -/
def clear (K V : Type) (_ : BPlusTree K V) : BPlusTree K V :=
  empty K V

/-- Return `true` if the root node is a leaf. -/
def BPlusTree.isLeaf {K V : Type} : BPlusTree K V → Bool
  | .leaf _ => true
  | .internal _ _ => false

/-- Number of keys stored directly in the root node. -/
def BPlusTree.numKeys {K V : Type} : BPlusTree K V → Nat
  | .leaf entries => entries.length
  | .internal keys _ => keys.length

/-- List of keys stored directly in the root node. -/
def BPlusTree.nodeKeys {K V : Type} : BPlusTree K V → List K
  | .leaf entries => entries.map Prod.fst
  | .internal keys _ => keys

/-- Height of the tree. An empty tree (single leaf) has height 1. -/
def BPlusTree.height {K V : Type} : BPlusTree K V → Nat
  | .leaf _ => 1
  | .internal _ children =>
    match children with
    | [] => 1
    | c :: _ => 1 + c.height

/-- All leaf entry lists from left to right. -/
def BPlusTree.leaves {K V : Type} : BPlusTree K V → List (List (K × V))
  | .leaf entries => [entries]
  | .internal _ children => children.flatMap BPlusTree.leaves

/-- All key-value pairs in left-to-right leaf order. -/
def BPlusTree.toList {K V : Type} (t : BPlusTree K V) : List (K × V) :=
  t.leaves.flatten

/-- Total number of key-value pairs stored in the tree. -/
def BPlusTree.count {K V : Type} : BPlusTree K V → Nat
  | .leaf entries => entries.length
  | .internal _ children => (children.map BPlusTree.count).sum

/-- Total number of nodes (internal and leaf) in the tree. -/
def BPlusTree.nodeCount {K V : Type} : BPlusTree K V → Nat
  | .leaf _ => 1
  | .internal _ children => 1 + (children.map BPlusTree.nodeCount).sum

mutual
  /-- Serialize a B+ tree into a canonical S-expression string for differential testing against C. -/
  def BPlusTree.serialize : BPlusTree Nat Nat → String
    | .leaf entries =>
      let items := entries.map (fun p => s!"{p.1}:{p.2}")
      s!"(leaf [{String.intercalate "," items}])"
    | .internal keys children =>
      let kstr := String.intercalate "," (keys.map toString)
      let cstr := String.intercalate "," (BPlusTree.serializeList children)
      s!"(internal [{kstr}] [{cstr}])"

  /-- Helper to serialize a list of child subtrees. -/
  def BPlusTree.serializeList : List (BPlusTree Nat Nat) → List String
    | [] => []
    | c :: cs => BPlusTree.serialize c :: BPlusTree.serializeList cs
end

/-- Minimum leaf keys is at least 1 when `3 ≤ maxKeys`. -/
theorem minLeafKeys_pos {maxKeys : Nat} (h : 3 ≤ maxKeys) : 1 ≤ minLeafKeys maxKeys := by
  unfold minLeafKeys
  omega

/-- Minimum internal keys is at least 1 when `3 ≤ maxKeys`. -/
theorem minInternalKeys_pos {maxKeys : Nat} (h : 3 ≤ maxKeys) : 1 ≤ minInternalKeys maxKeys := by
  unfold minInternalKeys
  omega

/-- Minimum leaf keys never exceeds `maxKeys` when `3 ≤ maxKeys`. -/
theorem minLeafKeys_le_maxKeys {maxKeys : Nat} (h : 3 ≤ maxKeys) :
    minLeafKeys maxKeys ≤ maxKeys := by
  unfold minLeafKeys
  omega

/-- Minimum internal keys never exceeds `maxKeys` when `3 ≤ maxKeys`. -/
theorem minInternalKeys_le_maxKeys {maxKeys : Nat} (h : 3 ≤ maxKeys) :
    minInternalKeys maxKeys ≤ maxKeys := by
  unfold minInternalKeys
  omega

/-- An empty tree has count 0. -/
theorem count_empty (K V : Type) : (empty K V).count = 0 := by
  simp [empty, BPlusTree.count]

/-- An empty tree has height 1. -/
theorem height_empty (K V : Type) : (empty K V).height = 1 := by
  simp [empty, BPlusTree.height]

/-- Clearing any tree yields the empty tree. -/
theorem clear_eq_empty {K V : Type} (t : BPlusTree K V) : clear K V t = empty K V := by
  rfl

/-- A cleared tree has count 0. -/
theorem clear_count {K V : Type} (t : BPlusTree K V) : (clear K V t).count = 0 := by
  simp [clear, count_empty]

/-- A cleared tree has height 1. -/
theorem clear_height {K V : Type} (t : BPlusTree K V) : (clear K V t).height = 1 := by
  simp [clear, height_empty]

/-- `t.count` equals the length of `t.toList`. -/
theorem count_eq_toList_length {K V : Type} : (t : BPlusTree K V) →
    t.count = t.toList.length
  | .leaf entries => by
    simp [BPlusTree.count, BPlusTree.toList, BPlusTree.leaves]
  | .internal _ children => by
    simp only [BPlusTree.count, BPlusTree.toList, BPlusTree.leaves, List.length_flatten,
      List.map_flatMap]
    have hchildren : ∀ c ∈ children, c.count = (c.leaves).flatten.length :=
      fun c _ => count_eq_toList_length c
    clear count_eq_toList_length
    induction children with
    | nil => simp
    | cons c cs ih =>
      simp only [List.map_cons, List.flatMap_cons, List.sum_cons, List.sum_append]
      have hc : c.count = c.leaves.flatten.length := hchildren c (by simp)
      have hcs := ih (fun x hx => hchildren x (by simp [hx]))
      simp only [List.length_flatten] at hc hcs
      omega

/-- The `toList` of an internal node is the concatenation of its children's `toList`s. -/
theorem toList_internal_eq_flatMap {K V : Type} (keys : List K)
    (children : List (BPlusTree K V)) :
    (BPlusTree.internal keys children).toList = children.flatMap BPlusTree.toList := by
  simp only [BPlusTree.toList, BPlusTree.leaves]
  induction children with
  | nil => simp
  | cons c cs ih =>
    simp only [List.flatMap_cons, List.flatten_append, ih]
    rfl

end Bptree

end
