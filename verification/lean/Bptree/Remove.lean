module

public import Bptree.Search

@[expose] public section

namespace Bptree

/-- Remove `k` from a leaf entry list. Returns `none` if `k` is not present,
    or `some (entries', wasFirst)` where `wasFirst` is `true` iff `k` was at index 0. -/
def removeEntries {V : Type} (entries : List (Nat × V)) (k : Nat) :
    Option (List (Nat × V) × Bool) :=
  match entries with
  | [] => none
  | (k', v') :: rest =>
    if k == k' then
      some (rest, true)
    else
      match removeEntries rest k with
      | none => none
      | some (rest', _) => some ((k', v') :: rest', false)

/-- Remove the element at index `i` from a list. -/
def eraseListIdx {α : Type} (xs : List α) (i : Nat) : List α :=
  xs.take i ++ xs.drop (i + 1)

/-- Return the last element of a list paired with its prefix, if non-empty. -/
def splitLast? {α : Type} : List α → Option (List α × α)
  | [] => none
  | [x] => some ([], x)
  | x :: xs =>
    match splitLast? xs with
    | none => none
    | some (init, last) => some (x :: init, last)

/-- Rebalance an underflowing child at `childIdx` inside an internal node,
    matching `bptree_rebalance_up` in `bptree.h` (borrow left, borrow right,
    merge left, merge right). -/
def rebalanceChild {V : Type} (maxKeys : Nat) (parentKeys : List Nat)
    (children : List (BPlusTree Nat V)) (childIdx : Nat) :
    List Nat × List (BPlusTree Nat V) :=
  match children[childIdx]? with
  | none => (parentKeys, children)
  | some child =>
    let minKeys := if child.isLeaf then minLeafKeys maxKeys else minInternalKeys maxKeys
    if child.numKeys ≥ minKeys then
      (parentKeys, children)
    else
      -- Try borrowing from the left sibling.
      let borrowedLeft : Option (List Nat × List (BPlusTree Nat V)) :=
        if childIdx > 0 then
          match children[childIdx - 1]? with
          | some leftSib =>
            let leftMin := if leftSib.isLeaf then minLeafKeys maxKeys else minInternalKeys maxKeys
            if leftSib.numKeys > leftMin then
              match leftSib, child with
              | .leaf leftEntries, .leaf childEntries =>
                match splitLast? leftEntries with
                | some (leftInit, lastEntry) =>
                  let leftSib' := .leaf leftInit
                  let child' := .leaf (lastEntry :: childEntries)
                  let parentKeys' := parentKeys.set (childIdx - 1) lastEntry.1
                  let children' := (children.set (childIdx - 1) leftSib').set childIdx child'
                  some (parentKeys', children')
                | none => none
              | .internal leftKeys leftChildren, .internal childKeys childChildren =>
                match splitLast? leftKeys, splitLast? leftChildren, parentKeys[childIdx - 1]? with
                | some (lkInit, lastKey), some (lcInit, lastChild), some sep =>
                  let leftSib' := .internal lkInit lcInit
                  let child' := .internal (sep :: childKeys) (lastChild :: childChildren)
                  let parentKeys' := parentKeys.set (childIdx - 1) lastKey
                  let children' := (children.set (childIdx - 1) leftSib').set childIdx child'
                  some (parentKeys', children')
                | _, _, _ => none
              | _, _ => none
            else
              none
          | none => none
        else
          none
      match borrowedLeft with
      | some res => res
      | none =>
        -- Try borrowing from the right sibling.
        let borrowedRight : Option (List Nat × List (BPlusTree Nat V)) :=
          if childIdx < parentKeys.length then
            match children[childIdx + 1]? with
            | some rightSib =>
              let rightMin :=
                if rightSib.isLeaf then minLeafKeys maxKeys else minInternalKeys maxKeys
              if rightSib.numKeys > rightMin then
                match child, rightSib with
                | .leaf childEntries, .leaf (r0 :: r1 :: rs) =>
                  let child' := .leaf (childEntries ++ [r0])
                  let rightSib' := .leaf (r1 :: rs)
                  let parentKeys' := parentKeys.set childIdx r1.1
                  let children' := (children.set childIdx child').set (childIdx + 1) rightSib'
                  some (parentKeys', children')
                | .internal childKeys childChildren, .internal (rk0 :: rks) (rc0 :: rcs) =>
                  match parentKeys[childIdx]? with
                  | some sep =>
                    let child' := .internal (childKeys ++ [sep]) (childChildren ++ [rc0])
                    let rightSib' := .internal rks rcs
                    let parentKeys' := parentKeys.set childIdx rk0
                    let children' := (children.set childIdx child').set (childIdx + 1) rightSib'
                    some (parentKeys', children')
                  | none => none
                | _, _ => none
              else
                none
            | none => none
          else
            none
        match borrowedRight with
        | some res => res
        | none =>
          -- Merge with left sibling if available, otherwise merge with right sibling.
          if childIdx > 0 then
            match children[childIdx - 1]? with
            | some leftSib =>
              match leftSib, child with
              | .leaf leftEntries, .leaf childEntries =>
                let merged := .leaf (leftEntries ++ childEntries)
                let parentKeys' := eraseListIdx parentKeys (childIdx - 1)
                let children' := eraseListIdx (children.set (childIdx - 1) merged) childIdx
                (parentKeys', children')
              | .internal leftKeys leftChildren, .internal childKeys childChildren =>
                match parentKeys[childIdx - 1]? with
                | some sep =>
                  let merged := .internal (leftKeys ++ [sep] ++ childKeys)
                    (leftChildren ++ childChildren)
                  let parentKeys' := eraseListIdx parentKeys (childIdx - 1)
                  let children' := eraseListIdx (children.set (childIdx - 1) merged) childIdx
                  (parentKeys', children')
                | none => (parentKeys, children)
              | _, _ => (parentKeys, children)
            | none => (parentKeys, children)
          else
            match children[childIdx + 1]? with
            | some rightSib =>
              match child, rightSib with
              | .leaf childEntries, .leaf rightEntries =>
                let merged := .leaf (childEntries ++ rightEntries)
                let parentKeys' := eraseListIdx parentKeys childIdx
                let children' := eraseListIdx (children.set childIdx merged) (childIdx + 1)
                (parentKeys', children')
              | .internal childKeys childChildren, .internal rightKeys rightChildren =>
                match parentKeys[childIdx]? with
                | some sep =>
                  let merged := .internal (childKeys ++ [sep] ++ rightKeys)
                    (childChildren ++ rightChildren)
                  let parentKeys' := eraseListIdx parentKeys childIdx
                  let children' := eraseListIdx (children.set childIdx merged) (childIdx + 1)
                  (parentKeys', children')
                | none => (parentKeys, children)
              | _, _ => (parentKeys, children)
            | none => (parentKeys, children)

/-- Recursive deletion helper returning `(updatedSubtree, pendingNewMin, needsRebalance)`. -/
def removeNode {V : Type} (maxKeys : Nat) (t : BPlusTree Nat V) (k : Nat) :
    Option (BPlusTree Nat V × Option Nat × Bool) :=
  match t with
  | .leaf entries =>
    match removeEntries entries k with
    | none => none
    | some (entries', wasFirst) =>
      let pendingMin := if wasFirst then entries'.head?.map Prod.fst else none
      some (.leaf entries', pendingMin, true)
  | .internal keys children =>
    let pos := findChildIdx keys k
    match _hc : children[pos]? with
    | none => none
    | some child =>
      match removeNode maxKeys child k with
      | none => none
      | some (child', pendingMin, doRebalance) =>
        let (keys1, pendingMin') :=
          match pendingMin with
          | none => (keys, none)
          | some newMin =>
            if pos > 0 then
              if keys[pos - 1]? == some k then
                (keys.set (pos - 1) newMin, none)
              else
                (keys, some newMin)
            else
              (keys, some newMin)
        let children1 := children.set pos child'
        if doRebalance then
          let minKeys := if child'.isLeaf then minLeafKeys maxKeys else minInternalKeys maxKeys
          if child'.numKeys ≥ minKeys then
            some (.internal keys1 children1, pendingMin', false)
          else
            let (keys2, children2) := rebalanceChild maxKeys keys1 children1 pos
            some (.internal keys2 children2, pendingMin', true)
        else
          some (.internal keys1 children1, pendingMin', false)
  termination_by sizeOf t
  decreasing_by
    simp_wf
    have hmem : child ∈ children := List.mem_of_getElem? _hc
    have hsz := List.sizeOf_lt_of_mem hmem
    omega

/-- Top-level deletion matching `bptree_remove`. -/
def remove {V : Type} (maxKeys : Nat) (t : BPlusTree Nat V) (k : Nat) :
    Except Status (BPlusTree Nat V) :=
  match t with
  | .leaf [] => .error .keyNotFound
  | _ =>
    match removeNode maxKeys t k with
    | none => .error .keyNotFound
    | some (.internal [] [c], _, _) => .ok c
    | some (t', _, _) => .ok t'

/-- Removing any key from an empty tree returns `Status.keyNotFound`. -/
theorem remove_empty (maxKeys : Nat) (V : Type) (k : Nat) :
    remove maxKeys (empty Nat V) k = .error .keyNotFound := by
  simp [empty, remove]

/-- If `leafLookup entries k = none`, then `removeEntries entries k = none`. -/
theorem removeEntries_not_found {V : Type} {entries : List (Nat × V)} {k : Nat}
    (h : leafLookup entries k = none) : removeEntries entries k = none := by
  induction entries with
  | nil => simp [removeEntries]
  | cons hd tl ih =>
    rcases hd with ⟨k', v'⟩
    simp only [leafLookup] at h
    by_cases hkk : k == k'
    · simp [hkk] at h
    · simp only [hkk, Bool.false_eq_true, ↓reduceIte] at h
      simp only [removeEntries, hkk, Bool.false_eq_true, ↓reduceIte, ih h]

/-- Removing an existing key decreases `entries.length` by 1. -/
theorem removeEntries_length {V : Type} {entries entries' : List (Nat × V)} {k : Nat} {b : Bool}
    (h : removeEntries entries k = some (entries', b)) :
    entries'.length + 1 = entries.length := by
  induction entries generalizing entries' b with
  | nil => simp [removeEntries] at h
  | cons hd tl ih =>
    rcases hd with ⟨k', v'⟩
    simp only [removeEntries] at h
    by_cases hkk : k == k'
    · simp only [hkk, ↓reduceIte, Option.some.injEq, Prod.mk.injEq] at h
      rcases h with ⟨rfl, _⟩
      simp
    · simp only [hkk, Bool.false_eq_true, ↓reduceIte] at h
      split at h
      · simp at h
      · rename_i rest' b' hrec
        simp only [Option.some.injEq, Prod.mk.injEq] at h
        rcases h with ⟨rfl, _⟩
        have hlen := ih hrec
        simp only [List.length_cons]
        omega

private theorem mem_of_mem_removeEntries {V : Type} {entries entries' : List (Nat × V)}
    {k : Nat} {b : Bool} (h : removeEntries entries k = some (entries', b)) (p : Nat × V) :
    p ∈ entries' → p ∈ entries := by
  induction entries generalizing entries' b with
  | nil => simp [removeEntries] at h
  | cons hd tl ih =>
    rcases hd with ⟨k', v'⟩
    simp only [removeEntries] at h
    by_cases hkk : k == k'
    · simp only [hkk, ↓reduceIte, Option.some.injEq, Prod.mk.injEq] at h
      rcases h with ⟨rfl, _⟩
      intro hp
      exact List.mem_cons_of_mem (k', v') hp
    · simp only [hkk, Bool.false_eq_true, ↓reduceIte] at h
      split at h
      · simp at h
      · rename_i rest' b' hrec
        simp only [Option.some.injEq, Prod.mk.injEq] at h
        rcases h with ⟨rfl, _⟩
        intro hp
        rcases List.mem_cons.mp hp with rfl | hp_tl
        · exact List.mem_cons_self
        · exact List.mem_cons_of_mem (k', v') (ih hrec hp_tl)

/-- `removeEntries` preserves `EntriesSorted`. -/
theorem removeEntries_preserves_sorted {V : Type} {entries entries' : List (Nat × V)}
    {k : Nat} {b : Bool} (hsort : EntriesSorted entries)
    (h : removeEntries entries k = some (entries', b)) :
    EntriesSorted entries' := by
  induction entries generalizing entries' b with
  | nil => simp [removeEntries] at h
  | cons hd tl ih =>
    rcases hd with ⟨k', v'⟩
    simp only [EntriesSorted, List.map_cons, List.pairwise_cons] at hsort
    simp only [removeEntries] at h
    by_cases hkk : k == k'
    · simp only [hkk, ↓reduceIte, Option.some.injEq, Prod.mk.injEq] at h
      rcases h with ⟨rfl, _⟩
      exact hsort.2
    · simp only [hkk, Bool.false_eq_true, ↓reduceIte] at h
      split at h
      · simp at h
      · rename_i rest' b' hrec
        simp only [Option.some.injEq, Prod.mk.injEq] at h
        rcases h with ⟨rfl, _⟩
        simp only [EntriesSorted, List.map_cons, List.pairwise_cons]
        refine ⟨?_, ih hsort.2 hrec⟩
        intro x hx
        rcases List.mem_map.mp hx with ⟨⟨xk, vx⟩, hmem, rfl⟩
        exact hsort.1 xk (List.mem_map.mpr ⟨(xk, vx), mem_of_mem_removeEntries hrec (xk, vx) hmem, rfl⟩)

/-- After removing `k` from a sorted leaf, `leafLookup entries' k = none`. -/
theorem leafLookup_removeEntries_self {V : Type} {entries entries' : List (Nat × V)}
    {k : Nat} {b : Bool} (hsort : EntriesSorted entries)
    (h : removeEntries entries k = some (entries', b)) :
    leafLookup entries' k = none := by
  induction entries generalizing entries' b with
  | nil => simp [removeEntries] at h
  | cons hd tl ih =>
    rcases hd with ⟨k', v'⟩
    simp only [EntriesSorted, List.map_cons, List.pairwise_cons] at hsort
    simp only [removeEntries] at h
    by_cases hkk : k == k'
    · simp only [hkk, ↓reduceIte, Option.some.injEq, Prod.mk.injEq] at h
      rcases h with ⟨rfl, _⟩
      have hkeq : k = k' := beq_iff_eq.mp hkk
      subst hkeq
      by_cases hnone : leafLookup tl k = none
      · exact hnone
      · obtain ⟨v, hv⟩ := Option.ne_none_iff_exists'.mp hnone
        have hmem := leafLookup_mem hv
        have hlt := hsort.1 k (List.mem_map.mpr ⟨(k, v), hmem, rfl⟩)
        omega
    · simp only [hkk, Bool.false_eq_true, ↓reduceIte] at h
      split at h
      · simp at h
      · rename_i rest' b' hrec
        simp only [Option.some.injEq, Prod.mk.injEq] at h
        rcases h with ⟨rfl, _⟩
        simp only [leafLookup, hkk, Bool.false_eq_true, ↓reduceIte]
        exact ih hsort.2 hrec

/-- Merging an underflowing leaf (`minLeafKeys - 1`) with a minimum-occupancy leaf
    (`minLeafKeys`) always fits within `[minLeafKeys, maxKeys]` (no buffer overflow). -/
theorem mergeLeaf_bounds {V : Type} {maxKeys : Nat} {left right : List (Nat × V)}
    (hcap : 3 ≤ maxKeys)
    (hl : left.length = minLeafKeys maxKeys - 1)
    (hr : right.length = minLeafKeys maxKeys) :
    minLeafKeys maxKeys ≤ (left ++ right).length ∧ (left ++ right).length ≤ maxKeys := by
  simp only [List.length_append, hl, hr, minLeafKeys]
  omega

/-- Merging an underflowing internal node (`minInternalKeys - 1`) with a minimum-occupancy
    internal node (`minInternalKeys`) and one pulled-down separator key always fits within
    `[minInternalKeys, maxKeys]` (no buffer overflow). -/
theorem mergeInternal_bounds {maxKeys : Nat} {leftKeys rightKeys : List Nat}
    (hcap : 3 ≤ maxKeys)
    (hl : leftKeys.length = minInternalKeys maxKeys - 1)
    (hr : rightKeys.length = minInternalKeys maxKeys) :
    minInternalKeys maxKeys ≤ leftKeys.length + 1 + rightKeys.length ∧
    leftKeys.length + 1 + rightKeys.length ≤ maxKeys := by
  simp only [hl, hr, minInternalKeys]
  omega

/-- Removing `k` from `entries` does not alter lookups for any other key `k' ≠ k`. -/
theorem leafLookup_removeEntries_ne {V : Type} {entries entries' : List (Nat × V)}
    {k k' : Nat} {b : Bool} (hne : k' ≠ k)
    (h : removeEntries entries k = some (entries', b)) :
    leafLookup entries' k' = leafLookup entries k' := by
  induction entries generalizing entries' b with
  | nil => simp [removeEntries] at h
  | cons hd tl ih =>
    rcases hd with ⟨kh, vh⟩
    simp only [removeEntries] at h
    by_cases hkk : k == kh
    · simp only [hkk, ↓reduceIte, Option.some.injEq, Prod.mk.injEq] at h
      rcases h with ⟨rfl, _⟩
      have hkeq : k = kh := beq_iff_eq.mp hkk
      subst hkeq
      have hbeq : (k' == k) = false := beq_false_of_ne hne
      simp [leafLookup, hbeq]
    · simp only [hkk, Bool.false_eq_true, ↓reduceIte] at h
      split at h
      · simp at h
      · rename_i rest' b' hrec
        simp only [Option.some.injEq, Prod.mk.injEq] at h
        rcases h with ⟨rfl, _⟩
        simp only [leafLookup]
        by_cases hkh : k' == kh
        · simp [hkh]
        · simp only [hkh, Bool.false_eq_true, ↓reduceIte]
          exact ih hrec

/-- Splitting the last element of a list preserves the original list on append. -/
theorem splitLast?_append_eq {α : Type} {xs init : List α} {last : α}
    (h : splitLast? xs = some (init, last)) : init ++ [last] = xs := by
  induction xs generalizing init with
  | nil => simp [splitLast?] at h
  | cons x xs ih =>
    cases xs with
    | nil =>
      simp only [splitLast?, Option.some.injEq, Prod.mk.injEq] at h
      rcases h with ⟨rfl, rfl⟩
      rfl
    | cons y ys =>
      simp only [splitLast?] at h
      split at h
      · simp at h
      · rename_i init' last' hrec
        simp only [Option.some.injEq, Prod.mk.injEq] at h
        rcases h with ⟨rfl, rfl⟩
        simp [ih hrec]

/-- Splitting the last element decreases list length by 1. -/
theorem splitLast?_length {α : Type} {xs init : List α} {last : α}
    (h : splitLast? xs = some (init, last)) : init.length + 1 = xs.length := by
  have happ := splitLast?_append_eq h
  have hlen := congrArg List.length happ
  simp only [List.length_append, List.length_singleton] at hlen
  exact hlen

/-- Borrowing the last entry from a left leaf preserves the concatenated entry sequence. -/
theorem borrowLeft_leaf_append {V : Type} {left leftInit right : List (Nat × V)}
    {lastEntry : Nat × V} (h : splitLast? left = some (leftInit, lastEntry)) :
    leftInit ++ (lastEntry :: right) = left ++ right := by
  have happ := splitLast?_append_eq h
  rw [← happ, List.append_assoc]
  rfl

/-- Borrowing the first entry from a right leaf preserves the concatenated entry sequence. -/
theorem borrowRight_leaf_append {V : Type} (left : List (Nat × V)) (r0 : Nat × V)
    (rRest : List (Nat × V)) :
    (left ++ [r0]) ++ rRest = left ++ (r0 :: rRest) := by
  simp

/-- Borrowing from a left leaf with `> minLeafKeys` keys restores occupancy to
    `[minLeafKeys, maxKeys]` for both leaves. -/
theorem borrowLeft_leaf_bounds {V : Type} {maxKeys : Nat} {left leftInit child : List (Nat × V)}
    {lastEntry : Nat × V} (hcap : 3 ≤ maxKeys)
    (hleft_lo : minLeafKeys maxKeys < left.length) (hleft_hi : left.length ≤ maxKeys)
    (hchild : child.length = minLeafKeys maxKeys - 1)
    (hsplit : splitLast? left = some (leftInit, lastEntry)) :
    (minLeafKeys maxKeys ≤ leftInit.length ∧ leftInit.length ≤ maxKeys) ∧
    (minLeafKeys maxKeys ≤ (lastEntry :: child).length ∧
      (lastEntry :: child).length ≤ maxKeys) := by
  have hlen := splitLast?_length hsplit
  have hmin := minLeafKeys_pos hcap
  have hle := minLeafKeys_le_maxKeys hcap
  simp only [List.length_cons]
  omega

/-- Borrowing from a right leaf with `> minLeafKeys` keys restores occupancy to
    `[minLeafKeys, maxKeys]` for both leaves. -/
theorem borrowRight_leaf_bounds {V : Type} {maxKeys : Nat} {child rRest : List (Nat × V)}
    {r0 : Nat × V} (hcap : 3 ≤ maxKeys)
    (hright_lo : minLeafKeys maxKeys < (r0 :: rRest).length)
    (hright_hi : (r0 :: rRest).length ≤ maxKeys)
    (hchild : child.length = minLeafKeys maxKeys - 1) :
    (minLeafKeys maxKeys ≤ (child ++ [r0]).length ∧ (child ++ [r0]).length ≤ maxKeys) ∧
    (minLeafKeys maxKeys ≤ rRest.length ∧ rRest.length ≤ maxKeys) := by
  have hmin := minLeafKeys_pos hcap
  have hle := minLeafKeys_le_maxKeys hcap
  simp only [List.length_append, List.length_cons, List.length_nil] at hright_lo hright_hi ⊢
  omega

/-- Borrowing from a left internal sibling with `> minInternalKeys` keys restores occupancy to
    `[minInternalKeys, maxKeys]` for both internal nodes. -/
theorem borrowLeft_internal_bounds {maxKeys : Nat} {leftKeys leftInit childKeys : List Nat}
    {lastKey sep : Nat} (hcap : 3 ≤ maxKeys)
    (hleft_lo : minInternalKeys maxKeys < leftKeys.length)
    (hleft_hi : leftKeys.length ≤ maxKeys)
    (hchild : childKeys.length = minInternalKeys maxKeys - 1)
    (hsplit : splitLast? leftKeys = some (leftInit, lastKey)) :
    (minInternalKeys maxKeys ≤ leftInit.length ∧ leftInit.length ≤ maxKeys) ∧
    (minInternalKeys maxKeys ≤ (sep :: childKeys).length ∧
      (sep :: childKeys).length ≤ maxKeys) := by
  have hlen := splitLast?_length hsplit
  have hmin := minInternalKeys_pos hcap
  have hle := minInternalKeys_le_maxKeys hcap
  simp only [List.length_cons]
  omega

/-- Borrowing from a right internal sibling with `> minInternalKeys` keys restores occupancy to
    `[minInternalKeys, maxKeys]` for both internal nodes. -/
theorem borrowRight_internal_bounds {maxKeys : Nat} {childKeys rRest : List Nat}
    {rk0 sep : Nat} (hcap : 3 ≤ maxKeys)
    (hright_lo : minInternalKeys maxKeys < (rk0 :: rRest).length)
    (hright_hi : (rk0 :: rRest).length ≤ maxKeys)
    (hchild : childKeys.length = minInternalKeys maxKeys - 1) :
    (minInternalKeys maxKeys ≤ (childKeys ++ [sep]).length ∧
      (childKeys ++ [sep]).length ≤ maxKeys) ∧
    (minInternalKeys maxKeys ≤ rRest.length ∧ rRest.length ≤ maxKeys) := by
  have hmin := minInternalKeys_pos hcap
  have hle := minInternalKeys_le_maxKeys hcap
  simp only [List.length_append, List.length_cons, List.length_nil] at hright_lo hright_hi ⊢
  omega

/-- Merging two internal nodes with a pulled-down separator key preserves
    `children.length = keys.length + 1`. -/
theorem mergeInternal_children_length {K V : Type} {leftKeys rightKeys : List K}
    {leftChildren rightChildren : List (BPlusTree K V)} (sep : K)
    (hl : leftChildren.length = leftKeys.length + 1)
    (hr : rightChildren.length = rightKeys.length + 1) :
    (leftChildren ++ rightChildren).length = (leftKeys ++ [sep] ++ rightKeys).length + 1 := by
  simp only [List.length_append, List.length_cons, List.length_nil, hl, hr]
  omega

/-- Full-tree missing-key rejection for `removeNode`: if `lookup t k = none`,
    `removeNode` returns `none`. -/
theorem removeNode_not_found_of_lookup {V : Type} (maxKeys : Nat) :
    (t : BPlusTree Nat V) → {k : Nat} →
    lookup t k = none → removeNode maxKeys t k = none
  | .leaf entries, k, hlook => by
    simp only [lookup] at hlook
    simp only [removeNode, removeEntries_not_found hlook]
  | .internal keys children, k, hlook => by
    simp only [lookup] at hlook
    simp only [removeNode]
    split at hlook
    · rename_i child hc
      split
      · rfl
      · rename_i child' hc'
        rw [hc] at hc'
        injection hc' with hceq
        have hmem : child ∈ children := List.mem_of_getElem? hc
        have ih := removeNode_not_found_of_lookup maxKeys child hlook
        rw [← hceq]
        simp [ih]
    · rename_i hnone
      split
      · rfl
      · rename_i child' hc'
        rw [hnone] at hc'
        simp at hc'
  termination_by t => sizeOf t
  decreasing_by
    simp_wf
    have hsz := List.sizeOf_lt_of_mem hmem
    omega

/-- Full-tree missing-key rejection for `remove`: if `lookup t k = none`,
    `remove` returns `Status.keyNotFound`. -/
theorem remove_not_found_of_lookup {V : Type} (maxKeys : Nat) {t : BPlusTree Nat V}
    {k : Nat} (hlook : lookup t k = none) :
    remove maxKeys t k = .error .keyNotFound := by
  cases t with
  | leaf entries =>
    cases entries with
    | nil => simp [remove]
    | cons hd tl =>
      simp only [remove, removeNode_not_found_of_lookup maxKeys _ hlook]
  | internal keys children =>
    simp only [remove, removeNode_not_found_of_lookup maxKeys _ hlook]

/-- Merging two sorted leaves where all left keys are strictly less than all right keys
    preserves `EntriesSorted`. -/
theorem mergeLeaf_preserves_sorted {V : Type} {left right : List (Nat × V)}
    (hsort_left : EntriesSorted left) (hsort_right : EntriesSorted right)
    (hlt : ∀ x ∈ left, ∀ y ∈ right, x.1 < y.1) :
    EntriesSorted (left ++ right) := by
  simp only [EntriesSorted, List.map_append, List.pairwise_append]
  refine ⟨hsort_left, hsort_right, ?_⟩
  intro a ha b hb
  rcases List.mem_map.mp ha with ⟨x, hx, rfl⟩
  rcases List.mem_map.mp hb with ⟨y, hy, rfl⟩
  exact hlt x hx y hy

/-- Merging two sorted internal key lists with a separator key `sep` strictly between them
    preserves `KeysSorted`. -/
theorem mergeInternal_preserves_sorted {leftKeys rightKeys : List Nat} {sep : Nat}
    (hsort_left : KeysSorted leftKeys) (hsort_right : KeysSorted rightKeys)
    (hleft_sep : ∀ x ∈ leftKeys, x < sep) (hsep_right : ∀ y ∈ rightKeys, sep < y) :
    KeysSorted (leftKeys ++ [sep] ++ rightKeys) := by
  simp only [KeysSorted, List.pairwise_append, List.pairwise_cons, List.mem_singleton]
  refine ⟨⟨hsort_left, ⟨fun y hy => by simp at hy, List.Pairwise.nil⟩, ?_⟩, hsort_right, ?_⟩
  · intro a ha b hb
    subst hb
    exact hleft_sep a ha
  · intro a ha b hb
    rcases List.mem_append.mp ha with ha_l | ha_s
    · exact Nat.lt_trans (hleft_sep a ha_l) (hsep_right b hb)
    · simp only [List.mem_singleton] at ha_s
      subst ha_s
      exact hsep_right b hb

/-- Removing from a leaf tree decreases `count` by 1 upon success. -/
theorem remove_leaf_count {V : Type} {maxKeys : Nat} {entries : List (Nat × V)}
    {k : Nat} {t' : BPlusTree Nat V}
    (h : remove maxKeys (.leaf entries) k = .ok t') :
    t'.count + 1 = (BPlusTree.leaf entries).count := by
  cases entries with
  | nil => simp [remove] at h
  | cons hd tl =>
    simp only [remove, removeNode] at h
    cases hrem : removeEntries (hd :: tl) k with
    | none => simp [hrem] at h
    | some res =>
      rcases res with ⟨entries', b⟩
      have hlen := removeEntries_length hrem
      simp only [hrem, Except.ok.injEq] at h
      subst h
      simp only [BPlusTree.count]
      omega

/-- Removing from a leaf tree produces `t'.toList = entries'` upon success. -/
theorem remove_leaf_toList {V : Type} {maxKeys : Nat} {entries entries' : List (Nat × V)}
    {k : Nat} {b : Bool} {t' : BPlusTree Nat V}
    (hrem : removeEntries entries k = some (entries', b))
    (h : remove maxKeys (.leaf entries) k = .ok t') :
    t'.toList = entries' := by
  cases entries with
  | nil => simp [removeEntries] at hrem
  | cons hd tl =>
    simp only [remove, removeNode, hrem, Except.ok.injEq] at h
    subst h
    simp [BPlusTree.toList, BPlusTree.leaves]

/-- Borrowing from a left leaf sibling conserves the total entry count across both leaves. -/
theorem borrowLeft_leaf_count_sum {V : Type} {left leftInit right : List (Nat × V)}
    {lastEntry : Nat × V} (h : splitLast? left = some (leftInit, lastEntry)) :
    leftInit.length + (lastEntry :: right).length = left.length + right.length := by
  have hlen := splitLast?_length h
  simp only [List.length_cons]
  omega

/-- Borrowing from a right leaf sibling conserves the total entry count across both leaves. -/
theorem borrowRight_leaf_count_sum {V : Type} (left : List (Nat × V)) (r0 : Nat × V)
    (rRest : List (Nat × V)) :
    (left ++ [r0]).length + rRest.length = left.length + (r0 :: rRest).length := by
  simp only [List.length_append, List.length_cons, List.length_nil]
  omega

private theorem getElem?_set_ne {α : Type} :
    (xs : List α) → {i j : Nat} → (x : α) → i ≠ j →
    (xs.set j x)[i]? = xs[i]?
  | [], _, _, _, _ => by simp
  | _ :: _, 0, 0, _, hne => by contradiction
  | _ :: _, 0, _ + 1, _, _ => by simp
  | _ :: _, _ + 1, 0, _, _ => by simp
  | _ :: xs, i + 1, j + 1, x, hne => by
    simp only [List.set_cons_succ, List.getElem?_cons_succ]
    exact getElem?_set_ne xs x (by omega)

private theorem list_map_count_set {K V : Type} :
    (xs : List (BPlusTree K V)) → {pos : Nat} → {c c' : BPlusTree K V} →
    xs[pos]? = some c →
    ((xs.set pos c').map BPlusTree.count).sum + c.count =
      (xs.map BPlusTree.count).sum + c'.count
  | [], _, _, _, h => by simp at h
  | _ :: xs, 0, c, c', h => by
    simp only [List.getElem?_cons_zero, Option.some.injEq] at h
    subst h
    simp only [List.set_cons_zero, List.map_cons, List.sum_cons]
    omega
  | _ :: xs, pos + 1, c, c', h => by
    simp only [List.getElem?_cons_succ] at h
    have ih := list_map_count_set xs h (c' := c')
    simp only [List.set_cons_succ, List.map_cons, List.sum_cons]
    omega

private theorem list_map_count_eraseListIdx {K V : Type} :
    (xs : List (BPlusTree K V)) → {pos : Nat} → {c : BPlusTree K V} →
    xs[pos]? = some c →
    ((eraseListIdx xs pos).map BPlusTree.count).sum + c.count =
      (xs.map BPlusTree.count).sum
  | [], _, _, h => by simp at h
  | _ :: xs, 0, c, h => by
    simp only [List.getElem?_cons_zero, Option.some.injEq] at h
    subst h
    simp only [eraseListIdx, List.take_zero, List.drop_succ_cons, List.drop_zero, List.nil_append,
      List.map_cons, List.sum_cons]
    omega
  | x :: xs, pos + 1, c, h => by
    simp only [List.getElem?_cons_succ] at h
    have ih := list_map_count_eraseListIdx xs h
    simp only [eraseListIdx, List.take_succ_cons, List.drop_succ_cons, List.cons_append,
      List.map_cons, List.sum_cons] at ih ⊢
    omega

private theorem splitLast?_map_count_sum {K V : Type} {xs init : List (BPlusTree K V)}
    {last : BPlusTree K V} (h : splitLast? xs = some (init, last)) :
    (init.map BPlusTree.count).sum + last.count = (xs.map BPlusTree.count).sum := by
  have happ := splitLast?_append_eq h
  rw [← happ, List.map_append, List.sum_append]
  simp

/-- `rebalanceChild` conserves the total entry count across all children. -/
theorem rebalanceChild_count {V : Type} (maxKeys : Nat) (parentKeys : List Nat)
    (children : List (BPlusTree Nat V)) (childIdx : Nat) :
    ((rebalanceChild maxKeys parentKeys children childIdx).2.map BPlusTree.count).sum =
      (children.map BPlusTree.count).sum := by
  unfold rebalanceChild
  split
  · rfl
  · rename_i child hchild
    by_cases hge : child.numKeys ≥ (if child.isLeaf then minLeafKeys maxKeys else minInternalKeys maxKeys)
    · simp [hge]
    · simp only [hge, ↓reduceIte]
      have hborrowLeft :
          ∀ res, (if childIdx > 0 then
            match children[childIdx - 1]? with
            | some leftSib =>
              let leftMin := if leftSib.isLeaf then minLeafKeys maxKeys else minInternalKeys maxKeys
              if leftSib.numKeys > leftMin then
                match leftSib, child with
                | .leaf leftEntries, .leaf childEntries =>
                  match splitLast? leftEntries with
                  | some (leftInit, lastEntry) =>
                    let leftSib' := BPlusTree.leaf leftInit
                    let child' := BPlusTree.leaf (lastEntry :: childEntries)
                    let parentKeys' := parentKeys.set (childIdx - 1) lastEntry.1
                    let children' := (children.set (childIdx - 1) leftSib').set childIdx child'
                    some (parentKeys', children')
                  | none => none
                | .internal leftKeys leftChildren, .internal childKeys childChildren =>
                  match splitLast? leftKeys, splitLast? leftChildren, parentKeys[childIdx - 1]? with
                  | some (lkInit, lastKey), some (lcInit, lastChild), some sep =>
                    let leftSib' := BPlusTree.internal lkInit lcInit
                    let child' := BPlusTree.internal (sep :: childKeys) (lastChild :: childChildren)
                    let parentKeys' := parentKeys.set (childIdx - 1) lastKey
                    let children' := (children.set (childIdx - 1) leftSib').set childIdx child'
                    some (parentKeys', children')
                  | _, _, _ => none
                | _, _ => none
              else none
            | none => none
          else none) = some res →
          (res.2.map BPlusTree.count).sum = (children.map BPlusTree.count).sum := by
        intro res hbl
        split_ifs at hbl with hgt
        · cases hleft : children[childIdx - 1]? with
          | none => simp [hleft] at hbl
          | some leftSib =>
            simp only [hleft] at hbl
            by_cases hgt2 : leftSib.numKeys >
                (if leftSib.isLeaf then minLeafKeys maxKeys else minInternalKeys maxKeys)
            · simp only [hgt2, ↓reduceIte] at hbl
              cases leftSib with
              | leaf leftEntries =>
                cases child with
                | leaf childEntries =>
                  cases hsp : splitLast? leftEntries with
                  | none => simp [hsp] at hbl
                  | some pr =>
                    rcases pr with ⟨leftInit, lastEntry⟩
                    simp only [hsp, Option.some.injEq] at hbl
                    subst hbl
                    have hlen := splitLast?_length hsp
                    have hget : (children.set (childIdx - 1) (BPlusTree.leaf leftInit))[childIdx]? =
                        some (BPlusTree.leaf childEntries) := by
                      rw [getElem?_set_ne children _ (by omega)]
                      exact hchild
                    have hset1 := list_map_count_set children hleft
                      (c' := BPlusTree.leaf leftInit)
                    have hset2 := list_map_count_set
                      (children.set (childIdx - 1) (BPlusTree.leaf leftInit)) hget
                      (c' := BPlusTree.leaf (lastEntry :: childEntries))
                    simp only [BPlusTree.count, List.length_cons] at hset1 hset2 ⊢
                    omega
                | internal _ _ => simp at hbl
              | internal leftKeys leftChildren =>
                cases child with
                | leaf _ => simp at hbl
                | internal childKeys childChildren =>
                  cases hspk : splitLast? leftKeys with
                  | none => simp [hspk] at hbl
                  | some pk =>
                    cases hspc : splitLast? leftChildren with
                    | none => simp [hspk, hspc] at hbl
                    | some pc =>
                      rcases pc with ⟨lcInit, lastChild⟩
                      cases hsep : parentKeys[childIdx - 1]? with
                      | none => simp [hspk, hspc, hsep] at hbl
                      | some sep =>
                        simp only [hspk, hspc, hsep, Option.some.injEq] at hbl
                        subst hbl
                        have hsumc := splitLast?_map_count_sum hspc
                        have hget :
                            (children.set (childIdx - 1)
                              (BPlusTree.internal pk.1 lcInit))[childIdx]? =
                            some (BPlusTree.internal childKeys childChildren) := by
                          rw [getElem?_set_ne children _ (by omega)]
                          exact hchild
                        have hset1 := list_map_count_set children hleft
                          (c' := BPlusTree.internal pk.1 lcInit)
                        have hset2 := list_map_count_set
                          (children.set (childIdx - 1) (BPlusTree.internal pk.1 lcInit)) hget
                          (c' := BPlusTree.internal (sep :: childKeys) (lastChild :: childChildren))
                        simp only [BPlusTree.count, List.map_cons, List.sum_cons] at hset1 hset2 ⊢
                        omega
            · simp [hgt2] at hbl
      have hborrowRight :
          ∀ res, (if childIdx < parentKeys.length then
            match children[childIdx + 1]? with
            | some rightSib =>
              let rightMin :=
                if rightSib.isLeaf then minLeafKeys maxKeys else minInternalKeys maxKeys
              if rightSib.numKeys > rightMin then
                match child, rightSib with
                | .leaf childEntries, .leaf (r0 :: r1 :: rs) =>
                  let child' := BPlusTree.leaf (childEntries ++ [r0])
                  let rightSib' := BPlusTree.leaf (r1 :: rs)
                  let parentKeys' := parentKeys.set childIdx r1.1
                  let children' := (children.set childIdx child').set (childIdx + 1) rightSib'
                  some (parentKeys', children')
                | .internal childKeys childChildren, .internal (rk0 :: rks) (rc0 :: rcs) =>
                  match parentKeys[childIdx]? with
                  | some sep =>
                    let child' := BPlusTree.internal (childKeys ++ [sep]) (childChildren ++ [rc0])
                    let rightSib' := BPlusTree.internal rks rcs
                    let parentKeys' := parentKeys.set childIdx rk0
                    let children' := (children.set childIdx child').set (childIdx + 1) rightSib'
                    some (parentKeys', children')
                  | none => none
                | _, _ => none
              else none
            | none => none
          else none) = some res →
          (res.2.map BPlusTree.count).sum = (children.map BPlusTree.count).sum := by
        intro res hbr
        split_ifs at hbr
        · cases hright : children[childIdx + 1]? with
          | none => simp [hright] at hbr
          | some rightSib =>
            simp only [hright] at hbr
            by_cases hgt2 : rightSib.numKeys >
                (if rightSib.isLeaf then minLeafKeys maxKeys else minInternalKeys maxKeys)
            · simp only [hgt2, ↓reduceIte] at hbr
              cases child with
              | leaf childEntries =>
                cases rightSib with
                | leaf rightEntries =>
                  cases rightEntries with
                  | nil => simp at hbr
                  | cons r0 rest =>
                    cases rest with
                    | nil => simp at hbr
                    | cons r1 rs =>
                      simp only [Option.some.injEq] at hbr
                      subst hbr
                      have hget : (children.set childIdx
                          (BPlusTree.leaf (childEntries ++ [r0])))[childIdx + 1]? =
                          some (BPlusTree.leaf (r0 :: r1 :: rs)) := by
                        rw [getElem?_set_ne children _ (by omega)]
                        exact hright
                      have hset1 := list_map_count_set children hchild
                        (c' := BPlusTree.leaf (childEntries ++ [r0]))
                      have hset2 := list_map_count_set
                        (children.set childIdx (BPlusTree.leaf (childEntries ++ [r0]))) hget
                        (c' := BPlusTree.leaf (r1 :: rs))
                      simp only [BPlusTree.count, List.length_append, List.length_cons,
                        List.length_nil] at hset1 hset2 ⊢
                      omega
                | internal _ _ => simp at hbr
              | internal childKeys childChildren =>
                cases rightSib with
                | leaf _ => simp at hbr
                | internal rightKeys rightChildren =>
                  cases rightKeys with
                  | nil => simp at hbr
                  | cons rk0 rks =>
                    cases rightChildren with
                    | nil => simp at hbr
                    | cons rc0 rcs =>
                      cases hsep : parentKeys[childIdx]? with
                      | none => simp [hsep] at hbr
                      | some sep =>
                        simp only [hsep, Option.some.injEq] at hbr
                        subst hbr
                        have hget : (children.set childIdx
                            (BPlusTree.internal (childKeys ++ [sep])
                              (childChildren ++ [rc0])))[childIdx + 1]? =
                            some (BPlusTree.internal (rk0 :: rks) (rc0 :: rcs)) := by
                          rw [getElem?_set_ne children _ (by omega)]
                          exact hright
                        have hset1 := list_map_count_set children hchild
                          (c' := BPlusTree.internal (childKeys ++ [sep]) (childChildren ++ [rc0]))
                        have hset2 := list_map_count_set
                          (children.set childIdx
                            (BPlusTree.internal (childKeys ++ [sep]) (childChildren ++ [rc0])))
                          hget (c' := BPlusTree.internal rks rcs)
                        simp only [BPlusTree.count, List.map_append, List.sum_append,
                          List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
                          at hset1 hset2 ⊢
                        omega
            · simp [hgt2] at hbr
      split
      · rename_i res hbl
        exact hborrowLeft res hbl
      · split
        · rename_i res hbr
          exact hborrowRight res hbr
        · split_ifs with hgt
          · cases hleft : children[childIdx - 1]? with
            | none => rfl
            | some leftSib =>
              cases leftSib with
              | leaf leftEntries =>
                cases child with
                | leaf childEntries =>
                  dsimp only
                  have hget : (children.set (childIdx - 1)
                      (BPlusTree.leaf (leftEntries ++ childEntries)))[childIdx]? =
                      some (BPlusTree.leaf childEntries) := by
                    rw [getElem?_set_ne children _ (by omega)]
                    exact hchild
                  have her := list_map_count_eraseListIdx
                    (children.set (childIdx - 1) (BPlusTree.leaf (leftEntries ++ childEntries)))
                    hget
                  have hset := list_map_count_set children hleft
                    (c' := BPlusTree.leaf (leftEntries ++ childEntries))
                  simp only [BPlusTree.count, List.length_append] at her hset ⊢
                  omega
                | internal _ _ => rfl
              | internal leftKeys leftChildren =>
                cases child with
                | leaf _ => rfl
                | internal childKeys childChildren =>
                  cases hsep : parentKeys[childIdx - 1]? with
                  | none => rfl
                  | some sep =>
                    dsimp only
                    have hget : (children.set (childIdx - 1)
                        (BPlusTree.internal (leftKeys ++ [sep] ++ childKeys)
                          (leftChildren ++ childChildren)))[childIdx]? =
                        some (BPlusTree.internal childKeys childChildren) := by
                      rw [getElem?_set_ne children _ (by omega)]
                      exact hchild
                    have her := list_map_count_eraseListIdx
                      (children.set (childIdx - 1)
                        (BPlusTree.internal (leftKeys ++ [sep] ++ childKeys)
                          (leftChildren ++ childChildren))) hget
                    have hset := list_map_count_set children hleft
                      (c' := BPlusTree.internal (leftKeys ++ [sep] ++ childKeys)
                        (leftChildren ++ childChildren))
                    simp only [BPlusTree.count, List.map_append, List.sum_append] at her hset ⊢
                    omega
          · cases hright : children[childIdx + 1]? with
            | none => rfl
            | some rightSib =>
              cases child with
              | leaf childEntries =>
                cases rightSib with
                | leaf rightEntries =>
                  dsimp only
                  have hget : (children.set childIdx
                      (BPlusTree.leaf (childEntries ++ rightEntries)))[childIdx + 1]? =
                      some (BPlusTree.leaf rightEntries) := by
                    rw [getElem?_set_ne children _ (by omega)]
                    exact hright
                  have her := list_map_count_eraseListIdx
                    (children.set childIdx (BPlusTree.leaf (childEntries ++ rightEntries)))
                    hget
                  have hset := list_map_count_set children hchild
                    (c' := BPlusTree.leaf (childEntries ++ rightEntries))
                  simp only [BPlusTree.count, List.length_append] at her hset ⊢
                  omega
                | internal _ _ => rfl
              | internal childKeys childChildren =>
                cases rightSib with
                | leaf _ => rfl
                | internal rightKeys rightChildren =>
                  cases hsep : parentKeys[childIdx]? with
                  | none => rfl
                  | some sep =>
                    dsimp only
                    have hget : (children.set childIdx
                        (BPlusTree.internal (childKeys ++ [sep] ++ rightKeys)
                          (childChildren ++ rightChildren)))[childIdx + 1]? =
                        some (BPlusTree.internal rightKeys rightChildren) := by
                      rw [getElem?_set_ne children _ (by omega)]
                      exact hright
                    have her := list_map_count_eraseListIdx
                      (children.set childIdx
                        (BPlusTree.internal (childKeys ++ [sep] ++ rightKeys)
                          (childChildren ++ rightChildren))) hget
                    have hset := list_map_count_set children hchild
                      (c' := BPlusTree.internal (childKeys ++ [sep] ++ rightKeys)
                        (childChildren ++ rightChildren))
                    simp only [BPlusTree.count, List.map_append, List.sum_append] at her hset ⊢
                    omega

/-- Multi-level count conservation for `removeNode`: a successful deletion from any subtree
    decreases `count` by 1. -/
theorem removeNode_count {V : Type} (maxKeys : Nat) :
    (t : BPlusTree Nat V) → {k : Nat} → {t' : BPlusTree Nat V} →
    {p : Option Nat} → {b : Bool} →
    removeNode maxKeys t k = some (t', p, b) →
    t'.count + 1 = t.count
  | .leaf entries, k, t', p, b, h => by
    simp only [removeNode] at h
    cases hrem : removeEntries entries k with
    | none => simp [hrem] at h
    | some res =>
      rcases res with ⟨entries', wasFirst⟩
      have hlen := removeEntries_length hrem
      simp only [hrem, Option.some.injEq, Prod.mk.injEq] at h
      rcases h with ⟨rfl, _, _⟩
      simp only [BPlusTree.count]
      exact hlen
  | .internal keys children, k, t', p, b, h => by
    simp only [removeNode] at h
    split at h
    · simp at h
    · rename_i child hc
      have hmem : child ∈ children := List.mem_of_getElem? hc
      split at h
      · simp at h
      · rename_i child' pendingMin doRebalance hrec
        have ih := removeNode_count maxKeys child hrec
        have hset := list_map_count_set children hc (c' := child')
        cases doRebalance
        · simp only [Bool.false_eq_true, ↓reduceIte, Option.some.injEq, Prod.mk.injEq] at h
          rcases h with ⟨rfl, _, _⟩
          simp only [BPlusTree.count]
          omega
        · simp only [↓reduceIte] at h
          by_cases hge : child'.numKeys ≥
              (if child'.isLeaf then minLeafKeys maxKeys else minInternalKeys maxKeys)
          · simp only [hge, ↓reduceIte, Option.some.injEq, Prod.mk.injEq] at h
            rcases h with ⟨rfl, _, _⟩
            simp only [BPlusTree.count]
            omega
          · simp only [hge, ↓reduceIte, Option.some.injEq, Prod.mk.injEq] at h
            rcases h with ⟨rfl, _, _⟩
            have hreb := rebalanceChild_count maxKeys
              (match pendingMin with
                | none => (keys, none)
                | some newMin =>
                  if findChildIdx keys k > 0 then
                    if keys[findChildIdx keys k - 1]? == some k then
                      (keys.set (findChildIdx keys k - 1) newMin, none)
                    else (keys, some newMin)
                  else (keys, some newMin)).1
              (children.set (findChildIdx keys k) child') (findChildIdx keys k)
            simp only [BPlusTree.count]
            omega
  termination_by t => sizeOf t
  decreasing_by
    all_goals (
      simp_wf
      have hsz := List.sizeOf_lt_of_mem hmem
      omega
    )

/-- Multi-level count conservation for `remove`: a successful deletion from any tree
    decreases `count` by 1. -/
theorem remove_count {V : Type} {maxKeys : Nat} {t t' : BPlusTree Nat V}
    {k : Nat} (h : remove maxKeys t k = .ok t') :
    t'.count + 1 = t.count := by
  cases t with
  | leaf entries =>
    cases entries with
    | nil => simp [remove] at h
    | cons hd tl =>
      simp only [remove] at h
      cases hrem : removeNode maxKeys (.leaf (hd :: tl)) k with
      | none => simp [hrem] at h
      | some res =>
        rcases res with ⟨t0, p, b⟩
        have hcnt := removeNode_count maxKeys (.leaf (hd :: tl)) hrem
        simp only [hrem] at h
        cases t0 with
        | leaf entries0 =>
          simp only [Except.ok.injEq] at h
          subst h
          exact hcnt
        | internal keys0 children0 =>
          cases keys0 with
          | nil =>
            cases children0 with
            | nil =>
              simp only [Except.ok.injEq] at h
              subst h
              exact hcnt
            | cons c cs =>
              cases cs with
              | nil =>
                simp only [Except.ok.injEq] at h
                subst h
                simp only [BPlusTree.count, List.map_cons, List.map_nil, List.sum_cons,
                  List.sum_nil] at hcnt ⊢
                omega
              | cons _ _ =>
                simp only [Except.ok.injEq] at h
                subst h
                exact hcnt
          | cons _ _ =>
            simp only [Except.ok.injEq] at h
            subst h
            exact hcnt
  | internal keys children =>
    simp only [remove] at h
    cases hrem : removeNode maxKeys (.internal keys children) k with
    | none => simp [hrem] at h
    | some res =>
      rcases res with ⟨t0, p, b⟩
      have hcnt := removeNode_count maxKeys (.internal keys children) hrem
      simp only [hrem] at h
      cases t0 with
      | leaf entries0 =>
        simp only [Except.ok.injEq] at h
        subst h
        exact hcnt
      | internal keys0 children0 =>
        cases keys0 with
        | nil =>
          cases children0 with
          | nil =>
            simp only [Except.ok.injEq] at h
            subst h
            exact hcnt
          | cons c cs =>
            cases cs with
            | nil =>
              simp only [Except.ok.injEq] at h
              subst h
              simp only [BPlusTree.count, List.map_cons, List.map_nil, List.sum_cons,
                List.sum_nil] at hcnt ⊢
              omega
            | cons _ _ =>
              simp only [Except.ok.injEq] at h
              subst h
              exact hcnt
        | cons _ _ =>
          simp only [Except.ok.injEq] at h
          subst h
          exact hcnt

end Bptree

end
