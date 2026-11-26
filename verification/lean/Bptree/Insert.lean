module

public import Bptree.Search

@[expose] public section

namespace Bptree

/-- Insert `(k, v)` into a sorted leaf entry list. Returns `none` if `k` already exists,
    matching `BPTREE_DUPLICATE_KEY` in `bptree_insert_internal`. -/
def insertEntries {V : Type} (entries : List (Nat × V)) (k : Nat) (v : V) :
    Option (List (Nat × V)) :=
  match entries with
  | [] => some [(k, v)]
  | (k', v') :: rest =>
    if k < k' then
      some ((k, v) :: (k', v') :: rest)
    else if k == k' then
      none
    else
      (insertEntries rest k v).map ((k', v') :: ·)

/-- Split an overflowing leaf entry list using `splitIdx = (entries.length + 1) / 2`,
    matching `bptree_insert_internal` (line 1218). Returns `(left, promotedKey, right)`. -/
def splitLeafEntries {V : Type} (entries : List (Nat × V)) :
    List (Nat × V) × Nat × List (Nat × V) :=
  let splitIdx := (entries.length + 1) / 2
  let left := entries.take splitIdx
  let right := entries.drop splitIdx
  let promoted := match right.head? with
    | some (k, _) => k
    | none => 0
  (left, promoted, right)

/-- Split an overflowing internal node using `splitIdx = keys.length / 2`,
    matching `bptree_insert_internal` (line 1264).
    Returns `((leftKeys, leftChildren), promotedKey, (rightKeys, rightChildren))`. -/
def splitInternalNodes {K V : Type} (keys : List K) (children : List (BPlusTree K V)) (defaultK : K) :
    (List K × List (BPlusTree K V)) × K × (List K × List (BPlusTree K V)) :=
  let splitIdx := keys.length / 2
  let leftKeys := keys.take splitIdx
  let promoted := keys[splitIdx]?.getD defaultK
  let rightKeys := keys.drop (splitIdx + 1)
  let leftChildren := children.take (splitIdx + 1)
  let rightChildren := children.drop (splitIdx + 1)
  ((leftKeys, leftChildren), promoted, (rightKeys, rightChildren))

/-- Insert `(k, v)` into a subtree, returning the updated node and an optional split `(promotedKey, rightNode)`.
    Returns `none` if `k` is a duplicate key. -/
def insertNode {V : Type} (maxKeys : Nat) (t : BPlusTree Nat V) (k : Nat) (v : V) :
    Option (BPlusTree Nat V × Option (Nat × BPlusTree Nat V)) :=
  match t with
  | .leaf entries =>
    match insertEntries entries k v with
    | none => none
    | some entries' =>
      if entries'.length > maxKeys then
        let (left, promoted, right) := splitLeafEntries entries'
        some (.leaf left, some (promoted, .leaf right))
      else
        some (.leaf entries', none)
  | .internal keys children =>
    let pos := findChildIdx keys k
    match _hc : children[pos]? with
    | none => none
    | some child =>
      match insertNode maxKeys child k v with
      | none => none
      | some (child', none) =>
        some (.internal keys (children.set pos child'), none)
      | some (child', some (promotedKey, newChild)) =>
        let keys' := keys.take pos ++ [promotedKey] ++ keys.drop pos
        let children' := (children.set pos child').take (pos + 1) ++ [newChild] ++
          (children.set pos child').drop (pos + 1)
        if keys'.length > maxKeys then
          let ((lk, lc), upKey, (rk, rc)) := splitInternalNodes keys' children' 0
          some (.internal lk lc, some (upKey, .internal rk rc))
        else
          some (.internal keys' children', none)
  termination_by sizeOf t
  decreasing_by
    simp_wf
    have hmem : child ∈ children := List.mem_of_getElem? _hc
    have hsz := List.sizeOf_lt_of_mem hmem
    omega

/-- Top-level insertion matching `bptree_put`. -/
def insert {V : Type} (maxKeys : Nat) (t : BPlusTree Nat V) (k : Nat) (v : V) :
    Except Status (BPlusTree Nat V) :=
  match insertNode maxKeys t k v with
  | none => .error .duplicateKey
  | some (t', none) => .ok t'
  | some (t', some (promotedKey, right)) =>
    .ok (.internal [promotedKey] [t', right])

/-- Helper for `countContiguousSplits`. -/
def countContiguousSplitsGo {V : Type} (maxKeys : Nat) (k : Nat) :
    BPlusTree Nat V → Nat → Nat
  | .leaf entries, acc =>
    if entries.length == maxKeys then acc + 1 else 0
  | .internal keys children, acc =>
    let acc' := if keys.length == maxKeys then acc + 1 else 0
    let pos := findChildIdx keys k
    match _hc : children[pos]? with
    | some child => countContiguousSplitsGo maxKeys k child acc'
    | none => acc'
  termination_by t _ => sizeOf t
  decreasing_by
    simp_wf
    have hmem : child ∈ children := List.mem_of_getElem? _hc
    have hsz := List.sizeOf_lt_of_mem hmem
    omega

/-- Pre-pass split counter matching `bptree_put` (lines 1293-1299):
    counts the contiguous suffix of full nodes along the search path for `k`. -/
def countContiguousSplits {V : Type} (maxKeys : Nat) (t : BPlusTree Nat V) (k : Nat) : Nat :=
  countContiguousSplitsGo maxKeys k t 0

/-- Exact number of nodes preallocated by `bptree_put` (lines 1304-1306). -/
def reservedNodesNeeded {V : Type} (maxKeys : Nat) (t : BPlusTree Nat V) (k : Nat) : Nat :=
  let splits := countContiguousSplits maxKeys t k
  let splitRoot := splits == t.height
  splits + (if splitRoot then 1 else 0)

/-- Inserting a key that is already present in `entries` returns `none`. -/
theorem insertEntries_duplicate {V : Type} {entries : List (Nat × V)} {k : Nat} {v v₀ : V}
    (hsort : EntriesSorted entries) (hlookup : leafLookup entries k = some v₀) :
    insertEntries entries k v = none := by
  induction entries with
  | nil =>
    simp [leafLookup] at hlookup
  | cons hd tl ih =>
    rcases hd with ⟨k', v'⟩
    simp only [EntriesSorted, List.map_cons, List.pairwise_cons] at hsort
    simp only [leafLookup] at hlookup
    by_cases hkk : k == k'
    · have hkeq : k = k' := beq_iff_eq.mp hkk
      subst hkeq
      simp [insertEntries]
    · simp only [hkk, Bool.false_eq_true, ↓reduceIte] at hlookup
      have hne : k ≠ k' := fun h => hkk (beq_iff_eq.mpr h)
      have hmem : (k, v₀) ∈ tl := leafLookup_mem hlookup
      have hgt : k' < k := hsort.1 k (List.mem_map.mpr ⟨(k, v₀), hmem, rfl⟩)
      have hnlt : ¬(k < k') := Nat.not_lt.mpr (Nat.le_of_lt hgt)
      simp only [insertEntries, hnlt, ↓reduceIte, hkk, Bool.false_eq_true]
      simp [ih hsort.2 hlookup]

/-- Inserting a new key increases `entries.length` by 1. -/
theorem insertEntries_length {V : Type} {entries entries' : List (Nat × V)} {k : Nat} {v : V}
    (h : insertEntries entries k v = some entries') :
    entries'.length = entries.length + 1 := by
  induction entries generalizing entries' with
  | nil =>
    simp [insertEntries] at h
    subst h
    rfl
  | cons hd tl ih =>
    rcases hd with ⟨k', v'⟩
    simp only [insertEntries] at h
    split_ifs at h with hlt hkk
    · injection h with h'
      subst h'
      simp
    · simp only [Option.map_eq_some_iff] at h
      rcases h with ⟨tl', htl', hcons⟩
      subst hcons
      simp [ih htl']

/-- Inserting a new key `(k, v)` into `entries` allows `leafLookup` to retrieve `v`. -/
theorem leafLookup_insertEntries_self {V : Type} {entries entries' : List (Nat × V)}
    {k : Nat} {v : V} (h : insertEntries entries k v = some entries') :
    leafLookup entries' k = some v := by
  induction entries generalizing entries' with
  | nil =>
    simp [insertEntries] at h
    subst h
    simp [leafLookup]
  | cons hd tl ih =>
    rcases hd with ⟨k', v'⟩
    simp only [insertEntries] at h
    split_ifs at h with hlt hkk
    · injection h with h'
      subst h'
      simp [leafLookup]
    · simp only [Option.map_eq_some_iff] at h
      rcases h with ⟨tl', htl', hcons⟩
      subst hcons
      simp only [leafLookup, hkk, Bool.false_eq_true, ↓reduceIte]
      exact ih htl'

/-- Inserting `(k, v)` into `entries` does not alter lookups for any other key `k' ≠ k`. -/
theorem leafLookup_insertEntries_ne {V : Type} {entries entries' : List (Nat × V)}
    {k k' : Nat} {v : V} (hne : k' ≠ k) (h : insertEntries entries k v = some entries') :
    leafLookup entries' k' = leafLookup entries k' := by
  induction entries generalizing entries' with
  | nil =>
    simp [insertEntries] at h
    subst h
    have hbeq : (k' == k) = false := beq_false_of_ne hne
    simp [leafLookup, hbeq]
  | cons hd tl ih =>
    rcases hd with ⟨kh, vh⟩
    simp only [insertEntries] at h
    split_ifs at h with hlt hkk
    · injection h with h'
      subst h'
      have hbeq : (k' == k) = false := beq_false_of_ne hne
      simp [leafLookup, hbeq]
    · simp only [Option.map_eq_some_iff] at h
      rcases h with ⟨tl', htl', hcons⟩
      subst hcons
      simp only [leafLookup]
      by_cases hkh : k' == kh
      · simp [hkh]
      · simp only [hkh, Bool.false_eq_true, ↓reduceIte]
        exact ih htl'

/-- Keys in `insertEntries` are strictly a superset of `entries`'s keys plus `k`. -/
private theorem mem_insertEntries_iff {V : Type} {entries entries' : List (Nat × V)}
    {k : Nat} {v : V} (h : insertEntries entries k v = some entries') (p : Nat × V) :
    p ∈ entries' ↔ p = (k, v) ∨ p ∈ entries := by
  induction entries generalizing entries' with
  | nil =>
    simp [insertEntries] at h
    subst h
    simp
  | cons hd tl ih =>
    rcases hd with ⟨k', v'⟩
    simp only [insertEntries] at h
    split_ifs at h with hlt hkk
    · injection h with h'
      subst h'
      simp
    · simp only [Option.map_eq_some_iff] at h
      rcases h with ⟨tl', htl', hcons⟩
      subst hcons
      simp only [List.mem_cons, ih htl']
      tauto

/-- `insertEntries` preserves `EntriesSorted`. -/
theorem insertEntries_preserves_sorted {V : Type} {entries entries' : List (Nat × V)}
    {k : Nat} {v : V} (hsort : EntriesSorted entries)
    (h : insertEntries entries k v = some entries') :
    EntriesSorted entries' := by
  induction entries generalizing entries' with
  | nil =>
    simp [insertEntries] at h
    subst h
    simp [EntriesSorted]
  | cons hd tl ih =>
    rcases hd with ⟨k', v'⟩
    simp only [EntriesSorted, List.map_cons, List.pairwise_cons] at hsort
    simp only [insertEntries] at h
    split_ifs at h with hlt hkk
    · injection h with h'
      subst h'
      simp only [EntriesSorted, List.map_cons, List.pairwise_cons]
      refine ⟨?_, hsort.1, hsort.2⟩
      intro x hx
      rcases List.mem_cons.mp hx with rfl | hx'
      · exact hlt
      · rcases List.mem_map.mp hx' with ⟨⟨xk, vx⟩, hmem, rfl⟩
        exact Nat.lt_trans hlt (hsort.1 xk (List.mem_map.mpr ⟨(xk, vx), hmem, rfl⟩))
    · simp only [Option.map_eq_some_iff] at h
      rcases h with ⟨tl', htl', hcons⟩
      subst hcons
      have ih_sorted := ih hsort.2 htl'
      simp only [EntriesSorted, List.map_cons, List.pairwise_cons]
      refine ⟨?_, ih_sorted⟩
      intro x hx
      rcases List.mem_map.mp hx with ⟨⟨xk, vx⟩, hmem, rfl⟩
      rcases (mem_insertEntries_iff htl' (xk, vx)).mp hmem with heq | htl
      · injection heq with hk _
        have hne : k ≠ k' := fun he => hkk (beq_iff_eq.mpr he)
        omega
      · exact hsort.1 xk (List.mem_map.mpr ⟨(xk, vx), htl, rfl⟩)

/-- Splitting a leaf preserves all entries without loss or duplication. -/
theorem splitLeafEntries_append {V : Type} (entries : List (Nat × V)) :
    let (left, _, right) := splitLeafEntries entries
    left ++ right = entries := by
  simp [splitLeafEntries, List.take_append_drop]

/-- Left half of a leaf split satisfies `[minLeafKeys maxKeys, maxKeys]` when overflowing. -/
theorem splitLeaf_left_bounds {V : Type} {maxKeys : Nat} {entries : List (Nat × V)}
    (hcap : 3 ≤ maxKeys) (hlen : entries.length = maxKeys + 1) :
    let (left, _, _) := splitLeafEntries entries
    minLeafKeys maxKeys ≤ left.length ∧ left.length ≤ maxKeys := by
  simp only [splitLeafEntries, List.length_take, hlen, minLeafKeys]
  omega

/-- Right half of a leaf split satisfies `[minLeafKeys maxKeys, maxKeys]` when overflowing. -/
theorem splitLeaf_right_bounds {V : Type} {maxKeys : Nat} {entries : List (Nat × V)}
    (hcap : 3 ≤ maxKeys) (hlen : entries.length = maxKeys + 1) :
    let (_, _, right) := splitLeafEntries entries
    minLeafKeys maxKeys ≤ right.length ∧ right.length ≤ maxKeys := by
  simp only [splitLeafEntries, List.length_drop, hlen, minLeafKeys]
  omega

/-- Left half of an internal split satisfies `[minInternalKeys maxKeys, maxKeys]` when overflowing. -/
theorem splitInternal_left_bounds {K V : Type} {maxKeys : Nat} {keys : List K}
    {children : List (BPlusTree K V)} (defaultK : K)
    (hcap : 3 ≤ maxKeys) (hlen : keys.length = maxKeys + 1) :
    let ((lk, _), _, _) := splitInternalNodes keys children defaultK
    minInternalKeys maxKeys ≤ lk.length ∧ lk.length ≤ maxKeys := by
  simp only [splitInternalNodes, List.length_take, hlen, minInternalKeys]
  omega

/-- Right half of an internal split satisfies `[minInternalKeys maxKeys, maxKeys]` when overflowing. -/
theorem splitInternal_right_bounds {K V : Type} {maxKeys : Nat} {keys : List K}
    {children : List (BPlusTree K V)} (defaultK : K)
    (hcap : 3 ≤ maxKeys) (hlen : keys.length = maxKeys + 1) :
    let (_, _, (rk, _)) := splitInternalNodes keys children defaultK
    minInternalKeys maxKeys ≤ rk.length ∧ rk.length ≤ maxKeys := by
  simp only [splitInternalNodes, List.length_drop, hlen, minInternalKeys]
  omega

private theorem uniformDepth_eq_height {K V : Type} :
    {t : BPlusTree K V} → {d : Nat} → UniformDepth t d → t.height = d
  | .leaf _, 0, hbal => by simp [UniformDepth] at hbal
  | .leaf _, 1, _ => rfl
  | .leaf _, _ + 2, hbal => by simp [UniformDepth] at hbal
  | .internal _ _, 0, hbal => by simp [UniformDepth] at hbal
  | .internal _ [], _ + 1, hbal => by simp [UniformDepth] at hbal
  | .internal _ (c :: _), d' + 1, hbal => by
    rcases hbal with ⟨_, hchildren⟩
    simp only [BPlusTree.height]
    have hc := uniformDepth_eq_height (hchildren c (by simp))
    omega

private theorem countContiguousSplitsGo_le {V : Type} (maxKeys : Nat) (k : Nat) :
    (t : BPlusTree Nat V) → (d acc : Nat) → UniformDepth t d →
    countContiguousSplitsGo maxKeys k t acc ≤ acc + d
  | .leaf entries, 0, _, hbal => by simp [UniformDepth] at hbal
  | .leaf entries, 1, acc, _ => by
    simp only [countContiguousSplitsGo]
    split_ifs <;> omega
  | .leaf entries, _ + 2, _, hbal => by cases hbal
  | .internal _ _, 0, _, hbal => by cases hbal
  | .internal keys children, d' + 1, acc, hbal => by
    rcases hbal with ⟨_, hchildren⟩
    simp only [countContiguousSplitsGo]
    split
    · rename_i child hsome
      have hmem : child ∈ children := List.mem_of_getElem? hsome
      have hle := countContiguousSplitsGo_le maxKeys k child d'
        (if keys.length == maxKeys then acc + 1 else 0) (hchildren child hmem)
      by_cases hfull : (keys.length == maxKeys) = true
      · simp only [hfull, ↓reduceIte] at hle ⊢; omega
      · simp only [hfull, Bool.false_eq_true, ↓reduceIte] at hle ⊢; omega
    · split_ifs <;> omega

/-- The contiguous split counter never exceeds the height of a uniformly balanced tree. -/
theorem countContiguousSplits_le_height {V : Type} {maxKeys : Nat} (t : BPlusTree Nat V) (k : Nat)
    (hbal : UniformDepth t t.height) :
    countContiguousSplits maxKeys t k ≤ t.height := by
  unfold countContiguousSplits
  have h := countContiguousSplitsGo_le maxKeys k t t.height 0 hbal
  omega

/-- Reserved node count never exceeds `t.height + 1`. -/
theorem reservedNodesNeeded_le_height_succ {V : Type} {maxKeys : Nat} (t : BPlusTree Nat V)
    (k : Nat) (hbal : UniformDepth t t.height) :
    reservedNodesNeeded maxKeys t k ≤ t.height + 1 := by
  unfold reservedNodesNeeded
  have h := countContiguousSplits_le_height (maxKeys := maxKeys) t k hbal
  dsimp only
  split_ifs <;> omega

/-- The left half of a leaf split preserves `EntriesSorted`. -/
theorem splitLeafEntries_left_sorted {V : Type} {entries : List (Nat × V)}
    (hsort : EntriesSorted entries) :
    EntriesSorted (splitLeafEntries entries).1 := by
  simp only [splitLeafEntries, EntriesSorted]
  exact hsort.sublist ((List.take_sublist _ _).map Prod.fst)

/-- The right half of a leaf split preserves `EntriesSorted`. -/
theorem splitLeafEntries_right_sorted {V : Type} {entries : List (Nat × V)}
    (hsort : EntriesSorted entries) :
    EntriesSorted (splitLeafEntries entries).2.2 := by
  simp only [splitLeafEntries, EntriesSorted]
  exact hsort.sublist ((List.drop_sublist _ _).map Prod.fst)

/-- Splitting an internal node preserves `children.length = keys.length + 1` for both halves. -/
theorem splitInternal_children_lengths {K V : Type} {maxKeys : Nat} {keys : List K}
    {children : List (BPlusTree K V)} (defaultK : K)
    (hcap : 3 ≤ maxKeys) (hlen : keys.length = maxKeys + 1)
    (hch : children.length = keys.length + 1) :
    let ((lk, lc), _, (rk, rc)) := splitInternalNodes keys children defaultK
    lc.length = lk.length + 1 ∧ rc.length = rk.length + 1 := by
  simp only [splitInternalNodes, List.length_take, List.length_drop, hch, hlen]
  omega

/-- Full-tree duplicate rejection for `insertNode`: if `lookup t k = some v₀` in a sorted tree,
    `insertNode` returns `none`. -/
theorem insertNode_duplicate_of_lookup {V : Type} (maxKeys : Nat) :
    (t : BPlusTree Nat V) → {k : Nat} → {v v₀ : V} →
    NodeSorted t → lookup t k = some v₀ → insertNode maxKeys t k v = none
  | .leaf entries, k, v, _, hsort, hlook => by
    simp only [NodeSorted] at hsort
    simp only [lookup] at hlook
    simp only [insertNode, insertEntries_duplicate hsort hlook]
  | .internal keys children, k, v, v₀, hsort, hlook => by
    simp only [NodeSorted] at hsort
    simp only [lookup] at hlook
    simp only [insertNode]
    split at hlook
    · rename_i child hc
      split
      · rfl
      · rename_i child' hc'
        rw [hc] at hc'
        injection hc' with hceq
        have hmem : child ∈ children := List.mem_of_getElem? hc
        have ih := insertNode_duplicate_of_lookup maxKeys child (v := v) (hsort.2 child hmem) hlook
        rw [← hceq]
        simp [ih]
    · simp at hlook
  termination_by t => sizeOf t
  decreasing_by
    simp_wf
    have hsz := List.sizeOf_lt_of_mem hmem
    omega

/-- Full-tree duplicate rejection for `insert`: if `lookup t k = some v₀` in a sorted tree,
    `insert` returns `Status.duplicateKey`. -/
theorem insert_duplicate_of_lookup {V : Type} (maxKeys : Nat) {t : BPlusTree Nat V}
    {k : Nat} {v v₀ : V} (hsort : NodeSorted t) (hlook : lookup t k = some v₀) :
    insert maxKeys t k v = .error .duplicateKey := by
  simp [insert, insertNode_duplicate_of_lookup maxKeys t hsort hlook]

/-- When an overflowing leaf splits, the promoted separator key equals `minKey` of the right leaf. -/
theorem splitLeafEntries_promoted_eq_min {V : Type} {maxKeys : Nat} {entries : List (Nat × V)}
    (hcap : 3 ≤ maxKeys) (hlen : entries.length = maxKeys + 1) :
    let (_, promoted, right) := splitLeafEntries entries
    minKey (.leaf right) = some promoted := by
  simp only [splitLeafEntries, minKey]
  have hpos : 0 < (entries.drop ((entries.length + 1) / 2)).length := by
    simp only [List.length_drop, hlen]
    omega
  cases hhead : (entries.drop ((entries.length + 1) / 2)).head? with
  | none =>
    have hempty := List.head?_eq_none_iff.mp hhead
    simp [hempty] at hpos
  | some p =>
    simp [Option.map]

/-- Every key in the left half of a sorted leaf split is strictly less than the promoted key
    (when the right half starts with `(promoted, vRight)`). -/
theorem splitLeafEntries_left_lt_promoted {V : Type} {entries : List (Nat × V)}
    {promoted : Nat} {vRight : V} {rest : List (Nat × V)} {p : Nat × V}
    (hsort : EntriesSorted entries)
    (hp : p ∈ (splitLeafEntries entries).1)
    (hright : (splitLeafEntries entries).2.2 = (promoted, vRight) :: rest) :
    p.1 < promoted := by
  have happ : (splitLeafEntries entries).1 ++ (splitLeafEntries entries).2.2 = entries :=
    splitLeafEntries_append entries
  rw [← happ, hright] at hsort
  simp only [EntriesSorted, List.map_append, List.map_cons, List.pairwise_append] at hsort
  exact hsort.2.2 p.1 (List.mem_map.mpr ⟨p, hp, rfl⟩) promoted (by simp)

/-- The left key list of an internal split preserves `KeysSorted`. -/
theorem splitInternal_left_sorted {V : Type} {keys : List Nat}
    {children : List (BPlusTree Nat V)} (defaultK : Nat)
    (hsort : KeysSorted keys) :
    KeysSorted (splitInternalNodes keys children defaultK).1.1 := by
  simp only [splitInternalNodes, KeysSorted]
  exact hsort.sublist (List.take_sublist _ _)

/-- The right key list of an internal split preserves `KeysSorted`. -/
theorem splitInternal_right_sorted {V : Type} {keys : List Nat}
    {children : List (BPlusTree Nat V)} (defaultK : Nat)
    (hsort : KeysSorted keys) :
    KeysSorted (splitInternalNodes keys children defaultK).2.2.1 := by
  simp only [splitInternalNodes, KeysSorted]
  exact hsort.sublist (List.drop_sublist _ _)

/-- Splitting a leaf conserves the total number of entries. -/
theorem splitLeafEntries_length_sum {V : Type} (entries : List (Nat × V)) :
    let (left, _, right) := splitLeafEntries entries
    left.length + right.length = entries.length := by
  simp only [splitLeafEntries, List.length_take, List.length_drop]
  omega

/-- Splitting an internal node conserves the total number of keys (left + promoted + right). -/
theorem splitInternal_keys_length_sum {K V : Type} {keys : List K}
    {children : List (BPlusTree K V)} (defaultK : K) (hne : 1 ≤ keys.length) :
    let ((lk, _), _, (rk, _)) := splitInternalNodes keys children defaultK
    lk.length + 1 + rk.length = keys.length := by
  simp only [splitInternalNodes, List.length_take, List.length_drop]
  omega

/-- Splitting an internal node conserves all child subtrees in order. -/
theorem splitInternal_children_append {K V : Type} (keys : List K)
    (children : List (BPlusTree K V)) (defaultK : K) :
    let ((_, lc), _, (_, rc)) := splitInternalNodes keys children defaultK
    lc ++ rc = children := by
  simp [splitInternalNodes, List.take_append_drop]

/-- Inserting into a leaf tree increases `count` by 1 upon success, even across a root split. -/
theorem insert_leaf_count {V : Type} {maxKeys : Nat} {entries : List (Nat × V)}
    {k : Nat} {v : V} {t' : BPlusTree Nat V}
    (h : insert maxKeys (.leaf entries) k v = .ok t') :
    t'.count = (BPlusTree.leaf entries).count + 1 := by
  simp only [insert, insertNode] at h
  cases hent : insertEntries entries k v with
  | none => simp [hent] at h
  | some entries' =>
    have hlen := insertEntries_length hent
    simp only [hent] at h
    split_ifs at h
    · simp only [Except.ok.injEq] at h
      subst h
      have hsum := splitLeafEntries_length_sum entries'
      simp only [BPlusTree.count, List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      omega
    · simp only [Except.ok.injEq] at h
      subst h
      simp only [BPlusTree.count]
      omega

/-- Inserting into a leaf tree produces `t'.toList = entries'` upon success, even across a root split. -/
theorem insert_leaf_toList {V : Type} {maxKeys : Nat} {entries entries' : List (Nat × V)}
    {k : Nat} {v : V} {t' : BPlusTree Nat V}
    (hent : insertEntries entries k v = some entries')
    (h : insert maxKeys (.leaf entries) k v = .ok t') :
    t'.toList = entries' := by
  simp only [insert, insertNode, hent] at h
  split_ifs at h
  · simp only [Except.ok.injEq] at h
    subst h
    simp only [BPlusTree.toList, BPlusTree.leaves, List.flatMap_cons, List.flatMap_nil,
      List.append_nil, List.singleton_append, List.flatten_cons, List.flatten_nil]
    exact splitLeafEntries_append entries'
  · simp only [Except.ok.injEq] at h
    subst h
    simp [BPlusTree.toList, BPlusTree.leaves]

end Bptree

end
