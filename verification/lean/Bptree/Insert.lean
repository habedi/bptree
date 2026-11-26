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

/-- Non-splitting leaf insertion preserves height 1. -/
theorem insertNode_leaf_non_split_height {V : Type} {maxKeys : Nat}
    {entries : List (Nat × V)} {k : Nat} {v : V} {t' : BPlusTree Nat V}
    (h : insertNode maxKeys (.leaf entries) k v = some (t', none)) :
    t'.height = 1 := by
  simp only [insertNode] at h
  cases hent : insertEntries entries k v with
  | none => simp [hent] at h
  | some entries' =>
    simp only [hent] at h
    split_ifs at h
    · simp at h
    · simp only [Option.some.injEq, Prod.mk.injEq] at h
      rcases h with ⟨rfl, _⟩
      rfl

/-- A root split increases the height of the left subtree by 1. -/
theorem insert_split_root_height {V : Type} (t r : BPlusTree Nat V) (p : Nat) :
    (BPlusTree.internal [p] [t, r]).height = t.height + 1 := by
  simp [BPlusTree.height]
  omega

/-- A root split sums the element counts of both new root children. -/
theorem insert_split_root_count {V : Type} (t r : BPlusTree Nat V) (p : Nat) :
    (BPlusTree.internal [p] [t, r]).count = t.count + r.count := by
  simp [BPlusTree.count]

private theorem list_map_count_set {K V : Type} :
    (xs : List (BPlusTree K V)) → {pos : Nat} → {c c' : BPlusTree K V} →
    xs[pos]? = some c →
    ((xs.set pos c').map BPlusTree.count).sum + c.count =
      (xs.map BPlusTree.count).sum + c'.count
  | [], _, _, _, h => by simp at h
  | x :: xs, 0, c, c', h => by
    simp only [List.getElem?_cons_zero, Option.some.injEq] at h
    subst h
    simp only [List.set_cons_zero, List.map_cons, List.sum_cons]
    omega
  | x :: xs, pos + 1, c, c', h => by
    simp only [List.getElem?_cons_succ] at h
    have ih := list_map_count_set xs h (c' := c')
    simp only [List.set_cons_succ, List.map_cons, List.sum_cons]
    omega

private theorem list_map_count_take_drop {K V : Type} (xs : List (BPlusTree K V)) (i : Nat) :
    ((xs.take i).map BPlusTree.count).sum + ((xs.drop i).map BPlusTree.count).sum =
      (xs.map BPlusTree.count).sum := by
  have h : (xs.take i).map BPlusTree.count ++ (xs.drop i).map BPlusTree.count =
      xs.map BPlusTree.count := by
    rw [← List.map_append, List.take_append_drop]
  rw [← List.sum_append, h]

private theorem splitInternalNodes_count_sum {K V : Type} (keys : List K)
    (children : List (BPlusTree K V)) (defaultK : K) :
    let ((_, lc), _, (_, rc)) := splitInternalNodes keys children defaultK
    (lc.map BPlusTree.count).sum + (rc.map BPlusTree.count).sum =
      (children.map BPlusTree.count).sum := by
  simp only [splitInternalNodes]
  exact list_map_count_take_drop children (keys.length / 2 + 1)

/-- Multi-level count conservation for `insertNode`: a successful insertion into any subtree
    increases the total entry count by 1 (split across both halves if the subtree splits). -/
theorem insertNode_count {V : Type} (maxKeys : Nat) :
    (t : BPlusTree Nat V) → {k : Nat} → {v : V} →
    {t' : BPlusTree Nat V} → {split : Option (Nat × BPlusTree Nat V)} →
    insertNode maxKeys t k v = some (t', split) →
    match split with
    | none => t'.count = t.count + 1
    | some (_, right) => t'.count + right.count = t.count + 1
  | .leaf entries, k, v, t', split, h => by
    simp only [insertNode] at h
    cases hent : insertEntries entries k v with
    | none => simp [hent] at h
    | some entries' =>
      have hlen := insertEntries_length hent
      simp only [hent] at h
      split_ifs at h
      · simp only [Option.some.injEq, Prod.mk.injEq] at h
        rcases h with ⟨rfl, rfl⟩
        have hsum := splitLeafEntries_length_sum entries'
        simp only [BPlusTree.count]
        omega
      · simp only [Option.some.injEq, Prod.mk.injEq] at h
        rcases h with ⟨rfl, rfl⟩
        simp only [BPlusTree.count]
        omega
  | .internal keys children, k, v, t', split, h => by
    simp only [insertNode] at h
    split at h
    · simp at h
    · rename_i child hc
      have hmem : child ∈ children := List.mem_of_getElem? hc
      split at h
      · simp at h
      · rename_i child' hrec
        simp only [Option.some.injEq, Prod.mk.injEq] at h
        rcases h with ⟨rfl, rfl⟩
        have ih := insertNode_count maxKeys child hrec
        have hset := list_map_count_set children hc (c' := child')
        simp only [BPlusTree.count] at ih ⊢
        omega
      · rename_i child' promotedKey newChild hrec
        have ih := insertNode_count maxKeys child hrec
        have hset := list_map_count_set children hc (c' := child')
        have htd := list_map_count_take_drop (children.set (findChildIdx keys k) child')
          (findChildIdx keys k + 1)
        dsimp only at ih
        split_ifs at h
        · simp only [Option.some.injEq, Prod.mk.injEq] at h
          rcases h with ⟨rfl, rfl⟩
          have hsp := splitInternalNodes_count_sum
            (keys.take (findChildIdx keys k) ++ [promotedKey] ++ keys.drop (findChildIdx keys k))
            ((children.set (findChildIdx keys k) child').take (findChildIdx keys k + 1) ++
              [newChild] ++
              (children.set (findChildIdx keys k) child').drop (findChildIdx keys k + 1)) 0
          dsimp only at hsp
          simp only [BPlusTree.count, List.map_append, List.sum_append, List.map_cons,
            List.map_nil, List.sum_cons, List.sum_nil] at hsp ⊢
          omega
        · simp only [Option.some.injEq, Prod.mk.injEq] at h
          rcases h with ⟨rfl, rfl⟩
          simp only [BPlusTree.count, List.map_append, List.sum_append, List.map_cons,
            List.map_nil, List.sum_cons, List.sum_nil]
          omega
  termination_by t => sizeOf t
  decreasing_by
    all_goals (
      simp_wf
      have hsz := List.sizeOf_lt_of_mem hmem
      omega
    )

/-- Multi-level count conservation for `insert`: a successful insertion into any tree
    increases `count` by 1. -/
theorem insert_count {V : Type} {maxKeys : Nat} {t t' : BPlusTree Nat V}
    {k : Nat} {v : V} (h : insert maxKeys t k v = .ok t') :
    t'.count = t.count + 1 := by
  simp only [insert] at h
  cases hins : insertNode maxKeys t k v with
  | none => simp [hins] at h
  | some res =>
    rcases res with ⟨t0, split⟩
    have hcnt := insertNode_count maxKeys t hins
    cases split with
    | none =>
      simp only [hins, Except.ok.injEq] at h
      subst h
      exact hcnt
    | some pr =>
      rcases pr with ⟨p, r⟩
      simp only [hins, Except.ok.injEq] at h
      subst h
      simp only [BPlusTree.count, List.map_cons, List.map_nil, List.sum_cons, List.sum_nil]
      omega

private theorem mem_set_of_mem {α : Type} :
    {xs : List α} → {pos : Nat} → {x y : α} →
    y ∈ xs.set pos x → y = x ∨ y ∈ xs
  | [], _, _, _, hy => by simp at hy
  | _ :: xs, 0, x, y, hy => by
    simp only [List.set_cons_zero, List.mem_cons] at hy
    rcases hy with rfl | hmem
    · exact Or.inl rfl
    · exact Or.inr (List.mem_cons_of_mem _ hmem)
  | hd :: xs, pos + 1, x, y, hy => by
    simp only [List.set_cons_succ, List.mem_cons] at hy
    rcases hy with rfl | hmem
    · exact Or.inr List.mem_cons_self
    · rcases mem_set_of_mem hmem with rfl | hmem'
      · exact Or.inl rfl
      · exact Or.inr (List.mem_cons_of_mem hd hmem')

private theorem length_take_append_drop_cons {α : Type} (xs : List α) (i : Nat) (x : α)
    (hi : i ≤ xs.length) :
    (xs.take i ++ [x] ++ xs.drop i).length = xs.length + 1 := by
  simp only [List.length_append, List.length_take, List.length_drop, List.length_singleton]
  omega

private theorem mem_take_append_drop_cons {α : Type} {xs : List α} {i : Nat} {x y : α}
    (hy : y ∈ xs.take i ++ [x] ++ xs.drop i) : y = x ∨ y ∈ xs := by
  simp only [List.mem_append, List.mem_singleton] at hy
  rcases hy with (hy_t | rfl) | hy_d
  · exact Or.inr (List.mem_of_mem_take hy_t)
  · exact Or.inl rfl
  · exact Or.inr (List.mem_of_mem_drop hy_d)

/-- Multi-level `ChildCountValid` preservation for `insertNode`. -/
theorem insertNode_preserves_ChildCountValid {V : Type} (maxKeys : Nat) :
    (t : BPlusTree Nat V) → {k : Nat} → {v : V} →
    {t' : BPlusTree Nat V} → {split : Option (Nat × BPlusTree Nat V)} →
    ChildCountValid t →
    insertNode maxKeys t k v = some (t', split) →
    match split with
    | none => ChildCountValid t'
    | some (_, right) => ChildCountValid t' ∧ ChildCountValid right
  | .leaf entries, k, v, t', split, _, h => by
    simp only [insertNode] at h
    cases hent : insertEntries entries k v with
    | none => simp [hent] at h
    | some entries' =>
      simp only [hent] at h
      split_ifs at h
      · simp only [Option.some.injEq, Prod.mk.injEq] at h
        rcases h with ⟨rfl, rfl⟩
        simp [ChildCountValid]
      · simp only [Option.some.injEq, Prod.mk.injEq] at h
        rcases h with ⟨rfl, rfl⟩
        simp [ChildCountValid]
  | .internal keys children, k, v, t', split, hcc, h => by
    simp only [ChildCountValid] at hcc ⊢
    rcases hcc with ⟨hlen, hcc_ch⟩
    simp only [insertNode] at h
    split at h
    · simp at h
    · rename_i child hc
      have hmem : child ∈ children := List.mem_of_getElem? hc
      have hpos_ch : findChildIdx keys k < children.length :=
        (List.getElem?_eq_some_iff.mp hc).1
      have hpos_k : findChildIdx keys k ≤ keys.length := findChildIdx_le_length keys k
      split at h
      · simp at h
      · rename_i child' hrec
        have ih := insertNode_preserves_ChildCountValid maxKeys child (hcc_ch child hmem) hrec
        simp only [Option.some.injEq, Prod.mk.injEq] at h
        rcases h with ⟨rfl, rfl⟩
        simp only [ChildCountValid]
        refine ⟨by simp [hlen], ?_⟩
        intro c hc_mem
        rcases mem_set_of_mem hc_mem with rfl | hc_old
        · exact ih
        · exact hcc_ch c hc_old
      · rename_i child' promotedKey newChild hrec
        have ih := insertNode_preserves_ChildCountValid maxKeys child (hcc_ch child hmem) hrec
        dsimp only at ih
        have hlen_k' :
            (keys.take (findChildIdx keys k) ++ [promotedKey] ++
              keys.drop (findChildIdx keys k)).length = keys.length + 1 :=
          length_take_append_drop_cons keys (findChildIdx keys k) promotedKey hpos_k
        have hlen_c' :
            ((children.set (findChildIdx keys k) child').take (findChildIdx keys k + 1) ++
              [newChild] ++
              (children.set (findChildIdx keys k) child').drop (findChildIdx keys k + 1)).length =
              children.length + 1 := by
          have hset_len : (children.set (findChildIdx keys k) child').length = children.length := by
            simp
          rw [length_take_append_drop_cons _ _ _ (by omega), hset_len]
        have hmem_c' : ∀ c ∈
            (children.set (findChildIdx keys k) child').take (findChildIdx keys k + 1) ++
              [newChild] ++
              (children.set (findChildIdx keys k) child').drop (findChildIdx keys k + 1),
            ChildCountValid c := by
          intro c hc_mem
          rcases mem_take_append_drop_cons hc_mem with rfl | hc_set
          · exact ih.2
          · rcases mem_set_of_mem hc_set with rfl | hc_old
            · exact ih.1
            · exact hcc_ch c hc_old
        split_ifs at h with hgt
        · simp only [Option.some.injEq, Prod.mk.injEq] at h
          rcases h with ⟨rfl, rfl⟩
          simp only [ChildCountValid]
          refine ⟨⟨?_, ?_⟩, ⟨?_, ?_⟩⟩
          · simp only [splitInternalNodes, List.length_take, hlen_c', hlen_k', hlen]
            omega
          · intro c hc_mem
            exact hmem_c' c (List.mem_of_mem_take hc_mem)
          · simp only [splitInternalNodes, List.length_drop, hlen_c', hlen_k', hlen]
            omega
          · intro c hc_mem
            exact hmem_c' c (List.mem_of_mem_drop hc_mem)
        · simp only [Option.some.injEq, Prod.mk.injEq] at h
          rcases h with ⟨rfl, rfl⟩
          simp only [ChildCountValid]
          exact ⟨by omega, hmem_c'⟩
  termination_by t => sizeOf t
  decreasing_by
    all_goals (
      simp_wf
      have hsz := List.sizeOf_lt_of_mem hmem
      omega
    )

/-- Multi-level `ChildCountValid` preservation for `insert`. -/
theorem insert_preserves_ChildCountValid {V : Type} {maxKeys : Nat}
    {t t' : BPlusTree Nat V} {k : Nat} {v : V}
    (hcc : ChildCountValid t) (h : insert maxKeys t k v = .ok t') :
    ChildCountValid t' := by
  simp only [insert] at h
  cases hins : insertNode maxKeys t k v with
  | none => simp [hins] at h
  | some res =>
    rcases res with ⟨t0, split⟩
    have hres := insertNode_preserves_ChildCountValid maxKeys t hcc hins
    cases split with
    | none =>
      simp only [hins, Except.ok.injEq] at h
      subst h
      exact hres
    | some pr =>
      rcases pr with ⟨p, r⟩
      simp only [hins, Except.ok.injEq] at h
      subst h
      simp only [ChildCountValid]
      refine ⟨by simp, ?_⟩
      intro c hc
      simp only [List.mem_cons, List.not_mem_nil, or_false] at hc
      rcases hc with rfl | rfl
      · exact hres.1
      · exact hres.2

/-- Multi-level `UniformDepth` preservation for `insertNode`. -/
theorem insertNode_preserves_UniformDepth {V : Type} (maxKeys : Nat) :
    (t : BPlusTree Nat V) → {d k : Nat} → {v : V} →
    {t' : BPlusTree Nat V} → {split : Option (Nat × BPlusTree Nat V)} →
    ChildCountValid t → UniformDepth t d →
    insertNode maxKeys t k v = some (t', split) →
    match split with
    | none => UniformDepth t' d
    | some (_, right) => UniformDepth t' d ∧ UniformDepth right d
  | .leaf entries, d, k, v, t', split, _, hud, h => by
    cases d with
    | zero => simp [UniformDepth] at hud
    | succ d' =>
      cases d' with
      | zero =>
        simp only [insertNode] at h
        cases hent : insertEntries entries k v with
        | none => simp [hent] at h
        | some entries' =>
          simp only [hent] at h
          split_ifs at h
          · simp only [Option.some.injEq, Prod.mk.injEq] at h
            rcases h with ⟨rfl, rfl⟩
            simp [UniformDepth]
          · simp only [Option.some.injEq, Prod.mk.injEq] at h
            rcases h with ⟨rfl, rfl⟩
            simp [UniformDepth]
      | succ _ => simp [UniformDepth] at hud
  | .internal keys children, d, k, v, t', split, hcc, hud, h => by
    cases d with
    | zero => simp [UniformDepth] at hud
    | succ d' =>
      simp only [ChildCountValid] at hcc
      simp only [UniformDepth] at hud ⊢
      rcases hcc with ⟨hlen, hcc_ch⟩
      rcases hud with ⟨hne, hud_ch⟩
      simp only [insertNode] at h
      split at h
      · simp at h
      · rename_i child hc
        have hmem : child ∈ children := List.mem_of_getElem? hc
        have hpos_ch : findChildIdx keys k < children.length :=
          (List.getElem?_eq_some_iff.mp hc).1
        have hpos_k : findChildIdx keys k ≤ keys.length := findChildIdx_le_length keys k
        split at h
        · simp at h
        · rename_i child' hrec
          have ih := insertNode_preserves_UniformDepth maxKeys child
            (hcc_ch child hmem) (hud_ch child hmem) hrec
          simp only [Option.some.injEq, Prod.mk.injEq] at h
          rcases h with ⟨rfl, rfl⟩
          refine ⟨?_, ?_⟩
          · intro hempty
            have hlen_set : (children.set (findChildIdx keys k) child').length = children.length :=
              by simp
            rw [hempty, List.length_nil] at hlen_set
            exact hne (List.length_eq_zero_iff.mp hlen_set.symm)
          · intro c hc_mem
            rcases mem_set_of_mem hc_mem with rfl | hc_old
            · exact ih
            · exact hud_ch c hc_old
        · rename_i child' promotedKey newChild hrec
          have ih := insertNode_preserves_UniformDepth maxKeys child
            (hcc_ch child hmem) (hud_ch child hmem) hrec
          dsimp only at ih
          have hlen_k' :
              (keys.take (findChildIdx keys k) ++ [promotedKey] ++
                keys.drop (findChildIdx keys k)).length = keys.length + 1 :=
            length_take_append_drop_cons keys (findChildIdx keys k) promotedKey hpos_k
          have hlen_c' :
              ((children.set (findChildIdx keys k) child').take (findChildIdx keys k + 1) ++
                [newChild] ++
                (children.set (findChildIdx keys k) child').drop (findChildIdx keys k + 1)).length =
                children.length + 1 := by
            have hset_len : (children.set (findChildIdx keys k) child').length = children.length :=
              by simp
            rw [length_take_append_drop_cons _ _ _ (by omega), hset_len]
          have hmem_c' : ∀ c ∈
              (children.set (findChildIdx keys k) child').take (findChildIdx keys k + 1) ++
                [newChild] ++
                (children.set (findChildIdx keys k) child').drop (findChildIdx keys k + 1),
              UniformDepth c d' := by
            intro c hc_mem
            rcases mem_take_append_drop_cons hc_mem with rfl | hc_set
            · exact ih.2
            · rcases mem_set_of_mem hc_set with rfl | hc_old
              · exact ih.1
              · exact hud_ch c hc_old
          split_ifs at h with hgt
          · simp only [Option.some.injEq, Prod.mk.injEq] at h
            rcases h with ⟨rfl, rfl⟩
            refine ⟨⟨?_, ?_⟩, ⟨?_, ?_⟩⟩
            · intro hempty
              have hlen_lc :
                  ((splitInternalNodes
                    (keys.take (findChildIdx keys k) ++ [promotedKey] ++
                      keys.drop (findChildIdx keys k))
                    ((children.set (findChildIdx keys k) child').take (findChildIdx keys k + 1) ++
                      [newChild] ++
                      (children.set (findChildIdx keys k) child').drop
                        (findChildIdx keys k + 1)) 0).1.2).length = 0 := by
                rw [hempty]; rfl
              simp only [splitInternalNodes, List.length_take, hlen_c', hlen_k', hlen] at hlen_lc
              omega
            · intro c hc_mem
              exact hmem_c' c (List.mem_of_mem_take hc_mem)
            · intro hempty
              have hlen_rc :
                  ((splitInternalNodes
                    (keys.take (findChildIdx keys k) ++ [promotedKey] ++
                      keys.drop (findChildIdx keys k))
                    ((children.set (findChildIdx keys k) child').take (findChildIdx keys k + 1) ++
                      [newChild] ++
                      (children.set (findChildIdx keys k) child').drop
                        (findChildIdx keys k + 1)) 0).2.2.2).length = 0 := by
                rw [hempty]; rfl
              simp only [splitInternalNodes, List.length_drop, hlen_c', hlen_k', hlen] at hlen_rc
              omega
            · intro c hc_mem
              exact hmem_c' c (List.mem_of_mem_drop hc_mem)
          · simp only [Option.some.injEq, Prod.mk.injEq] at h
            rcases h with ⟨rfl, rfl⟩
            refine ⟨?_, hmem_c'⟩
            intro hempty
            have hlen_zero :
                ((children.set (findChildIdx keys k) child').take (findChildIdx keys k + 1) ++
                  [newChild] ++
                  (children.set (findChildIdx keys k) child').drop
                    (findChildIdx keys k + 1)).length = 0 := by
              simp [hempty]
            omega
  termination_by t => sizeOf t
  decreasing_by
    all_goals (
      simp_wf
      have hsz := List.sizeOf_lt_of_mem hmem
      omega
    )

/-- Multi-level `UniformDepth` preservation for `insert`. -/
theorem insert_preserves_UniformDepth {V : Type} {maxKeys : Nat}
    {t t' : BPlusTree Nat V} {k : Nat} {v : V}
    (hcc : ChildCountValid t) (hud : UniformDepth t t.height)
    (h : insert maxKeys t k v = .ok t') :
    UniformDepth t' t'.height := by
  simp only [insert] at h
  cases hins : insertNode maxKeys t k v with
  | none => simp [hins] at h
  | some res =>
    rcases res with ⟨t0, split⟩
    have hres := insertNode_preserves_UniformDepth maxKeys t hcc hud hins
    cases split with
    | none =>
      simp only [hins, Except.ok.injEq] at h
      subst h
      have hht : t0.height = t.height := uniformDepth_eq_height hres
      rw [hht]
      exact hres
    | some pr =>
      rcases pr with ⟨p, r⟩
      simp only [hins, Except.ok.injEq] at h
      subst h
      have hud_root : UniformDepth (BPlusTree.internal [p] [t0, r]) (t.height + 1) := by
        simp only [UniformDepth]
        refine ⟨by simp, ?_⟩
        intro c hc
        simp only [List.mem_cons, List.not_mem_nil, or_false] at hc
        rcases hc with rfl | rfl
        · exact hres.1
        · exact hres.2
      have hht : (BPlusTree.internal [p] [t0, r]).height = t.height + 1 :=
        uniformDepth_eq_height hud_root
      rw [hht]
      exact hud_root

/-- Multi-level `NonRootOccupancy` preservation for `insertNode`. -/
theorem insertNode_preserves_NonRootOccupancy {V : Type} {maxKeys : Nat}
    (hcap : 3 ≤ maxKeys) :
    (t : BPlusTree Nat V) → {k : Nat} → {v : V} →
    {t' : BPlusTree Nat V} → {split : Option (Nat × BPlusTree Nat V)} →
    NonRootOccupancy maxKeys t →
    insertNode maxKeys t k v = some (t', split) →
    match split with
    | none => NonRootOccupancy maxKeys t'
    | some (_, right) => NonRootOccupancy maxKeys t' ∧ NonRootOccupancy maxKeys right
  | .leaf entries, k, v, t', split, hno, h => by
    simp only [NonRootOccupancy] at hno
    rcases hno with ⟨hmin, hmax⟩
    simp only [insertNode] at h
    cases hent : insertEntries entries k v with
    | none => simp [hent] at h
    | some entries' =>
      have hlen := insertEntries_length hent
      simp only [hent] at h
      split_ifs at h with hgt
      · simp only [Option.some.injEq, Prod.mk.injEq] at h
        rcases h with ⟨rfl, rfl⟩
        have hl := splitLeaf_left_bounds (V := V) hcap (entries := entries') (by omega)
        have hr := splitLeaf_right_bounds (V := V) hcap (entries := entries') (by omega)
        dsimp only at hl hr
        simp only [NonRootOccupancy]
        exact ⟨hl, hr⟩
      · simp only [Option.some.injEq, Prod.mk.injEq] at h
        rcases h with ⟨rfl, rfl⟩
        simp only [NonRootOccupancy]
        omega
  | .internal keys children, k, v, t', split, hno, h => by
    simp only [NonRootOccupancy] at hno
    rcases hno with ⟨hmin, hmax, hno_ch⟩
    simp only [insertNode] at h
    split at h
    · simp at h
    · rename_i child hc
      have hmem : child ∈ children := List.mem_of_getElem? hc
      have hpos_k : findChildIdx keys k ≤ keys.length := findChildIdx_le_length keys k
      split at h
      · simp at h
      · rename_i child' hrec
        have ih := insertNode_preserves_NonRootOccupancy hcap child (hno_ch child hmem) hrec
        simp only [Option.some.injEq, Prod.mk.injEq] at h
        rcases h with ⟨rfl, rfl⟩
        simp only [NonRootOccupancy]
        refine ⟨hmin, hmax, ?_⟩
        intro c hc_mem
        rcases mem_set_of_mem hc_mem with rfl | hc_old
        · exact ih
        · exact hno_ch c hc_old
      · rename_i child' promotedKey newChild hrec
        have ih := insertNode_preserves_NonRootOccupancy hcap child (hno_ch child hmem) hrec
        dsimp only at ih
        have hlen_k' :
            (keys.take (findChildIdx keys k) ++ [promotedKey] ++
              keys.drop (findChildIdx keys k)).length = keys.length + 1 :=
          length_take_append_drop_cons keys (findChildIdx keys k) promotedKey hpos_k
        have hmem_c' : ∀ c ∈
            (children.set (findChildIdx keys k) child').take (findChildIdx keys k + 1) ++
              [newChild] ++
              (children.set (findChildIdx keys k) child').drop (findChildIdx keys k + 1),
            NonRootOccupancy maxKeys c := by
          intro c hc_mem
          rcases mem_take_append_drop_cons hc_mem with rfl | hc_set
          · exact ih.2
          · rcases mem_set_of_mem hc_set with rfl | hc_old
            · exact ih.1
            · exact hno_ch c hc_old
        split_ifs at h with hgt
        · simp only [Option.some.injEq, Prod.mk.injEq] at h
          rcases h with ⟨rfl, rfl⟩
          have hl := splitInternal_left_bounds (V := V) 0 hcap (keys := keys.take (findChildIdx keys k) ++
            [promotedKey] ++ keys.drop (findChildIdx keys k))
            (children := (children.set (findChildIdx keys k) child').take
              (findChildIdx keys k + 1) ++ [newChild] ++
              (children.set (findChildIdx keys k) child').drop (findChildIdx keys k + 1))
            (by omega)
          have hr := splitInternal_right_bounds (V := V) 0 hcap (keys := keys.take (findChildIdx keys k) ++
            [promotedKey] ++ keys.drop (findChildIdx keys k))
            (children := (children.set (findChildIdx keys k) child').take
              (findChildIdx keys k + 1) ++ [newChild] ++
              (children.set (findChildIdx keys k) child').drop (findChildIdx keys k + 1))
            (by omega)
          dsimp only at hl hr
          simp only [NonRootOccupancy]
          refine ⟨⟨hl.1, hl.2, ?_⟩, ⟨hr.1, hr.2, ?_⟩⟩
          · intro c hc_mem
            exact hmem_c' c (List.mem_of_mem_take hc_mem)
          · intro c hc_mem
            exact hmem_c' c (List.mem_of_mem_drop hc_mem)
        · simp only [Option.some.injEq, Prod.mk.injEq] at h
          rcases h with ⟨rfl, rfl⟩
          simp only [NonRootOccupancy]
          refine ⟨by omega, by omega, hmem_c'⟩
  termination_by t => sizeOf t
  decreasing_by
    all_goals (
      simp_wf
      have hsz := List.sizeOf_lt_of_mem hmem
      omega
    )

/-- Multi-level `RootOccupancy` preservation for `insert`. -/
theorem insert_preserves_RootOccupancy {V : Type} {maxKeys : Nat}
    (hcap : 3 ≤ maxKeys) {t t' : BPlusTree Nat V} {k : Nat} {v : V}
    (hro : RootOccupancy maxKeys t)
    (h : insert maxKeys t k v = .ok t') :
    RootOccupancy maxKeys t' := by
  cases t with
  | leaf entries =>
    simp only [RootOccupancy] at hro
    simp only [insert, insertNode] at h
    cases hent : insertEntries entries k v with
    | none => simp [hent] at h
    | some entries' =>
      have hlen := insertEntries_length hent
      simp only [hent] at h
      split_ifs at h with hgt
      · simp only [Except.ok.injEq] at h
        subst h
        have hl := splitLeaf_left_bounds (V := V) hcap (entries := entries') (by omega)
        have hr := splitLeaf_right_bounds (V := V) hcap (entries := entries') (by omega)
        dsimp only at hl hr
        simp only [RootOccupancy]
        refine ⟨by simp, by simp; omega, ?_⟩
        intro c hc
        simp only [List.mem_cons, List.not_mem_nil, or_false] at hc
        rcases hc with rfl | rfl
        · simpa [NonRootOccupancy] using hl
        · simpa [NonRootOccupancy] using hr
      · simp only [Except.ok.injEq] at h
        subst h
        simp only [RootOccupancy]
        omega
  | internal keys children =>
    simp only [RootOccupancy] at hro
    rcases hro with ⟨hmin, hmax, hno_ch⟩
    simp only [insert] at h
    cases hins : insertNode maxKeys (.internal keys children) k v with
    | none => simp [hins] at h
    | some res =>
      rcases res with ⟨t0, split⟩
      simp only [hins] at h
      simp only [insertNode] at hins
      split at hins
      · cases hins
      · rename_i child hc
        have hmem : child ∈ children := List.mem_of_getElem? hc
        have hpos_k : findChildIdx keys k ≤ keys.length := findChildIdx_le_length keys k
        split at hins
        · cases hins
        · rename_i child' hrec
          have hchild' := insertNode_preserves_NonRootOccupancy hcap child (hno_ch child hmem) hrec
          simp only [Option.some.injEq, Prod.mk.injEq] at hins
          rcases hins with ⟨rfl, rfl⟩
          simp only [Except.ok.injEq] at h
          subst h
          simp only [RootOccupancy]
          refine ⟨hmin, hmax, ?_⟩
          intro c hc_mem
          rcases mem_set_of_mem hc_mem with rfl | hc_old
          · exact hchild'
          · exact hno_ch c hc_old
        · rename_i child' promotedKey newChild hrec
          have hchild' := insertNode_preserves_NonRootOccupancy hcap child (hno_ch child hmem) hrec
          dsimp only at hchild'
          have hlen_k' :
              (keys.take (findChildIdx keys k) ++ [promotedKey] ++
                keys.drop (findChildIdx keys k)).length = keys.length + 1 :=
            length_take_append_drop_cons keys (findChildIdx keys k) promotedKey hpos_k
          have hmem_c' : ∀ c ∈
              (children.set (findChildIdx keys k) child').take (findChildIdx keys k + 1) ++
                [newChild] ++
                (children.set (findChildIdx keys k) child').drop (findChildIdx keys k + 1),
              NonRootOccupancy maxKeys c := by
            intro c hc_mem
            rcases mem_take_append_drop_cons hc_mem with rfl | hc_set
            · exact hchild'.2
            · rcases mem_set_of_mem hc_set with rfl | hc_old
              · exact hchild'.1
              · exact hno_ch c hc_old
          split_ifs at hins with hgt
          · simp only [Option.some.injEq, Prod.mk.injEq] at hins
            rcases hins with ⟨rfl, rfl⟩
            simp only [Except.ok.injEq] at h
            subst h
            have hl := splitInternal_left_bounds (V := V) 0 hcap (keys := keys.take (findChildIdx keys k) ++
              [promotedKey] ++ keys.drop (findChildIdx keys k))
              (children := (children.set (findChildIdx keys k) child').take
                (findChildIdx keys k + 1) ++ [newChild] ++
                (children.set (findChildIdx keys k) child').drop (findChildIdx keys k + 1))
              (by omega)
            have hr := splitInternal_right_bounds (V := V) 0 hcap (keys := keys.take (findChildIdx keys k) ++
              [promotedKey] ++ keys.drop (findChildIdx keys k))
              (children := (children.set (findChildIdx keys k) child').take
                (findChildIdx keys k + 1) ++ [newChild] ++
                (children.set (findChildIdx keys k) child').drop (findChildIdx keys k + 1))
              (by omega)
            dsimp only at hl hr
            simp only [RootOccupancy]
            refine ⟨by simp, by simp; omega, ?_⟩
            intro c hc_root
            simp only [List.mem_cons, List.not_mem_nil, or_false] at hc_root
            rcases hc_root with rfl | rfl
            · simp only [NonRootOccupancy]
              refine ⟨hl.1, hl.2, ?_⟩
              intro x hx
              exact hmem_c' x (List.mem_of_mem_take hx)
            · simp only [NonRootOccupancy]
              refine ⟨hr.1, hr.2, ?_⟩
              intro x hx
              exact hmem_c' x (List.mem_of_mem_drop hx)
          · simp only [Option.some.injEq, Prod.mk.injEq] at hins
            rcases hins with ⟨rfl, rfl⟩
            simp only [Except.ok.injEq] at h
            subst h
            simp only [RootOccupancy]
            refine ⟨by omega, by omega, hmem_c'⟩

/-- Splitting an internal node preserves the concatenated `toList` of both halves. -/
theorem splitInternalNodes_toList_append {K V : Type} (keys : List K)
    (children : List (BPlusTree K V)) (defaultK : K) :
    let ((lk, lc), _, (rk, rc)) := splitInternalNodes keys children defaultK
    (BPlusTree.internal lk lc).toList ++ (BPlusTree.internal rk rc).toList =
      (BPlusTree.internal keys children).toList := by
  simp only [toList_internal_eq_flatMap]
  rw [← List.flatMap_append]
  have h := splitInternal_children_append keys children defaultK
  dsimp only at h
  rw [h]

private theorem mem_flatMap_set_iff {K V : Type} {P : K × V → Prop} :
    (xs : List (BPlusTree K V)) → {pos : Nat} → {c c' : BPlusTree K V} →
    xs[pos]? = some c →
    (∀ x : K × V, x ∈ c'.toList ↔ P x ∨ x ∈ c.toList) →
    ∀ x : K × V,
      x ∈ (xs.set pos c').flatMap BPlusTree.toList ↔ P x ∨ x ∈ xs.flatMap BPlusTree.toList
  | [], _, _, _, h, _, _ => by simp at h
  | y :: ys, 0, c, c', h, hc', x => by
    simp only [List.getElem?_cons_zero, Option.some.injEq] at h
    subst h
    simp only [List.set_cons_zero, List.flatMap_cons, List.mem_append, hc']
    tauto
  | y :: ys, pos + 1, c, c', h, hc', x => by
    simp only [List.getElem?_cons_succ] at h
    have ih := mem_flatMap_set_iff ys h hc' x
    simp only [List.set_cons_succ, List.flatMap_cons, List.mem_append] at ih ⊢
    tauto

private theorem mem_flatMap_set_or_iff {K V : Type} {P : K × V → Prop} :
    (xs : List (BPlusTree K V)) → {pos : Nat} → {c c1 c2 : BPlusTree K V} →
    xs[pos]? = some c →
    (∀ x : K × V, x ∈ c1.toList ∨ x ∈ c2.toList ↔ P x ∨ x ∈ c.toList) →
    ∀ x : K × V,
      x ∈ c2.toList ∨ x ∈ (xs.set pos c1).flatMap BPlusTree.toList ↔
        P x ∨ x ∈ xs.flatMap BPlusTree.toList
  | [], _, _, _, _, h, _, _ => by simp at h
  | y :: ys, 0, c, c1, c2, h, hc', x => by
    simp only [List.getElem?_cons_zero, Option.some.injEq] at h
    subst h
    simp only [List.set_cons_zero, List.flatMap_cons, List.mem_append]
    have hcx := hc' x
    tauto
  | y :: ys, pos + 1, c, c1, c2, h, hc', x => by
    simp only [List.getElem?_cons_succ] at h
    have ih := mem_flatMap_set_or_iff ys h hc' x
    simp only [List.set_cons_succ, List.flatMap_cons, List.mem_append] at ih ⊢
    tauto

private theorem flatMap_take_append_drop_cons {K V : Type} (xs : List (BPlusTree K V)) (i : Nat)
    (c2 : BPlusTree K V) (x : K × V) :
    x ∈ (xs.take i ++ [c2] ++ xs.drop i).flatMap BPlusTree.toList ↔
      x ∈ c2.toList ∨ x ∈ xs.flatMap BPlusTree.toList := by
  have happ : (xs.take i).flatMap BPlusTree.toList ++ (xs.drop i).flatMap BPlusTree.toList =
      xs.flatMap BPlusTree.toList := by
    rw [← List.flatMap_append, List.take_append_drop]
  simp only [List.flatMap_append, List.flatMap_cons, List.flatMap_nil, List.append_nil,
    List.mem_append]
  rw [← happ]
  simp only [List.mem_append]
  tauto

/-- Multi-level `toList` membership refinement for `insertNode`. -/
theorem insertNode_mem_toList_iff {V : Type} (maxKeys : Nat) :
    (t : BPlusTree Nat V) → {k : Nat} → {v : V} →
    {t' : BPlusTree Nat V} → {split : Option (Nat × BPlusTree Nat V)} →
    insertNode maxKeys t k v = some (t', split) →
    ∀ p : Nat × V,
      (match split with
       | none => p ∈ t'.toList
       | some (_, right) => p ∈ t'.toList ∨ p ∈ right.toList) ↔
      (p = (k, v) ∨ p ∈ t.toList)
  | .leaf entries, k, v, t', split, h, p => by
    simp only [insertNode] at h
    cases hent : insertEntries entries k v with
    | none => simp [hent] at h
    | some entries' =>
      have hmem := mem_insertEntries_iff hent p
      simp only [hent] at h
      split_ifs at h
      · simp only [Option.some.injEq, Prod.mk.injEq] at h
        rcases h with ⟨rfl, rfl⟩
        have happ := splitLeafEntries_append entries'
        dsimp only at happ
        simp only [BPlusTree.toList, BPlusTree.leaves, List.flatten_cons, List.flatten_nil,
          List.append_nil] at hmem ⊢
        have hmem_app : p ∈ (splitLeafEntries entries').1 ∨
            p ∈ (splitLeafEntries entries').2.2 ↔ p ∈ entries' := by
          rw [← List.mem_append, happ]
        exact hmem_app.trans hmem
      · simp only [Option.some.injEq, Prod.mk.injEq] at h
        rcases h with ⟨rfl, rfl⟩
        simpa [BPlusTree.toList, BPlusTree.leaves] using hmem
  | .internal keys children, k, v, t', split, h, p => by
    simp only [insertNode] at h
    split at h
    · simp at h
    · rename_i child hc
      have hmem : child ∈ children := List.mem_of_getElem? hc
      split at h
      · simp at h
      · rename_i child' hrec
        have ih := insertNode_mem_toList_iff maxKeys child hrec
        simp only [Option.some.injEq, Prod.mk.injEq] at h
        rcases h with ⟨rfl, rfl⟩
        simp only [toList_internal_eq_flatMap]
        exact mem_flatMap_set_iff children hc ih p
      · rename_i child' promotedKey newChild hrec
        have ih := insertNode_mem_toList_iff maxKeys child hrec
        dsimp only at ih
        have hchildren' : ∀ x : Nat × V,
            x ∈ ((children.set (findChildIdx keys k) child').take (findChildIdx keys k + 1) ++
              [newChild] ++
              (children.set (findChildIdx keys k) child').drop (findChildIdx keys k + 1)).flatMap
                BPlusTree.toList ↔
            x = (k, v) ∨ x ∈ children.flatMap BPlusTree.toList := by
          intro x
          rw [flatMap_take_append_drop_cons]
          exact mem_flatMap_set_or_iff children hc ih x
        split_ifs at h
        · simp only [Option.some.injEq, Prod.mk.injEq] at h
          rcases h with ⟨rfl, rfl⟩
          have happ := splitInternalNodes_toList_append
            (keys.take (findChildIdx keys k) ++ [promotedKey] ++ keys.drop (findChildIdx keys k))
            ((children.set (findChildIdx keys k) child').take (findChildIdx keys k + 1) ++
              [newChild] ++
              (children.set (findChildIdx keys k) child').drop (findChildIdx keys k + 1)) 0
          dsimp only at happ ⊢
          rw [← List.mem_append, happ, toList_internal_eq_flatMap, toList_internal_eq_flatMap]
          exact hchildren' p
        · simp only [Option.some.injEq, Prod.mk.injEq] at h
          rcases h with ⟨rfl, rfl⟩
          simp only [toList_internal_eq_flatMap]
          exact hchildren' p
  termination_by t => sizeOf t
  decreasing_by
    all_goals (
      simp_wf
      have hsz := List.sizeOf_lt_of_mem hmem
      omega
    )

/-- Multi-level `toList` membership refinement for `insert`: `p ∈ t'.toList ↔ p = (k, v) ∨ p ∈ t.toList`. -/
theorem insert_mem_toList_iff {V : Type} {maxKeys : Nat} {t t' : BPlusTree Nat V}
    {k : Nat} {v : V} (h : insert maxKeys t k v = .ok t') (p : Nat × V) :
    p ∈ t'.toList ↔ p = (k, v) ∨ p ∈ t.toList := by
  simp only [insert] at h
  cases hins : insertNode maxKeys t k v with
  | none => simp [hins] at h
  | some res =>
    rcases res with ⟨t0, split⟩
    have hmem := insertNode_mem_toList_iff maxKeys t hins p
    cases split with
    | none =>
      simp only [hins, Except.ok.injEq] at h
      subst h
      exact hmem
    | some pr =>
      rcases pr with ⟨upKey, right⟩
      simp only [hins, Except.ok.injEq] at h
      subst h
      simp only [toList_internal_eq_flatMap, List.flatMap_cons, List.flatMap_nil,
        List.append_nil, List.mem_append]
      exact hmem

end Bptree

end
