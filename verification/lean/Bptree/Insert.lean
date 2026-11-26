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

end Bptree

end
