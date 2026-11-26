import Bptree

open Bptree

private def nextRand (state : UInt64) : UInt64 :=
  state * 6364136223846793005 + 1442695040888963407

private def formatStatus : Status → String
  | .ok => "OK"
  | .duplicateKey => "DUPLICATE_KEY"
  | .keyNotFound => "KEY_NOT_FOUND"
  | .allocationFailure => "ALLOCATION_FAILURE"
  | .invalidArgument => "INVALID_ARGUMENT"
  | .internalError => "INTERNAL_ERROR"

private def runStep (maxKeys : Nat) (tree : BPlusTree Nat Nat) (stepIdx k v : Nat) :
    IO (BPlusTree Nat Nat) := do
  match insert maxKeys tree k v with
  | .ok t' =>
    if !checkInvariants maxKeys t' then
      throw (IO.userError s!"Lean invariant check failed after PUT {k}")
    IO.println s!"STEP {stepIdx} PUT {k} {v} -> OK count={t'.count} height={t'.height} nodes={t'.nodeCount} tree={t'.serialize}"
    return t'
  | .error st =>
    if !checkInvariants maxKeys tree then
      throw (IO.userError s!"Lean invariant check failed after failed PUT {k}")
    IO.println s!"STEP {stepIdx} PUT {k} {v} -> {formatStatus st} count={tree.count} height={tree.height} nodes={tree.nodeCount} tree={tree.serialize}"
    return tree

private def runRemoveStep (maxKeys : Nat) (tree : BPlusTree Nat Nat) (stepIdx k : Nat) :
    IO (BPlusTree Nat Nat) := do
  match remove maxKeys tree k with
  | .ok t' =>
    if !checkInvariants maxKeys t' then
      throw (IO.userError s!"Lean invariant check failed after DEL {k}")
    IO.println s!"STEP {stepIdx} DEL {k} -> OK count={t'.count} height={t'.height} nodes={t'.nodeCount} tree={t'.serialize}"
    return t'
  | .error st =>
    if !checkInvariants maxKeys tree then
      throw (IO.userError s!"Lean invariant check failed after failed DEL {k}")
    IO.println s!"STEP {stepIdx} DEL {k} -> {formatStatus st} count={tree.count} height={tree.height} nodes={tree.nodeCount} tree={tree.serialize}"
    return tree

private def runClearStep (maxKeys : Nat) (tree : BPlusTree Nat Nat) (stepIdx : Nat) :
    IO (BPlusTree Nat Nat) := do
  let t' := clear Nat Nat tree
  if !checkInvariants maxKeys t' then
    throw (IO.userError "Lean invariant check failed after CLEAR")
  IO.println s!"STEP {stepIdx} CLEAR -> OK count={t'.count} height={t'.height} nodes={t'.nodeCount} tree={t'.serialize}"
  return t'

private def runQueries (tree : BPlusTree Nat Nat) : IO Unit := do
  let iterItems := (iterList tree).map (fun p => s!"{p.1}:{p.2}")
  IO.println s!"QUERY ITER -> [{String.intercalate "," iterItems}]"
  for i in List.range 10 do
    let k := i * 6
    match lowerBound tree k with
    | some (k', v') => IO.println s!"QUERY LB {k} -> {k'}:{v'}"
    | none => IO.println s!"QUERY LB {k} -> END"
    match upperBound tree k with
    | some (k', v') => IO.println s!"QUERY UB {k} -> {k'}:{v'}"
    | none => IO.println s!"QUERY UB {k} -> END"
  for i in List.range 20 do
    let k := i * 3 + 1
    match iterFind tree k with
    | some (k', v') => IO.println s!"QUERY FIND {k} -> {k'}:{v'}"
    | none => IO.println s!"QUERY FIND {k} -> END"
    match lookup tree k with
    | some v => IO.println s!"QUERY GET {k} -> FOUND {v}"
    | none => IO.println s!"QUERY GET {k} -> NOT_FOUND"
  for i in List.range 10 do
    let lo := i * 5 + 1
    let hi := lo + 12
    let vals := (rangeQuery tree lo hi).map toString
    IO.println s!"QUERY RANGE {lo} {hi} -> [{String.intercalate "," vals}]"

def main : IO Unit := do
  let orders := [3, 4, 5, 6, 7, 8, 12]
  for maxKeys in orders do
    -- Workload 1: Sequential + duplicates
    IO.println s!"WORKLOAD sequential max_keys={maxKeys}"
    let mut t1 : BPlusTree Nat Nat := empty Nat Nat
    let mut step := 0
    for i in List.range 40 do
      let k := i + 1
      t1 ← runStep maxKeys t1 step k (k * 10)
      step := step + 1
    for i in List.range 10 do
      let k := i + 1
      t1 ← runStep maxKeys t1 step k (k * 10 + 1)
      step := step + 1
    runQueries t1
    for i in List.range 20 do
      let k := (i + 1) * 2
      t1 ← runRemoveStep maxKeys t1 step k
      step := step + 1
    for i in List.range 14 do
      let k := i * 3 + 1
      t1 ← runRemoveStep maxKeys t1 step k
      step := step + 1
    for i in List.range 40 do
      let k := i + 1
      t1 ← runRemoveStep maxKeys t1 step k
      step := step + 1
    runQueries t1

    -- Workload 2: Reverse
    IO.println s!"WORKLOAD reverse max_keys={maxKeys}"
    let mut t2 : BPlusTree Nat Nat := empty Nat Nat
    step := 0
    for i in List.range 40 do
      let k := 40 - i
      t2 ← runStep maxKeys t2 step k (k * 10)
      step := step + 1
    runQueries t2
    for i in List.range 40 do
      let k := 40 - i
      t2 ← runRemoveStep maxKeys t2 step k
      step := step + 1
    runQueries t2

    -- Workload 3: Pseudo-random with collisions
    IO.println s!"WORKLOAD random max_keys={maxKeys}"
    let mut t3 : BPlusTree Nat Nat := empty Nat Nat
    let mut rng : UInt64 := 42
    for s in List.range 100 do
      rng := nextRand rng
      let k := ((rng >>> 16) % 60).toNat + 1
      let v := k * 100 + s
      t3 ← runStep maxKeys t3 s k v
    runQueries t3
    for s in List.range 80 do
      rng := nextRand rng
      let k := ((rng >>> 16) % 60).toNat + 1
      t3 ← runRemoveStep maxKeys t3 (100 + s) k
    runQueries t3

    -- Workload 4: Interleaved PUT/DEL + CLEAR + post-CLEAR wave
    IO.println s!"WORKLOAD interleaved_clear max_keys={maxKeys}"
    let mut t4 : BPlusTree Nat Nat := empty Nat Nat
    let mut rng4 : UInt64 := 1337
    for s in List.range 200 do
      rng4 := nextRand rng4
      let op := ((rng4 >>> 8) % 5).toNat
      rng4 := nextRand rng4
      let k := ((rng4 >>> 16) % 50).toNat + 1
      if op < 3 then
        t4 ← runStep maxKeys t4 s k (k * 100 + s)
      else
        t4 ← runRemoveStep maxKeys t4 s k
    runQueries t4
    t4 ← runClearStep maxKeys t4 200
    runQueries t4
    for s in List.range 40 do
      rng4 := nextRand rng4
      let op := ((rng4 >>> 8) % 5).toNat
      rng4 := nextRand rng4
      let k := ((rng4 >>> 16) % 50).toNat + 1
      if op < 3 then
        t4 ← runStep maxKeys t4 (201 + s) k (k * 100 + s)
      else
        t4 ← runRemoveStep maxKeys t4 (201 + s) k
    runQueries t4
