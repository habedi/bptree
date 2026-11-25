import Bptree

open Bptree

def main : IO Unit := do
  let maxKeys := 3
  let keys := [10, 20, 5, 15, 25, 30, 3, 7]
  let mut tree : BPlusTree Nat Nat := empty Nat Nat
  for k in keys do
    match insert maxKeys tree k (k * 10) with
    | .ok t' => tree := t'
    | .error e =>
      IO.println s!"Insert {k} failed: {repr e}"
  IO.println s!"Inserted {keys.length} keys (maxKeys={maxKeys})"
  IO.println s!"count={tree.count}, height={tree.height}, nodeCount={tree.nodeCount}"
  IO.println s!"range [7, 25] = {rangeQuery tree 7 25}"
