import Lake
open Lake DSL

package «bptree-lean» where
  version := v!"0.1.0"
  keywords := #["lean4", "bptree", "verification", "mathlib"]
  leanOptions := #[
    ⟨`pp.unicode.fun, true⟩,
    ⟨`autoImplicit, false⟩,
    ⟨`relaxedAutoImplicit, false⟩
  ]

require "leanprover-community" / "mathlib" @ "git#v4.35.0"

@[default_target]
lean_lib «Bptree» where
  -- Core B+ tree formalization library

@[test_driver]
lean_lib «Tests» where
  -- Test suite module

lean_exe «bptree-lean» where
  root := `Main
