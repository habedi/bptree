# AGENTS.md

This file provides guidance to coding agents collaborating on this repository.

## Mission

Bptree is a lightweight, single-header B+ tree implementation in C.
It provides an in-memory ordered map where keys are stored in sorted order and values can be any type.
The library is designed to be embedded in other C projects by including a single header file.

Priorities, in order:

1. Correctness of tree operations (insertion, deletion, search, and range queries) and structural invariants.
2. Minimal public API surface for use as a drop-in library from other C projects.
3. Zero external dependencies and maintainable, well-tested code.
4. Memory safety: no leaks, no undefined behavior, no buffer overflows.

## Core Rules

- Use English for code, comments, docs, and tests.
- Prefer small, focused changes over large refactoring.
- Add comments only when they clarify non-obvious behavior.
- Do not add features, error handling, or abstractions beyond what is needed for the current task.
- Keep the project dependency-free: no external C libraries unless explicitly agreed. The only dependencies are the C11 standard library headers.

## Writing Style

- Use Oxford commas in inline lists: "a, b, and c" not "a, b, c".
- Do not use em dashes. Restructure the sentence, or use a colon or semicolon instead.
- Avoid colorful adjectives and adverbs. Write "B+ tree" not "lightweight B+ tree", "range query" not "efficient range query".
- Use noun phrases for checklist items, not imperative verbs. Write "node merge logic" not "merge nodes".
- Headings in Markdown files must be in the title case: "Build from Source" not "Build from source". Minor words (a, an, the, and, but, or, for, in,
  on, at, to, by, of, is, are, was, were, be) stay lowercase unless they are the first word.

## Repository Layout

- `include/bptree.h`: Single-header library. Public API declarations are at the top; the implementation is guarded by
  `#ifdef BPTREE_IMPLEMENTATION`. Internal types (e.g., `bptree_node`) are defined only inside that guard.
- `test/test_bptree.c`: Unit test suite using a custom test harness (`ASSERT`, `RUN_TEST` macros).
- `test/bench_bptree.c`: Performance benchmarks for insertions, searches, deletions, iteration, and range queries.
- `test/example.c`: Example usage demonstrating tree creation, CRUD operations, range queries, and custom comparators.
- `test/extras/`: Additional test files for specific scenarios (merge operations, bug reproductions, string keys).
- `.github/workflows/`: CI workflows (`tests.yml` for unit tests and coverage, `lints.yml` for static analysis, `benches.yml`
  for benchmarks).
- `Makefile`: GNU Make build automation with targets for building, testing, linting, formatting, profiling, and Zig build
  integration.
- `build.zig` / `build.zig.zon`: Zig build system configuration (requires Zig 0.16.0). Compiles the same C source files as the
  Makefile.
- `.clang-format`: Code formatting rules (Google base style, 4-space indent, 100-column limit).
- `Doxyfile`: Doxygen configuration for API documentation generation.

## Architecture

### Single-Header Design

The entire library lives in `include/bptree.h`. Users include the header normally for declarations, and define
`BPTREE_IMPLEMENTATION` in exactly one translation unit to pull in the implementation. This keeps integration simple: copy one
file, add two lines of code.

The public section (before the `#ifdef BPTREE_IMPLEMENTATION` guard) includes only `<stdbool.h>` and `<stdint.h>`.
Implementation-only headers (`assert.h`, `stdalign.h`, `stdarg.h`, `stdio.h`, `stdlib.h`, `string.h`, `time.h`) are included
inside the guard to avoid polluting the consumer's namespace. `bptree_node` is an opaque type in the public section (forward
declaration only); its full definition is inside the implementation guard.

### Key and Value Generics

Key and value types are configured at compile time via preprocessor macros (`BPTREE_NUMERIC_TYPE`,
`BPTREE_KEY_TYPE_STRING`, `BPTREE_KEY_SIZE`, `BPTREE_VALUE_TYPE`). Defaults are `int64_t` keys and `void *` values. The library
provides default comparators for both numeric and string key types; users can supply custom comparators via `bptree_create`.

### Tree Operations

- **Insertion** (`bptree_put`): inserts a key-value pair, splitting nodes as needed. Duplicate keys are rejected.
- **Search** (`bptree_get`, `bptree_contains`): traverses internal nodes to find the correct leaf.
- **Deletion** (`bptree_remove`): removes a key-value pair and rebalances the tree via key borrowing or node merging.
- **Range query** (`bptree_get_range`): returns all values in a `[start, end]` inclusive range by scanning leaf nodes.
- **Iteration** (`bptree_iter_*`): forward iterator over the leaf-level linked list. Supports `begin`, `next`, `find`,
  `lower_bound`, and `upper_bound`.
- **Accessors** (`bptree_count`, `bptree_height`): O(1) reads. `bptree_clear` resets the tree without freeing it.

### Node Layout

Nodes use flexible array members (FAM) for memory-efficient storage with proper alignment. Internal nodes store keys and child
pointers; leaf nodes store keys, values, and a next-leaf pointer for efficient sequential access.

### Public API Surface

All functions and types declared before the `#ifdef BPTREE_IMPLEMENTATION` guard in `include/bptree.h` are the public API.
Changes to names, signatures, or status codes there are breaking. Internal `static` functions within the implementation section
may be refactored freely.

### Memory Management

The tree manages memory for its own internal nodes only. If values are pointers, the caller is responsible for allocating and
freeing the pointed-to data. `bptree_free` releases the tree structure but not external data referenced by stored values.

### Dependencies

Bptree has **no external C dependencies**. The public header requires only `<stdbool.h>` and `<stdint.h>`. The implementation
additionally uses `<assert.h>`, `<stdalign.h>`, `<stdarg.h>`, `<stdio.h>`, `<stdlib.h>`, `<string.h>`, and `<time.h>`. Do not
add dependencies without prior discussion.

## C Conventions

- C standard: C11 (compiled with `-std=c11`).
- Compiler: Clang (default) or GCC. Set via `CC` variable in the Makefile.
- Formatting is enforced by `clang-format` (Google base style, 4-space indent, 100-column limit). Run `make format` before
  committing.
- Naming: `snake_case` for functions and variables (e.g., `bptree_create`, `bptree_get_range`), `snake_case` with `_t` suffix
  for types (e.g., `bptree_key_t`, `bptree_value_t`), `UPPER_CASE` for macros and enum values (e.g., `BPTREE_IMPLEMENTATION`,
  `BPTREE_OK`).
- All public symbols are prefixed with `bptree_` or `BPTREE_`.
- Compile with `-Wall -Wextra -pedantic` (already set in the Makefile).

## Required Validation

Run the relevant targets for any change:

| Target            | Command            | What It Runs                                                 |
|-------------------|--------------------|--------------------------------------------------------------|
| Unit tests        | `make test`        | Compiles and runs `test/test_bptree.c`                       |
| Lint              | `make lint`        | Runs `cppcheck` static analysis on the `test/` directory     |
| Format check      | `make format`      | Formats all `.c` and `.h` files with `clang-format`          |
| Benchmarks        | `make bench`       | Compiles and runs `test/bench_bptree.c`                      |
| Examples          | `make example`     | Compiles and runs `test/example.c`                           |
| Memory check      | `make memcheck`    | Runs Valgrind leak checks on tests, benchmarks, and examples |
| Address sanitizer | `make asan`        | Builds and runs with AddressSanitizer enabled                |
| UB sanitizer      | `make ubsan`       | Builds and runs with UndefinedBehaviorSanitizer enabled      |
| Documentation     | `make doc`         | Generates Doxygen API docs into `doc/html/`                  |
| Everything        | `make all`         | Runs `clean`, `test`, `bench`, `example`, and `doc`          |
| Zig tests         | `make zig-test`    | Builds and runs tests via the Zig build system               |
| Zig benchmarks    | `make zig-bench`   | Builds and runs benchmarks via the Zig build system          |
| Zig example       | `make zig-example` | Builds and runs the example via the Zig build system         |
| Zig release       | `make zig-release` | Builds all artifacts with Zig in ReleaseFast mode            |

## First Contribution Flow

1. Read `include/bptree.h`, focusing on the public API and the area relevant to the change.
2. Implement the smallest change that covers the requirement.
3. Add or update tests in `test/test_bptree.c` (or the appropriate file under `test/extras/`) to cover the new behavior.
4. Run `make format`, `make test`, and `make lint`.
5. If public behavior changed, also run `make example` to ensure the examples still work.

Good first tasks:

- Fix a bug reported in the issue tracker and add a regression test in `test/extras/repro_bugs.c`.
- Add a new test case in `test/test_bptree.c` for an untested edge case or tree order.
- Improve an existing API function (with a corresponding test update).
- Add a new usage example in `test/example.c`.

## Testing Expectations

- Unit tests live in `test/test_bptree.c`. Additional test files for specific scenarios live in `test/extras/`.
- Tests use a custom harness: `ASSERT(condition)` for assertions, `RUN_TEST(function)` for test execution, with global
  `tests_run` and `tests_failed` counters.
- Tests cover multiple tree orders (`max_keys`: 3, 4, 7, 12, 32) to exercise different splitting and merging paths.
- Both numeric and string key modes should be tested. String key tests use conditional compilation with
  `BPTREE_KEY_TYPE_STRING`.
- Every new public API function or behavioral change must ship with tests covering the new or changed behavior.
- Run `make memcheck` or `make asan` when changing memory management logic to verify there are no leaks or out-of-bounds
  accesses.

## Change Design Checklist

Before coding:

1. Identify whether the change affects the public API (declarations before `BPTREE_IMPLEMENTATION`) or only internal
   implementation.
2. Consider whether both numeric and string key modes are affected.
3. Check memory management implications: does the change allocate, reallocate, or free memory? If so, verify cleanup paths.
4. Check cross-platform implications; the library targets Linux primarily but should remain portable C11.

Before submitting:

1. `make test` passes.
2. `make lint` passes.
3. `make format` has been run (no formatting diff).
4. `make example` still succeeds when touching public API functions.
5. `make memcheck` or `make asan` passes when changing memory-related code.
6. Doxygen comments updated (`make doc`) if the public API surface changed.

## Commit and PR Hygiene

- Keep commits scoped to one logical change.
- PR descriptions should include:
    1. Behavioral change summary.
    2. Tests added or updated.
    3. Validation commands run locally (e.g., `make test`, `make asan`).
