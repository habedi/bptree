<div align="center">

<picture>
    <img alt="B+ tree" src="logo.svg" height="90%" width="90%">
</picture>
<br>

<h2>Bptree</h2>

[![Tests](https://img.shields.io/github/actions/workflow/status/habedi/bptree/tests.yml?label=tests&style=flat&labelColor=282c34&logo=github)](https://github.com/habedi/bptree/actions/workflows/tests.yml)
[![Lints](https://img.shields.io/github/actions/workflow/status/habedi/bptree/lints.yml?label=lints&style=flat&labelColor=282c34&logo=github)](https://github.com/habedi/bptree/actions/workflows/lints.yml)
[![Benchmarks](https://img.shields.io/github/actions/workflow/status/habedi/bptree/benches.yml?label=benches&style=flat&labelColor=282c34&logo=github)](https://github.com/habedi/bptree/actions/workflows/benches.yml)
[![Code Coverage](https://img.shields.io/codecov/c/github/habedi/bptree?label=coverage&style=flat&labelColor=282c34&logo=codecov)](https://codecov.io/gh/habedi/bptree)
[![CodeFactor](https://img.shields.io/codefactor/grade/github/habedi/bptree?label=code%20quality&style=flat&labelColor=282c34&logo=codefactor)](https://www.codefactor.io/repository/github/habedi/bptree)
[![License](https://img.shields.io/badge/license-MIT-007ec6?label=license&style=flat&labelColor=282c34&logo=open-source-initiative)](https://github.com/habedi/bptree/blob/main/LICENSE)
[![Release](https://img.shields.io/github/release/habedi/bptree.svg?label=release&style=flat&labelColor=282c34&logo=github)](https://github.com/habedi/bptree/releases/latest)

A customizable B+ tree implementation in C

</div>

---

Bptree is a single-header, generic [B+ tree](https://en.wikipedia.org/wiki/B%2B_tree) implementation written in pure C.

### Features

- **Single-header C library:** Requires only the C standard library (C11) (see [include/bptree.h](include/bptree.h))
- **Generic Key/Value Storage:** Supports different key types (numeric, fixed-size strings) and value types via configuration
  macros. Uses customizable key comparators.
- **Core Operations:** Supports insertion (`bptree_put`), deletion with rebalancing (`bptree_remove`), point queries (
  `bptree_get`, `bptree_contains`), and range queries (`bptree_get_range`).
- **Utilities:** Includes functions for checking tree structural invariants (`bptree_check_invariants`) and getting statistics (
  `bptree_get_stats`).
- **Memory Management:** Manages internal node memory; caller manages memory for stored values.
- **Standard Compliance:** Compatible with C11 or newer (uses `<stdalign.h>`, `<stdint.h>`, `<stdbool.h>`, `aligned_alloc`).

---

### Getting Started

Download the [include/bptree.h](include/bptree.h) file and include it in your projects.

Define `BPTREE_IMPLEMENTATION` in **exactly one** C source file before including the header to generate the implementation:

```c
// Add these lines to one of your C source files (e.g., main.c)
#define BPTREE_IMPLEMENTATION
#include "bptree.h"
````

See the "Key and Value Configuration" section below for how to customize key/value types.

### Examples

| File                                                        | Description                        |
|:------------------------------------------------------------|:-----------------------------------|
| [example.c](https://www.google.com/search?q=test/example.c) | Example usages of the B+ tree API. |

To compile and run the example(s), use the `make example` command (assuming a suitable Makefile exists).

### Documentation

API documentation can be generated using [Doxygen](https://www.doxygen.nl).
To generate the documentation, use the `make doc` command (assuming a suitable Makefile target exists) and then open the
`doc/html/index.html` file in a web browser.

#### API Summary

| Function / Type             | Description                                                                                                            |
|:----------------------------|:-----------------------------------------------------------------------------------------------------------------------|
| `bptree_create`             | Creates a new B+ tree with specified `max_keys`, comparator function (or `NULL` for default), and debug flag.          |
| `bptree_free`               | Frees the tree structure and all its internal nodes. Does **not** free user-managed values.                            |
| `bptree_put`                | Inserts a key-value pair. Returns `BPTREE_DUPLICATE_KEY` if the key already exists.                                    |
| `bptree_get`                | Retrieves the value associated with a key via an out-parameter. Returns `bptree_status`.                               |
| `bptree_contains`           | Checks if a key exists in the tree. Returns `true` or `false`.                                                         |
| `bptree_remove`             | Deletes a key-value pair. Performs full node rebalancing (borrow/merge) if necessary.                                  |
| `bptree_get_range`          | Returns a dynamically allocated array of values for keys within `[start, end]` (inclusive). Caller must free.          |
| `bptree_free_range_results` | Frees the array allocated by `bptree_get_range`.                                                                       |
| `bptree_get_stats`          | Returns tree statistics: item count, tree height, and internal node count.                                             |
| `bptree_check_invariants`   | Checks structural correctness (key ordering, node fill levels, leaf depth, child relationships). Useful for debugging. |
| `bptree_key_t`              | The data type used for keys (configurable).                                                                            |
| `bptree_value_t`            | The data type used for values (configurable, defaults to `void *`).                                                    |
| `bptree_status`             | Enum returned by most API functions indicating success or failure type.                                                |

#### Status Codes

```c
// Status codes returned by bptree functions
typedef enum {
    BPTREE_OK = 0,             // Operation succeeded
    BPTREE_DUPLICATE_KEY,      // Key already exists during bptree_put
    BPTREE_KEY_NOT_FOUND,      // Key not found for get/remove operation
    BPTREE_ALLOCATION_FAILURE, // Memory allocation (e.g., malloc, aligned_alloc) failed
    BPTREE_INVALID_ARGUMENT,   // NULL or invalid function argument provided
    BPTREE_INTERNAL_ERROR      // An unexpected internal state or error occurred
} bptree_status;
```

#### Key and Value Configuration

The key and value types, as well as linkage, can be customized by defining these macros **before** including `bptree.h` where
`BPTREE_IMPLEMENTATION` is defined:

| Macro / Type             | Description                                                                                                       | Default             |
|:-------------------------|:------------------------------------------------------------------------------------------------------------------|:--------------------|
| `BPTREE_NUMERIC_TYPE`    | Use a specific integer or floating-point type for keys (if `BPTREE_KEY_TYPE_STRING` is **not** defined).          | `int64_t`           |
| `BPTREE_KEY_TYPE_STRING` | Define this macro (no value needed) to use fixed-size string keys instead of numeric keys.                        | *Not defined*       |
| `BPTREE_KEY_SIZE`        | **Required** if `BPTREE_KEY_TYPE_STRING` is defined. Specifies the exact size (bytes) of the string key struct.   | *N/A*               |
| `BPTREE_VALUE_TYPE`      | Specifies the data type for values stored in the tree.                                                            | `void *`            |
| `BPTREE_STATIC`          | Define this macro (no value needed) along with `BPTREE_IMPLEMENTATION` to give the implementation static linkage. | *Not defined*       |
| `bptree_key_t`           | The C type representing a key. Resolved based on the `BPTREE_...` macros above.                                   | `int64_t` or struct |
| `bptree_value_t`         | The C type representing a value. Resolved from `BPTREE_VALUE_TYPE`.                                               | `void *`            |

**Important Note on Values:** The tree only stores the `bptree_value_t` itself. If you store pointers (like the default `void *`
or `struct my_data *`), **you** are responsible for managing the memory pointed to (allocation and deallocation).

##### Examples

To use `uint32_t` keys and `struct record *` values:

```c
#define BPTREE_NUMERIC_TYPE uint32_t
#define BPTREE_VALUE_TYPE struct record *
#define BPTREE_IMPLEMENTATION
#include "bptree.h"
```

To use fixed-size string keys (e.g., 32-byte strings) and store integer IDs as values:

```c
#define BPTREE_KEY_TYPE_STRING
#define BPTREE_KEY_SIZE 32
#define BPTREE_VALUE_TYPE intptr_t // Store integers directly if they fit
#define BPTREE_IMPLEMENTATION
#include "bptree.h"
```

### Tests and Benchmarks

| File                                                                   | Description                                 |
|:-----------------------------------------------------------------------|:--------------------------------------------|
| [test\_bptree.c](https://www.google.com/search?q=test/test_bptree.c)   | Unit tests verifying API correctness.       |
| [bench\_bptree.c](https://www.google.com/search?q=test/bench_bptree.c) | Benchmarks measuring operation performance. |

To run the tests and benchmarks, use the `make test` and `make bench` commands respectively (assuming a suitable Makefile exists).

-----

### Contributing

See [CONTRIBUTING.md](https://www.google.com/search?q=CONTRIBUTING.md) for details on how to make a contribution.

### License

Bptree is licensed under the MIT License ([LICENSE](https://www.google.com/search?q=LICENSE)).

