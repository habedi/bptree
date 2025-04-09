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

A B+ tree implementation in C

</div>

---

Bptree is a single-header, generic [B+ tree](https://en.wikipedia.org/wiki/B%2B_tree) implementation written in pure C.

### Features

- Single-header C library (no external dependencies) (see [include/bptree.h](include/bptree.h))
- Generic storage using pointers with customizable key comparators
- Supports insertion, deletion, bulk loading, upsert, point queries, and range queries
- Supports in-order iterations
- Can work with user-provided `alloc`, `realloc`, and `free` functions
- Compatible with C11 or newer

---

### Getting Started

Download the [include/bptree.h](include/bptree.h) file and include it in your projects:

```c
// Add these lines to your C source file
#define BPTREE_IMPLEMENTATION
#include "bptree.h"
```

``BPTREE_IMPLEMENTATION`` needs to be defined only once in your project, typically in one of your source files.

### Examples

| File                        | Description                        |
|-----------------------------|------------------------------------|
| [example.c](test/example.c) | Example usages of the B+ tree API. |

To run the example(s), run `make example` command.

### Documentation

Bptree documentation can be generated using [Doxygen](https://www.doxygen.nl).
To generate the documentation,
use `make doc` command and then open the `doc/html/index.html` file in a web browser.

#### API

| Function / Type           | Description                                                                                                                                     |
|---------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| `bptree_create`           | Creates a new B+ tree with specified `max_keys`, comparator function (or NULL for default), and debug flag. Returns pointer or NULL on failure. |
| `bptree_free`             | Frees the tree and all its nodes. Does not free user-managed values.                                                                            |
| `bptree_put`              | Inserts a key-value pair. Fails with `BPTREE_DUPLICATE_KEY` if key exists.                                                                      |
| `bptree_upsert`           | Inserts or updates a key-value pair (insert if absent, overwrite if exists).                                                                    |
| `bptree_get`              | Looks up a value by key. Returns value or NULL if not found.                                                                                    |
| `bptree_remove`           | Deletes a key. Does not rebalance fully; just removes from leaf.                                                                                |
| `bptree_get_range`        | Returns a dynamically allocated array of values within `[start, end]` inclusive. Caller must free it.                                           |
| `bptree_bulk_load`        | Builds a tree from sorted, unique key-value pairs. More efficient than repeated inserts.                                                        |
| `bptree_get_stats`        | Returns tree stats: total keys, tree height, and number of nodes.                                                                               |
| `bptree_check_invariants` | Checks structural correctness of the tree (e.g., sorted keys, uniform leaf depth, and correct key promotion).                                   | |
| `bptree_iterator_new`     | Creates a new iterator starting from the smallest key.                                                                                          |
| `bptree_iterator_next`    | Returns next value in order or NULL at the end.                                                                                                 |
| `bptree_iterator_free`    | Frees the iterator.                                                                                                                             | 

#### Status Codes

```c
typedef enum {
    BPTREE_OK = 0,               // Operation succeeded
    BPTREE_DUPLICATE_KEY,        // Duplicate key inserted on insert
    BPTREE_KEY_NOT_FOUND,        // Key not found in the tree
    BPTREE_ALLOCATION_FAILURE,   // malloc or alloc failed
    BPTREE_INVALID_ARGUMENT,     // NULL or bad input argument
    BPTREE_BULK_LOAD_NOT_SORTED, // Input to bulk load not sorted (keys must be sorted)
    BPTREE_BULK_LOAD_DUPLICATE,  // Duplicate keys in bulk load (keys must be distinct)
    BPTREE_INTERNAL_ERROR        // An internal implementation-specific error occurred
} bptree_status;
```

#### Key and Value Configuration

| Macro / Type             | Description                                                                                    |
|--------------------------|------------------------------------------------------------------------------------------------|
| `BPTREE_KEY_TYPE_INT`    | Define this to use integer keys. Defaults to `long int`. Override type with `BPTREE_INT_TYPE`. |
| `BPTREE_KEY_TYPE_FLOAT`  | Define this to use float keys. Defaults to `double`. Override type with `BPTREE_FLOAT_TYPE`.   |
| `BPTREE_KEY_TYPE_STRING` | Define this to use fixed-size string keys. Must also define `BPTREE_KEY_SIZE`.                 |
| `BPTREE_KEY_SIZE`        | Required for string keys. Specifies fixed length (e.g. 32 or 64).                              |
| `bptree_key_t`           | Key type used internally. Depends on which macros above are defined.                           |
| `BPTREE_VALUE_TYPE`      | Type for values stored in the tree. Defaults to `void *`. Can be redefined to any other type.  |
| `bptree_value_t`         | Final resolved type of values (from `BPTREE_VALUE_TYPE`).                                      |

Note that he tree only stores pointers to values; it does not copy or free them. You are responsible
for memory management.

##### Examples

To use integer keys and `struct record *` values:

```c
#define BPTREE_KEY_TYPE_INT
#define BPTREE_VALUE_TYPE struct record *
#include "bptree.h"
```

To use fixed-size string keys (e.g., 32-byte strings):

```c
#define BPTREE_KEY_TYPE_STRING
#define BPTREE_KEY_SIZE 32
#include "bptree.h"
```

To use float keys and store raw data pointers:

```c
#define BPTREE_KEY_TYPE_FLOAT
#define BPTREE_FLOAT_TYPE float
#include "bptree.h"
```

### Tests and Benchmarks

| File                                  | Description                     |
|---------------------------------------|---------------------------------|
| [test_bptree.c](test/test_bptree.c)   | Unit tests for the B+ tree API. |
| [bench_bptree.c](test/bench_bptree.c) | Benchmarks for the B+ tree API. |

To run the tests and benchmarks, use the `make test` and `make bench` commands respectively.

---

### Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to make a contribution.

### License

Bptree is licensed under the MIT License ([LICENSE](LICENSE)).
