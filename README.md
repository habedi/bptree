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

- Single-header C library (see [include/bptree.h](include/bptree.h))
- Generic pointer storage with custom key comparator
- Supports insertion, deletion, point, and range queries
- Supports bulk loading from sorted items
- In-order iteration using an iterator API
- Custom memory allocator support via user-provided `malloc`, `free`, and `realloc` functions
- Compatible with C99 and newer

> [!NOTE]
> Bptree is in early stage of development, so breaking (API) changes may happen.
> Additionally, it is not thoroughly tested and benchmarked yet, so it may have lots of bugs and performance issues.
> Currently, it is more for educational purposes to learn about B+ trees than for real-world usage.

---

### Getting Started

To use Bptree, download the [include/bptree.h](include/bptree.h) file and include it in your project like this:

```c
// Add these lines to one C source file before including bptree.h
#define BPTREE_IMPLEMENTATION
#include "bptree.h"
```

### Example

To run the example shown below, run the `make example` command.

```c
#define BPTREE_IMPLEMENTATION
#include <stdio.h>

#include "bptree.h"

// Define a record structure for our sample data (a user record)
struct record {
    int id;         // Unique identifier
    char name[32];  // Name of the user
};

// Comparison function for records based on id
int record_compare(const void *a, const void *b, const void *udata) {
    (void)udata;  // Not used for this example
    const struct record *rec1 = a;
    const struct record *rec2 = b;
    return (rec1->id > rec2->id) - (rec1->id < rec2->id);
}

int main() {
    // Create a new B+ tree instance. We set max_keys to 4 for this example.
    // Passing NULL for user_data, alloc_ctx, and custom allocators uses the defaults.
    bptree *tree = bptree_new(4, record_compare, NULL, NULL, NULL, NULL, NULL, true);
    if (!tree) {
        printf("Failed to create the tree\n");
        return 1;
    }

    // Insert some records into the tree
    struct record rec1 = {1, "A"};
    struct record rec2 = {2, "B"};
    struct record rec3 = {3, "C"};
    struct record rec4 = {4, "D"};
    struct record rec5 = {5, "E"};
    struct record rec6 = {6, "F"};
    struct record rec7 = {7, "G"};
    struct record rec8 = {8, "H"};
    struct record rec9 = {9, "I"};

    // Insert records into the tree (not sorted)
    bptree_put(tree, &rec1);
    bptree_put(tree, &rec2);
    bptree_put(tree, &rec3);

    bptree_put(tree, &rec6);
    bptree_put(tree, &rec7);
    bptree_put(tree, &rec8);
    bptree_put(tree, &rec9);

    bptree_put(tree, &rec4);
    bptree_put(tree, &rec5);
    
    // Try inserting a duplicate record
    bptree_status dup_status = bptree_put(tree, &rec3);
    if (dup_status == BPTREE_DUPLICATE) {
        printf("Duplicate insert for id=%d correctly rejected.\n", rec3.id);
    }

    // Retrieve a record by key (id)
    const struct record key = {3, ""};
    struct record *result = bptree_get(tree, &key);
    if (result) {
        printf("Found record: id=%d, name=%s\n", result->id, result->name);
    } else {
        printf("Record with id=%d not found\n", key.id);
    }

    // Perform a range search: get records with id between 2 and 4 (including boundaries)
    // Note that only the `id` field is relevant here since the comparator uses it.
    int count = 0;
    void **range_results =
        bptree_get_range(tree, &(struct record){2, ""}, &(struct record){4, ""}, &count);
    if (range_results) {
        printf("Range search results:\n");
        for (int i = 0; i < count; i++) {
            struct record *r = range_results[i];
            printf("  id=%d, name=%s\n", r->id, r->name);
        }
        // Free the results array returned by bptree_get_range.
        // Pass 0 for the size as the default free ignores it.
        tree->free_fn(range_results, 0, tree->alloc_ctx);
    }

    // Iterate through the whole tree using the iterator
    printf("Iterating all records:\n");
    bptree_iterator *iter = bptree_iterator_new(tree);
    void *item;
    while ((item = bptree_iterator_next(iter))) {
        struct record *r = item;
        printf("  id=%d, name=%s\n", r->id, r->name);
    }
    bptree_iterator_free(iter, tree->free_fn, tree->alloc_ctx);

    // Remove a record
    const bptree_status status = bptree_remove(tree, &rec2);
    if (status == BPTREE_OK) {
        printf("Record with id=%d removed successfully.\n", rec2.id);
    } else {
        printf("Failed to remove record with id=%d.\n", rec2.id);
    }

    // Try to retrieve the removed record
    result = bptree_get(tree, &rec2);
    if (result) {
        printf("Found record: id=%d, name=%s\n", result->id, result->name);
    } else {
        printf("Record with id=%d not found (as expected)\n", rec2.id);
    }

    // Check the tree stats
    const bptree_stats stats = bptree_get_stats(tree);
    printf("Count: %d, Height: %d, Nodes: %d\n", stats.count, stats.height, stats.node_count);

    // Free the tree
    bptree_free(tree);

    return 0;
}
```

### Documentation

Bptree documentation can be generated using [Doxygen](https://www.doxygen.nl).
To generate the documentation,
use `make doc` command and then open the `doc/html/index.html` file in a web browser.

#### API

| Function / Type                                        | Description                                                                                                                                                                                                         |
|--------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `bptree_new`                                           | Creates a new B+ tree. It takes max keys, a key comparator (must return negative/zero/positive similar to `strcmp`), optional user data, custom allocators, and a debug flag. Returns a pointer or NULL on failure. |
| `bptree_free`                                          | Frees all nodes and internal structures. Does **not** free user-provided items.                                                                                                                                     |
| `bptree_put`                                           | Inserts an item. Returns `BPTREE_DUPLICATE` if the key already exists.                                                                                                                                              |
| `bptree_get`                                           | Retrieves an item by key. Returns the item pointer or NULL if not found.                                                                                                                                            |
| `bptree_remove`                                        | Removes an item by key and rebalances the tree if needed. Returns a status code (`BPTREE_OK`, `BPTREE_NOT_FOUND`, etc.).                                                                                            |
| `bptree_get_range`                                     | Returns an array of items between `start_key` and `end_key` (inclusive). Stores the number of results in a count variable. Must free the result array using `free_fn` (size can be 0).                              |
| `bptree_bulk_load`                                     | Builds a tree from a sorted, deduplicated array of items. Much faster than inserting one by one.                                                                                                                    |
| `bptree_iterator_new`                                  | Returns an iterator (`bptree_iterator *`) starting at the smallest key. Returns NULL if the tree is empty.                                                                                                          |
| `bptree_iterator_next`                                 | Returns the next item in key order by walking linked leaf nodes. Returns NULL when iteration is complete.                                                                                                           |
| `bptree_iterator_free`                                 | Frees the iterator. You must pass the `free_fn` and `alloc_ctx` used to allocate it.                                                                                                                                |
| `bptree_get_stats`                                     | Returns a `bptree_stats` struct with total item count, tree height, and total node count.                                                                                                                           |
| `bptree_iterator`                                      | Struct for maintaining state during in-order traversal. Contains current leaf and position.                                                                                                                         |
| `bptree_stats`                                         | Struct returned by `bptree_get_stats`, includes `count`, `height`, and `node_count` fields.                                                                                                                         |
| `bptree_malloc_t`, `bptree_free_t`, `bptree_realloc_t` | Function pointer types for custom allocators. See `bptree.h` for exact signatures. Used in `bptree_new` and `bptree_bulk_load`.                                                                                     |
| `debug_enabled`                                        | Boolean flag in `bptree_new` that enables internal debug logging to stdout via `bptree_logger`.                                                                                                                     |

#### Status Codes

The status codes are defined in the `bptree.h` header file as an enum:

```c
typedef enum {
    BPTREE_OK,               /* Operation performed successfully */
    BPTREE_DUPLICATE,        /* Duplicate key found during insertion */
    BPTREE_ALLOCATION_ERROR, /* Memory allocation error */
    BPTREE_NOT_FOUND,        /* Key not found */
    BPTREE_ERROR             /* General error */
} bptree_status;
```

### Tests and Benchmarks

Check out [test/test_bptree.c](test/test_bptree.c) for more detailed usage examples and test cases,
and [test/bench_bptree.c](test/bench_bptree.c) for performance benchmarks.

To run the tests and benchmarks, use the `make test` and `make bench` commands respectively.

Run `make all` to run the tests, benchmarks, examples, and generate the documentation.

---

### Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to make a contribution.

### License

Bptree is licensed under the MIT License ([LICENSE](LICENSE)).