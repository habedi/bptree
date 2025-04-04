## Bptree

[![Tests](https://img.shields.io/github/actions/workflow/status/habedi/bptree/tests.yml?label=tests&style=flat&labelColor=282c34&logo=github)](https://github.com/habedi/bptree/actions/workflows/tests.yml)
[![Lints](https://img.shields.io/github/actions/workflow/status/habedi/bptree/lints.yml?label=lints&style=flat&labelColor=282c34&logo=github)](https://github.com/habedi/bptree/actions/workflows/lints.yml)
[![Benches](https://img.shields.io/github/actions/workflow/status/habedi/bptree/benches.yml?label=benches&style=flat&labelColor=282c34&logo=github)](https://github.com/habedi/bptree/actions/workflows/benches.yml)
[![Code Coverage](https://img.shields.io/codecov/c/github/habedi/bptree?label=coverage&style=flat&labelColor=282c34&logo=codecov)](https://codecov.io/gh/habedi/bptree)
[![CodeFactor](https://img.shields.io/codefactor/grade/github/habedi/bptree?label=code%20quality&style=flat&labelColor=282c34&logo=codefactor)](https://www.codefactor.io/repository/github/habedi/bptree)
[![License](https://img.shields.io/badge/license-MIT-007ec6?label=license&style=flat&labelColor=282c34&logo=open-source-initiative)](https://github.com/habedi/bptree/blob/main/LICENSE)
[![Release](https://img.shields.io/github/release/habedi/bptree.svg?label=release&style=flat&labelColor=282c34&logo=github)](https://github.com/habedi/bptree/releases/latest)

Bptree is a [B+tree](https://en.wikipedia.org/wiki/B%2B_tree) implementation in pure C.

### Features

- Single-header C library (see [include/bptree.h](include/bptree.h))
- Generic pointer storage with custom key comparator
- Supports insertion, deletion, point and range queries
- Supports bulk loading from sorted items
- In-order iteration using an iterator API
- Custom memory allocator support via user-provided `malloc` and `free` functions
- Compatible with C99 and newer

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
#include <string.h>

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
    // Create a new B+tree instance. We set max_keys to 4 for this example.
    bptree *tree = bptree_new(4, record_compare, NULL, NULL, NULL, true);  // debug_enabled = true
    if (!tree) {
        printf("Failed to create B+tree\n");
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

    // Insert records into the tree (not sorted by id for demonstration)
    bptree_put(tree, &rec1);
    bptree_put(tree, &rec2);
    bptree_put(tree, &rec3);

    bptree_put(tree, &rec6);
    bptree_put(tree, &rec7);
    bptree_put(tree, &rec8);
    bptree_put(tree, &rec9);

    bptree_put(tree, &rec4);
    bptree_put(tree, &rec5);

    // Retrieve a record by key (id)
    struct record key = {3, ""};
    struct record *result = bptree_get(tree, &key);
    if (result) {
        printf("Found record: id=%d, name=%s\n", result->id, result->name);
    } else {
        printf("Record with id=%d not found\n", key.id);
    }

    // Perform a range search: get records with id between 2 and 4 (including boundaries)
    int count = 0;
    void **range_results =
        bptree_get_range(tree, &(struct record){2, ""}, &(struct record){4, ""}, &count);
    if (range_results) {
        printf("Range search results:\n");
        for (int i = 0; i < count; i++) {
            struct record *r = range_results[i];
            printf("  id=%d, name=%s\n", r->id, r->name);
        }
        // Free the results array returned by bptree_get_range
        tree->free_fn(
            range_results);  // Always use tree->free_fn to free memory allocated by the tree
    }

    // Iterate through the whole tree using the iterator
    printf("Iterating all records:\n");
    bptree_iterator *iter = bptree_iterator_new(tree);
    void *item;
    while ((item = bptree_iterator_next(iter))) {
        struct record *r = item;
        printf("  id=%d, name=%s\n", r->id, r->name);
    }
    bptree_iterator_free(iter, tree->free_fn);

    // Remove a record
    bptree_status status = bptree_remove(tree, &rec2);
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
    bptree_stats stats = bptree_get_stats(tree);
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

| Function               | Description                                                                                                                                                                                                                                                                 |
|------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `bptree_new`           | Creates a new B+tree instance. Accepts maximum keys per node, a key comparison function (which must return -1, 0, or 1 like `strcmp`), user data, optional custom memory allocation/free functions, and a debug flag. Returns a pointer to the new tree or NULL on failure. |
| `bptree_free`          | Frees the tree along with all its associated memory and nodes.                                                                                                                                                                                                              |
| `bptree_put`           | Inserts an item into the tree. Fails with `BPTREE_DUPLICATE` if the key already exists.                                                                                                                                                                                     |
| `bptree_get`           | Retrieves an item from the tree by key. Returns a pointer to the item if found, or NULL if the key does not exist.                                                                                                                                                          |
| `bptree_remove`        | Removes an item from the tree by key and rebalances the tree if necessary. Returns a status code (e.g., `BPTREE_OK`, `BPTREE_NOT_FOUND`, etc.).                                                                                                                             |
| `bptree_get_range`     | Performs an inclusive range search. Returns an array of items with keys between the specified start and end values (inclusive), and stores the number of items found in a count variable. The returned array must be freed using the tree’s `free_fn` function.             |
| `bptree_bulk_load`     | Builds a B+tree from a sorted array of distinct items. Much faster than inserting items individually. Useful for initialization or loading large datasets.                                                                                                                  |
| `bptree_iterator_new`  | Creates a new iterator starting from the smallest key in the tree. Returns NULL if the tree is empty.                                                                                                                                                                       |
| `bptree_iterator_next` | Returns the next item in key order. Returns NULL when iteration is complete.                                                                                                                                                                                                |
| `bptree_iterator_free` | Frees the iterator. Uses the tree's `free_fn` for deallocation.                                                                                                                                                                                                             |
| `bptree_get_stats`     | Returns statistics about the tree, including the number of items, height, and node count.                                                                                                                                                                                   |

#### Status Codes

The status codes are defined in the `bptree.h` header file as an enum:

```c
typedef enum {
    BPTREE_OK, // Operation completed successfully
    BPTREE_DUPLICATE, // Attempted to insert a duplicate key
    BPTREE_ALLOCATION_ERROR, // Memory allocation failed
    BPTREE_NOT_FOUND, // Key not found in the tree
    BPTREE_ERROR // A general error occurred
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
