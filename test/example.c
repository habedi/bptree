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
