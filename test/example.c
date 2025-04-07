/**
 * @file example.c
 * @brief An example usage of the Bptree library.
 *
 * This example demonstrates how to create a B+ tree, insert records,
 * retrieve records, perform range queries, iterate through the tree,
 * remove records, and bulk load new records.
 */

#define BPTREE_IMPLEMENTATION
#include <stdio.h>
#include <stdlib.h>

#include "bptree.h"

/* Global flag to enable or disable debug logging */
const bool debug_enabled = false;

/* Define a record structure for sample data. */
struct record {
    int id;         // Unique identifier; used as the key.
    char name[32];  // Name of the user.
};

/* Comparison function for keys (which are ints).
   This function compares the record ids. */
int record_compare(const bptree_key_t *a, const bptree_key_t *b) {
    int key1 = *a;
    int key2 = *b;
    return (key1 > key2) - (key1 < key2);
}

int main(void) {
    // Create a new B+ tree instance with max_keys = 4 and debug disabled.
    bptree *tree = bptree_create(4, record_compare, debug_enabled);
    if (!tree) {
        fprintf(stderr, "Error: failed to create B+ tree\n");
        return EXIT_FAILURE;
    }

    /* Create some sample records. */
    struct record rec1 = {1, "A"};
    struct record rec2 = {2, "B"};
    struct record rec3 = {3, "C"};
    struct record rec4 = {4, "D"};
    struct record rec5 = {5, "E"};
    struct record rec6 = {6, "F"};
    struct record rec7 = {7, "G"};
    struct record rec8 = {8, "H"};
    struct record rec9 = {9, "I"};

    /* Insert records into the tree.
       The key is the record's id, and the value is a pointer to the record. */
    bptree_put(tree, &rec1.id, &rec1);
    bptree_put(tree, &rec2.id, &rec2);
    bptree_put(tree, &rec3.id, &rec3);
    bptree_put(tree, &rec6.id, &rec6);
    bptree_put(tree, &rec7.id, &rec7);
    bptree_put(tree, &rec8.id, &rec8);
    bptree_put(tree, &rec9.id, &rec9);
    bptree_put(tree, &rec4.id, &rec4);
    bptree_put(tree, &rec5.id, &rec5);

    /* Attempt to insert a duplicate record. */
    bptree_status dup_status = bptree_put(tree, &rec3.id, &rec3);
    if (dup_status == BPTREE_DUPLICATE_KEY) {
        printf("Duplicate insert for id=%d correctly rejected.\n", rec3.id);
    }

    /* Retrieve a record by key. */
    int search_key = 3;
    struct record *found = bptree_get(tree, &search_key);
    if (found) {
        printf("Found record: id=%d, name=%s\n", found->id, found->name);
    } else {
        printf("Record with id=%d not found.\n", search_key);
    }

    /* Perform a range query for records with id between 2 and 4 (inclusive). */
    int low = 2, high = 4, range_count = 0;
    void **range_results = bptree_get_range(tree, &low, &high, &range_count);
    if (range_results) {
        printf("Range query results (id in [%d, %d]):\n", low, high);
        for (int i = 0; i < range_count; i++) {
            struct record *r = range_results[i];
            printf("  id=%d, name=%s\n", r->id, r->name);
        }
        free(range_results);
    }

    /* Iterate through all records using an iterator. */
    printf("Iterating all records:\n");
    bptree_iterator *iter = bptree_iterator_new(tree);
    void *item;
    while ((item = bptree_iterator_next(iter)) != NULL) {
        struct record *r = item;
        printf("  id=%d, name=%s\n", r->id, r->name);
    }
    bptree_iterator_destroy(iter);

    /* Remove a record. */
    if (bptree_remove(tree, &rec2.id) == BPTREE_OK) {
        printf("Record with id=%d removed successfully.\n", rec2.id);
    } else {
        printf("Failed to remove record with id=%d.\n", rec2.id);
    }

    /* Try to retrieve the removed record. */
    found = bptree_get(tree, &rec2.id);
    if (found) {
        printf("Found record: id=%d, name=%s\n", found->id, found->name);
    } else {
        printf("Record with id=%d not found (as expected).\n", rec2.id);
    }

    /* Check invariants of the tree. */
    if (bptree_check_invariants(tree)) {
        printf("Tree invariants are valid.\n");
    } else {
        printf("Tree invariants are invalid!\n");
    }

    /* Upsert a record (insert or update). */
    struct record rec10 = {3, "Updated C2"};
    bptree_upsert(tree, &rec10.id, &rec10);

    /* Retrieve the updated record. */
    found = bptree_get(tree, &rec10.id);
    if (found) {
        printf("Found upserted record: id=%d, name=%s\n", found->id, found->name);
    } else {
        printf("Upserted record with id=%d not found.\n", rec10.id);
    }

    /* Get and print tree statistics. */
    bptree_stats stats = bptree_get_stats(tree);
    printf("Tree stats: count=%d, height=%d, node_count=%d\n", stats.count, stats.height,
           stats.node_count);

    /* Perform a bulk load.
       Prepare separate arrays for keys and record pointers.
       Bulk load replaces the tree's contents, so be careful!.
    */
    struct record bulk_records[] = {
        {10, "J"}, {11, "K"}, {12, "L"}, {13, "M"}, {14, "N"}, {15, "O"},
    };
    int bulk_count = sizeof(bulk_records) / sizeof(bulk_records[0]);

    int bulk_keys[bulk_count];
    struct record *bulk_values[bulk_count];
    for (int i = 0; i < bulk_count; i++) {
        bulk_keys[i] = bulk_records[i].id;
        bulk_values[i] = &bulk_records[i];
    }
    bptree_bulk_load(tree, bulk_keys, (bptree_value_t *)bulk_values, bulk_count);

    /* Iterate through all records again after the bulk load. */
    printf("Iterating all records after bulk load:\n");
    iter = bptree_iterator_new(tree);
    while ((item = bptree_iterator_next(iter)) != NULL) {
        struct record *r = item;
        printf("  id=%d, name=%s\n", r->id, r->name);
    }
    bptree_iterator_destroy(iter);

    /* Assert that the tree is still valid after the bulk load. */
    bptree_assert_invariants(tree);

    /* Get and print tree statistics after the bulk load. */
    stats = bptree_get_stats(tree);
    printf("Tree stats after bulk load: count=%d, height=%d, node_count=%d\n", stats.count,
           stats.height, stats.node_count);

    /* Free the tree. */
    bptree_free(tree);

    return EXIT_SUCCESS;
}
