/**
 * @file example.c
 * @brief An improved example usage of the Bptree library.
 *
 * This example demonstrates how to create a B+ tree, insert dynamically
 * allocated records, retrieve records, perform range queries, iterate
 * through the tree, remove records, upsert, check invariants, get stats,
 * and bulk load new records, ensuring proper memory management.
 */

// Define BPTREE_IMPLEMENTATION in this file only to generate the code
#define BPTREE_IMPLEMENTATION
// Define the key type before including bptree.h (default is INT anyway)
#define BPTREE_KEY_TYPE_INT
// Explicitly define the value type for clarity (though void* is default)
#define BPTREE_VALUE_TYPE struct record *
#include "bptree.h" // Include the header AFTER definitions

#include <stdio.h>
#include <stdlib.h>
#include <string.h> // For strncpy

/* Global flag to enable or disable debug logging */
const bool debug_enabled = false; // Set to true for verbose B+ tree logs

/* Define a record structure for sample data. */
typedef struct record {
    int id;         // Unique identifier; used as the key.
    char name[32];  // Name of the user.
} record_t; // Use typedef for cleaner syntax

/* Comparison function for keys (which are ints).
   Note: The library passes const bptree_key_t*, which is const int* here.
   The default comparator would work fine too, but this shows how to provide one. */
static int record_compare(const bptree_key_t *a, const bptree_key_t *b) {
    // Since BPTREE_KEY_TYPE_INT, bptree_key_t is int.
    // a and b are pointers to int.
    return (*a < *b) ? -1 : ((*a > *b) ? 1 : 0);
    // Or using the simpler difference (if overflow is not a concern for the range of keys)
    // return *a - *b;
}

/* Helper function to create and allocate a record */
static record_t *create_record(int id, const char *name) {
    record_t *rec = malloc(sizeof(record_t));
    if (!rec) {
        perror("Failed to allocate memory for record");
        return NULL;
    }
    rec->id = id;
    strncpy(rec->name, name, sizeof(rec->name) - 1);
    rec->name[sizeof(rec->name) - 1] = '\0'; // Ensure null termination
    return rec;
}

/* Helper function to print status codes */
static const char *status_to_string(bptree_status status) {
    switch (status) {
        case BPTREE_OK: return "OK";
        case BPTREE_DUPLICATE_KEY: return "DUPLICATE_KEY";
        case BPTREE_KEY_NOT_FOUND: return "KEY_NOT_FOUND";
        case BPTREE_ALLOCATION_FAILURE: return "ALLOCATION_FAILURE";
        case BPTREE_INVALID_ARGUMENT: return "INVALID_ARGUMENT";
        case BPTREE_BULK_LOAD_NOT_SORTED: return "BULK_LOAD_NOT_SORTED";
        case BPTREE_BULK_LOAD_DUPLICATE: return "BULK_LOAD_DUPLICATE";
        case BPTREE_INTERNAL_ERROR: return "INTERNAL_ERROR";
        default: return "UNKNOWN_STATUS";
    }
}

/* Function to clean up: free all records stored in the tree */
static void free_all_records(bptree *tree) {
    printf("\n--- Freeing Records ---\n");
    bptree_iterator *iter = bptree_iterator_new(tree);
    if (!iter) {
        fprintf(stderr, "Failed to create iterator for freeing records.\n");
        // Potential memory leak if tree contains records, but we should proceed to free the tree structure
        return;
    }
    record_t *item;
    int count = 0;
    // Note: BPTREE_VALUE_TYPE is record_t*, so iterator_next returns record_t*
    while ((item = bptree_iterator_next(iter)) != NULL) {
        // printf("Freeing record: id=%d, name=%s\n", item->id, item->name); // Optional: verbose freeing
        free(item);
        count++;
    }
    printf("Freed %d records.\n", count);
    bptree_iterator_destroy(iter);
}


int main(void) {
    printf("--- B+ Tree Example ---\n");

    // --- Initialization ---
    printf("\n--- Creating Tree ---\n");
    // Create a new B+ tree instance with max_keys = 4.
    // Explicitly pass the comparison function (or NULL to use default for INT).
    bptree *tree = bptree_create(4, record_compare, debug_enabled);
    if (!tree) {
        fprintf(stderr, "Error: failed to create B+ tree\n");
        return EXIT_FAILURE;
    }
    printf("B+ Tree created successfully (max_keys=%d).\n", tree->max_keys);

    // --- Basic Operations ---
    printf("\n--- Inserting Records ---\n");
    bptree_status status;
    record_t *rec_ptrs[9]; // Keep track of pointers to free later if needed, though we iterate

    // Create records dynamically
    rec_ptrs[0] = create_record(1, "Alice");
    rec_ptrs[1] = create_record(2, "Bob");
    rec_ptrs[2] = create_record(3, "Charlie");
    rec_ptrs[3] = create_record(6, "Frank");
    rec_ptrs[4] = create_record(7, "Grace");
    rec_ptrs[5] = create_record(8, "Heidi");
    rec_ptrs[6] = create_record(9, "Ivan");
    rec_ptrs[7] = create_record(4, "David");
    rec_ptrs[8] = create_record(5, "Eve");

    // Check allocation results
    for (int i = 0; i < 9; ++i) {
        if (!rec_ptrs[i]) {
            fprintf(stderr, "Failed to allocate memory for initial records. Cleaning up.\n");
            // Free already allocated records before freeing the tree
            for(int j = 0; j < i; ++j) free(rec_ptrs[j]);
            bptree_free(tree);
            return EXIT_FAILURE;
        }
    }

    // Insert records into the tree. Key is record's id, value is pointer.
    for (int i = 0; i < 9; ++i) {
        status = bptree_put(tree, &rec_ptrs[i]->id, rec_ptrs[i]);
        printf("Inserting id=%d: Status = %s\n", rec_ptrs[i]->id, status_to_string(status));
        if (status != BPTREE_OK && status != BPTREE_DUPLICATE_KEY /* Should not happen here */) {
            fprintf(stderr, "Error inserting record id=%d. Aborting.\n", rec_ptrs[i]->id);
            free_all_records(tree); // Free records inserted so far
            bptree_free(tree);
            // Also free any remaining rec_ptrs that weren't inserted
            for(int j = i; j < 9; ++j) free(rec_ptrs[j]);
            return EXIT_FAILURE;
        }
    }


    /* Attempt to insert a duplicate record. */
    printf("\n--- Testing Duplicate Insert ---\n");
    record_t *dup_rec = create_record(3, "Charlie Duplicate"); // Need a new record instance
    if (!dup_rec) return EXIT_FAILURE; // Handle allocation failure
    status = bptree_put(tree, &dup_rec->id, dup_rec);
    if (status == BPTREE_DUPLICATE_KEY) {
        printf("Duplicate insert for id=%d correctly rejected (Status: %s).\n", dup_rec->id, status_to_string(status));
        free(dup_rec); // Free the duplicate record not inserted
    } else {
         fprintf(stderr, "Error: Duplicate key was not rejected or another error occurred (Status: %s).\n", status_to_string(status));
         // If it somehow got inserted (shouldn't happen), it would need freeing later.
         // If another error, clean up everything.
         free(dup_rec); // Free the record anyway
         free_all_records(tree);
         bptree_free(tree);
         return EXIT_FAILURE;
    }


    /* Retrieve a record by key. */
    printf("\n--- Retrieving Record ---\n");
    const int search_key = 3;
    // Note: bptree_get returns BPTREE_VALUE_TYPE, which is record_t*
    record_t *found = bptree_get(tree, &search_key);
    if (found) {
        printf("Found record: id=%d, name=%s\n", found->id, found->name);
    } else {
        printf("Record with id=%d not found.\n", search_key);
    }

    /* Perform a range query for records with id between 4 and 7 (inclusive). */
    printf("\n--- Range Query ---\n");
    const int low = 4;
    const int high = 7;
    int range_count = 0;
    // bptree_get_range returns an array of BPTREE_VALUE_TYPE (record_t*)
    // Cast the result to record_t**
    record_t **range_results = (record_t **)bptree_get_range(tree, &low, &high, &range_count);

    if (range_results) {
        printf("Range query results (id in [%d, %d]), count = %d:\n", low, high, range_count);
        for (int i = 0; i < range_count; i++) {
            record_t *r = range_results[i]; // Direct access, no cast needed inside loop
            if (r) { // Good practice to check pointer, though library should return valid ones
                 printf("  id=%d, name=%s\n", r->id, r->name);
            }
        }
        // IMPORTANT: The caller must free the array returned by bptree_get_range,
        // but NOT the records *within* the array (they are still owned by the tree).
        free(range_results);
    } else {
         printf("No records found in range [%d, %d] or error occurred.\n", low, high);
    }

    /* Iterate through all records using an iterator. */
    printf("\n--- Iterating All Records ---\n");
    bptree_iterator *iter = bptree_iterator_new(tree);
    if (!iter) {
         fprintf(stderr, "Failed to create iterator.\n");
    } else {
        record_t *item;
        while ((item = bptree_iterator_next(iter)) != NULL) {
            printf("  id=%d, name=%s\n", item->id, item->name);
        }
        bptree_iterator_destroy(iter);
    }


    /* Remove a record. */
    printf("\n--- Removing Record ---\n");
    const int remove_key = 2;
    // First, get the pointer to free it *after* successful removal
    record_t *record_to_remove = bptree_get(tree, &remove_key);
    if (record_to_remove) {
        status = bptree_remove(tree, &remove_key);
        printf("Attempting to remove id=%d: Status = %s\n", remove_key, status_to_string(status));
        if (status == BPTREE_OK) {
            printf("Record with id=%d removed successfully from tree.\n", remove_key);
            free(record_to_remove); // Now free the memory for the removed record
        } else {
            fprintf(stderr, "Failed to remove record with id=%d from tree structure.\n", remove_key);
            // Do NOT free record_to_remove as it's presumably still in the tree
        }
    } else {
         printf("Record with id=%d not found, cannot remove.\n", remove_key);
    }


    /* Try to retrieve the removed record. */
    printf("\n--- Retrieving Removed Record ---\n");
    found = bptree_get(tree, &remove_key);
    if (found) {
        // This should not happen if removal was successful
        printf("Error: Found removed record: id=%d, name=%s\n", found->id, found->name);
    } else {
        printf("Record with id=%d not found (as expected after removal).\n", remove_key);
    }

    /* Upsert: Update an existing record */
    printf("\n--- Upserting (Update) ---\n");
    const int upsert_key_update = 3;
    record_t *old_record_to_replace = bptree_get(tree, &upsert_key_update); // Get pointer to old record
    record_t *updated_rec = create_record(upsert_key_update, "Charlie Updated");
    if (!updated_rec) { /* handle alloc error */ return EXIT_FAILURE; }

    status = bptree_upsert(tree, &updated_rec->id, updated_rec);
    printf("Upserting id=%d: Status = %s\n", updated_rec->id, status_to_string(status));

    if (status == BPTREE_OK) {
         printf("Upsert successful for id=%d.\n", updated_rec->id);
         if (old_record_to_replace && old_record_to_replace != updated_rec) {
             // If upsert replaced an existing item, free the old one.
             printf("Freeing old record for id=%d.\n", old_record_to_replace->id);
             free(old_record_to_replace);
         }
    } else {
        fprintf(stderr, "Upsert failed for id=%d with status %s\n", updated_rec->id, status_to_string(status));
        free(updated_rec); // Free the newly allocated record if upsert failed
    }


    /* Upsert: Insert a new record */
    printf("\n--- Upserting (Insert) ---\n");
    record_t *new_rec = create_record(10, "Judy");
    if (!new_rec) { /* handle alloc error */ return EXIT_FAILURE; }
    status = bptree_upsert(tree, &new_rec->id, new_rec);
    printf("Upserting id=%d: Status = %s\n", new_rec->id, status_to_string(status));
     if (status != BPTREE_OK) {
        fprintf(stderr, "Upsert failed for id=%d with status %s\n", new_rec->id, status_to_string(status));
        free(new_rec); // Free the newly allocated record if upsert failed
    }


    /* Retrieve the upserted records */
    printf("\n--- Retrieving Upserted Records ---\n");
    found = bptree_get(tree, &upsert_key_update); // Key 3
    if (found) printf("Found upserted record: id=%d, name=%s\n", found->id, found->name);
    else printf("Upserted record with id=%d not found.\n", upsert_key_update);

    found = bptree_get(tree, &new_rec->id); // Key 10
    if (found) printf("Found upserted record: id=%d, name=%s\n", found->id, found->name);
    else printf("Upserted record with id=%d not found.\n", new_rec->id);

    /* Check invariants of the tree. */
    printf("\n--- Checking Invariants ---\n");
    if (bptree_check_invariants(tree)) {
        printf("Tree invariants check passed.\n");
    } else {
        fprintf(stderr, "Error: Tree invariants check failed!\n");
        // Consider aborting or detailed logging if this happens
    }

    /* Get and print tree statistics. */
    printf("\n--- Getting Statistics ---\n");
    bptree_stats stats = bptree_get_stats(tree);
    printf("Tree stats: count=%d, height=%d, node_count=%d\n", stats.count, stats.height, stats.node_count);

    // --- Bulk Load ---
    // Note: Bulk load REPLACES the current tree contents.
    // We must free the records currently in the tree *before* bulk loading.
    printf("\n--- Preparing for Bulk Load ---\n");
    free_all_records(tree); // Free existing records before replacing the tree

    printf("\n--- Performing Bulk Load ---\n");
    // Create data dynamically for bulk load
    const int bulk_count = 6;
    record_t *bulk_records[bulk_count]; // Array of pointers
    int bulk_keys[bulk_count];
    bptree_value_t bulk_values[bulk_count]; // Array of record_t* (which matches BPTREE_VALUE_TYPE)

    bool bulk_alloc_ok = true;
    for (int i = 0; i < bulk_count; ++i) {
        int id = 20 + i;
        char name_buf[10];
        snprintf(name_buf, sizeof(name_buf), "Bulk %c", 'A' + i);
        bulk_records[i] = create_record(id, name_buf);
        if (!bulk_records[i]) {
            fprintf(stderr, "Failed to allocate record %d for bulk load.\n", i);
            // Free already allocated bulk records
            for(int j = 0; j < i; ++j) free(bulk_records[j]);
            bulk_alloc_ok = false;
            break;
        }
        bulk_keys[i] = bulk_records[i]->id;
        bulk_values[i] = bulk_records[i]; // Assign the pointer
    }

    if (bulk_alloc_ok) {
        // Ensure data is sorted for bulk load
        // (It is sorted by design here, but a real application might need qsort)
        status = bptree_bulk_load(tree, bulk_keys, bulk_values, bulk_count);
        printf("Bulk load status: %s\n", status_to_string(status));

        if (status != BPTREE_OK) {
            fprintf(stderr, "Bulk load failed. Cleaning up allocated bulk records.\n");
            // If bulk load failed, the tree state is undefined/empty, but we still own the allocated records
            for (int i = 0; i < bulk_count; ++i) free(bulk_records[i]);
            bptree_free(tree);
            return EXIT_FAILURE;
        }

         /* Iterate through all records again after the bulk load. */
        printf("\n--- Iterating After Bulk Load ---\n");
        iter = bptree_iterator_new(tree);
        if (iter) {
            record_t *item;
            while ((item = bptree_iterator_next(iter)) != NULL) {
                printf("  id=%d, name=%s\n", item->id, item->name);
            }
            bptree_iterator_destroy(iter);
        }

        /* Assert that the tree is still valid after the bulk load. */
        printf("\n--- Asserting Invariants After Bulk Load ---\n");
        // bptree_assert_invariants(tree); // Use assert only if you want program termination on failure
        if (bptree_check_invariants(tree)) {
            printf("Tree invariants check passed after bulk load.\n");
        } else {
             fprintf(stderr, "Error: Tree invariants check failed after bulk load!\n");
        }


        /* Get and print tree statistics after the bulk load. */
        printf("\n--- Statistics After Bulk Load ---\n");
        stats = bptree_get_stats(tree);
        printf("Tree stats: count=%d, height=%d, node_count=%d\n", stats.count, stats.height, stats.node_count);

    } else {
        // Bulk allocation failed earlier
        bptree_free(tree); // Free the empty tree structure
        return EXIT_FAILURE;
    }

    // --- Cleanup ---
    printf("\n--- Final Cleanup ---\n");
    // Free all records currently in the tree (from the bulk load)
    free_all_records(tree);

    // Free the tree structure itself
    bptree_free(tree);
    printf("B+ Tree freed.\n");

    printf("\n--- Example Finished Successfully ---\n");
    return EXIT_SUCCESS;
}