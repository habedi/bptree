/**
 * @file example.c
 * @brief Example usage of the B+ tree library (bptree.h).
 *
 * This program demonstrates basic operations such as creating a tree,
 * inserting records (handling duplicates), retrieving records, performing range queries,
 * removing records, and updating records using the bptree library.
 * It uses numeric keys and stores pointers to a custom `record_t` struct as values.
 *
 * @version 0.4.1-beta
 */

#define BPTREE_IMPLEMENTATION              // Include implementation of B+ tree
#define BPTREE_VALUE_TYPE struct record *  // Configure tree to store pointers to records
#include <assert.h>                        // For assertions
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Include the B+ tree library header
#include "bptree.h"

/** @brief Global flag to enable/disable debug logging from the bptree library. */
const bool debug_enabled = false;

/**
 * @brief Structure for the data records stored as values in the B+ tree.
 *
 * In this example, the B+ tree is configured via `BPTREE_VALUE_TYPE`
 * to store pointers (`struct record *`) to instances of this structure.
 */
typedef struct record {
    /** @brief The numeric key used for indexing in the B+ tree. */
    bptree_key_t id;
    /** @brief A simple data field associated with the record. */
    char name[32];
    // Other data fields could be added here...
} record_t;

/**
 * @brief Comparison function for B+ tree keys (required by bptree_create).
 *
 * This function compares two keys of type `bptree_key_t`. Since this example uses
 * the default numeric key type (`int64_t`), it performs a standard numeric comparison.
 *
 * @param a Pointer to the first key.
 * @param b Pointer to the second key.
 * @return -1 if *a < *b, 0 if *a == *b, 1 if *a > *b.
 */
static int record_compare(const bptree_key_t *a, const bptree_key_t *b) {
    // Assumes BPTREE_NUMERIC_TYPE is defined (default is int64_t)
    return (*a < *b) ? -1 : ((*a > *b) ? 1 : 0);
}

/**
 * @brief Allocates and initializes a new record structure.
 *
 * @param id The key (ID) for the new record.
 * @param name The name string to store in the record.
 * @return A pointer to the newly allocated record_t, or NULL on allocation failure.
 * @note The caller is responsible for freeing the returned pointer when done.
 */
static record_t *create_record(const bptree_key_t id, const char *name) {
    record_t *rec = malloc(sizeof(record_t));
    if (!rec) {
        perror("Failed to allocate memory for record");
        return NULL;
    }
    rec->id = id;
    // Use strncpy for safety against buffer overflows
    strncpy(rec->name, name, sizeof(rec->name) - 1);
    rec->name[sizeof(rec->name) - 1] = '\0';  // Ensure null termination
    return rec;
}

/**
 * @brief Helper function to print a B+ tree key to standard output.
 *
 * Adjust the printf format specifier (`%lld`) if `BPTREE_NUMERIC_TYPE` is changed
 * from its default (`int64_t`).
 *
 * @param key The key to print.
 */
static void print_key(const bptree_key_t key) {
    printf("%lld", (long long)key);  // Use %lld for int64_t
}

/**
 * @brief Converts a bptree_status enum value to a human-readable string.
 *
 * Useful for printing diagnostic messages based on function return codes.
 *
 * @param status The status enum value returned by a bptree function.
 * @return A constant string representation of the status.
 */
static const char *status_to_string(const bptree_status status) {
    switch (status) {
        case BPTREE_OK:
            return "OK";
        case BPTREE_DUPLICATE_KEY:
            return "DUPLICATE_KEY";
        case BPTREE_KEY_NOT_FOUND:
            return "KEY_NOT_FOUND";
        case BPTREE_ALLOCATION_FAILURE:
            return "ALLOCATION_FAILURE";
        case BPTREE_INVALID_ARGUMENT:
            return "INVALID_ARGUMENT";
        case BPTREE_INTERNAL_ERROR:
            return "INTERNAL_ERROR";
        default:
            return "UNKNOWN_STATUS";
    }
}

/**
 * @brief Frees memory associated with records stored in the tree and the tree itself.
 *
 * This function specifically handles the case where the tree stores pointers
 * to dynamically allocated `record_t` structs (as defined by `BPTREE_VALUE_TYPE`).
 * It iterates through all leaf nodes, retrieves the stored pointers, frees the
 * memory pointed to by them, and finally calls `bptree_free()` to release the
 * tree's internal node memory.
 *
 * @param tree Pointer to the B+ tree to clean up.
 * @warning This function assumes `bptree_value_t` is `record_t*` and that
 * the pointers stored are valid results from `malloc` or NULL.
 */
static void cleanup_records_and_tree(bptree *tree) {
    printf("Cleaning up records and tree...\n");
    if (!tree) {
        printf("Tree pointer is NULL. Nothing to clean up.\n");
        return;
    }
    if (!tree->root) {
        fprintf(stderr, "Warning: Tree has no root node.\n");
        free(tree);  // Free tree struct itself if root is null
        return;
    }

    // Only iterate if there are records potentially stored
    if (tree->count > 0) {
        printf("Iterating through leaves to free %d records...\n", tree->count);
        // 1. Find the first leaf node
        bptree_node *leaf = tree->root;
        while (leaf && !leaf->is_leaf) {
            bptree_node **children = bptree_node_children(leaf, tree->max_keys);
            // Descend to the leftmost child
            if (leaf->num_keys >= 0 && children && children[0]) {
                leaf = children[0];
            } else {
                fprintf(stderr,
                        "Error: Corrupted internal node or missing child[0] during cleanup "
                        "traversal.\n");
                leaf = NULL;  // Stop traversal
                break;
            }
        }

        if (!leaf) {
            fprintf(
                stderr,
                "Error: Could not find the first leaf node for cleanup. Records may be leaked.\n");
        } else {
            // 2. Iterate through all leaf nodes using the 'next' pointer
            int freed_count = 0;
            bptree_node *current_leaf = leaf;
            while (current_leaf) {
                assert(current_leaf->is_leaf);  // Should always be leaf here
                const bptree_value_t *values = bptree_node_values(current_leaf, tree->max_keys);
                for (int i = 0; i < current_leaf->num_keys; i++) {
                    record_t *rec_ptr = (record_t *)values[i];  // Cast from bptree_value_t
                    if (rec_ptr) {
                        free(rec_ptr);  // Free the actual record_t struct
                        freed_count++;
                    } else {
                        // Note: Depending on usage, NULL values might be valid.
                        // fprintf(stderr, "Warning: Found NULL record pointer in leaf node during
                        // cleanup.\n");
                    }
                }
                current_leaf = current_leaf->next;
            }
            printf("Freed %d record structs.\n", freed_count);
            // Check if the number freed matches the tree's count (might differ if NULLs were
            // stored)
            if (freed_count != tree->count) {
                fprintf(stderr,
                        "Warning: Number of freed records (%d) may not match tree count (%d) if "
                        "NULL values were stored.\n",
                        freed_count, tree->count);
            }
        }
    } else {
        printf("Tree is empty, no records to free.\n");
    }

    // 3. Free the tree structure itself (nodes are freed recursively)
    bptree_free(tree);
    printf("B+ Tree structure freed.\n");
}

/**
 * @brief Main entry point for the B+ tree example program.
 *
 * Demonstrates various operations of the `bptree` library:
 * - Creates a B+ tree storing pointers to `record_t`.
 * - Inserts a set of initial records.
 * - Tests duplicate key insertion rejection.
 * - Retrieves a specific record using `bptree_get`.
 * - Performs a range query using `bptree_get_range`.
 * - Removes a record using `bptree_remove` (and frees the associated record data).
 * - Updates a record (implemented as remove + insert).
 * - Performs final checks and statistics gathering.
 * - Cleans up all allocated memory (records and tree structure).
 *
 * @param void Takes no arguments.
 * @return EXIT_SUCCESS on successful execution, EXIT_FAILURE otherwise.
 */
int main(void) {
    // Create the B+ tree with max_keys = 4 (Order 5)
    // Store pointers to record_t structs (BPTREE_VALUE_TYPE)
    bptree *tree = bptree_create(4, record_compare, debug_enabled);
    if (!tree) {
        fprintf(stderr, "Error: failed to create B+ tree\n");
        return EXIT_FAILURE;
    }
    printf("B+ Tree created (max_keys=%d).\n", tree->max_keys);

    // Array to hold pointers to allocated records for easier cleanup on error
    // Note: Size needs to be sufficient for all potential allocations before cleanup/removal
    record_t *allocated_records[20];
    int allocated_count = 0;
    bptree_status status;
    bool error_occurred = false;

    // --- Initial Data Insertion ---
    printf("Inserting records...\n");
    record_t *initial_data[] = {
        create_record(1, "Alice"), create_record(2, "Bob"),   create_record(3, "Charlie"),
        create_record(6, "Frank"), create_record(7, "Grace"), create_record(8, "Heidi"),
        create_record(9, "Ivan"),  create_record(4, "David"), create_record(5, "Eve")};
    const int num_initial = sizeof(initial_data) / sizeof(initial_data[0]);

    for (int i = 0; i < num_initial; ++i) {
        record_t *rec = initial_data[i];
        if (!rec) {
            fprintf(stderr, "Memory allocation failed for initial record %d. Aborting.\n", i);
            error_occurred = true;
            break;  // Exit insertion loop
        }
        // Track successfully allocated record *before* attempting insertion
        if (allocated_count < (int)(sizeof(allocated_records) / sizeof(allocated_records[0]))) {
            allocated_records[allocated_count++] = rec;
        } else {
            fprintf(stderr, "Error: allocated_records tracking array full. Aborting.\n");
            free(rec);  // Free the record we can't track
            error_occurred = true;
            break;
        }

        printf("Inserting id=");
        print_key(rec->id);
        printf(" ('%s')... ", rec->name);
        status = bptree_put(tree, &rec->id, rec);  // Store the pointer 'rec'
        printf("Status = %s\n", status_to_string(status));

        if (status != BPTREE_OK) {
            // Handle specific non-fatal errors if needed
            if (status != BPTREE_DUPLICATE_KEY) {  // Duplicate is not an error in this sequence
                fprintf(stderr, "Error inserting record id=");
                print_key(rec->id);
                printf(". Aborting.\n");
                error_occurred = true;
                // Note: The record 'rec' is already tracked in allocated_records for emergency
                // cleanup
                break;  // Exit insertion loop
            }
            // If it was a duplicate, it wasn't inserted, so we should free it and remove from
            // tracking
            else {
                printf("Note: Duplicate key %lld encountered during initial insert.\n",
                       (long long)rec->id);
                allocated_count--;  // Remove from tracking
                free(rec);          // Free the duplicate record data
            }
        }
    }

    // --- Test Duplicate Insert ---
    if (!error_occurred) {
        printf("Testing duplicate insert...\n");
        record_t *dup_rec = create_record(3, "Charlie Duplicate");
        if (!dup_rec) {
            fprintf(stderr, "Failed to allocate duplicate record.\n");
            error_occurred = true;
        } else {
            // Only track if allocation succeeds
            if (allocated_count < (int)(sizeof(allocated_records) / sizeof(allocated_records[0]))) {
                allocated_records[allocated_count++] = dup_rec;
            } else {
                fprintf(stderr, "Error: allocated_records tracking array full. Aborting.\n");
                free(dup_rec);
                error_occurred = true;
            }

            if (!error_occurred) {
                status = bptree_put(tree, &dup_rec->id, dup_rec);
                if (status == BPTREE_DUPLICATE_KEY) {
                    printf("Duplicate insert for id=");
                    print_key(dup_rec->id);
                    printf(" correctly rejected (Status: %s).\n", status_to_string(status));
                    // Since it wasn't inserted, remove from tracking and free it now
                    allocated_count--;
                    free(dup_rec);
                } else {
                    fprintf(stderr, "Error: duplicate key (id=3) was not rejected (Status: %s).\n",
                            status_to_string(status));
                    error_occurred = true;
                    // Keep dup_rec in allocated_records for final cleanup if error occurred
                }
            }
        }
    }

    // --- Test Get Operation ---
    if (!error_occurred) {
        printf("Retrieving record with key 3...\n");
        const bptree_key_t search_key = 3;
        record_t *found_rec_ptr = NULL;  // Variable to hold the retrieved pointer
        status = bptree_get(tree, &search_key, (bptree_value_t *)&found_rec_ptr);
        if (status == BPTREE_OK && found_rec_ptr) {
            printf("Found record: id=");
            print_key(found_rec_ptr->id);
            printf(", name=%s [Pointer: %p]\n", found_rec_ptr->name, (void *)found_rec_ptr);
        } else {
            printf("Record with key ");
            print_key(search_key);
            printf(" not found (Status: %s).\n", status_to_string(status));
            // If key 3 should exist at this point, this is an error
            // error_occurred = true;
        }
    }

    // --- Test Range Query ---
    if (!error_occurred) {
        printf("Performing range query for keys in [4, 7]...\n");
        const bptree_key_t low = 4, high = 7;
        int range_count = 0;
        record_t **range_results = NULL;  // Array of POINTERS to records
        status =
            bptree_get_range(tree, &low, &high, (bptree_value_t **)&range_results, &range_count);
        if (status == BPTREE_OK) {
            printf("Range query: count = %d\n", range_count);
            if (range_results) {
                for (int i = 0; i < range_count; i++) {
                    record_t *r = range_results[i];  // Get the pointer from the results array
                    if (r) {
                        printf("  id=");
                        print_key(r->id);
                        printf(", name=%s [Pointer: %p]\n", r->name, (void *)r);
                    } else {
                        fprintf(stderr, "Warning: NULL pointer found in range results.\n");
                    }
                }
                // IMPORTANT: Free the array allocated by bptree_get_range,
                // NOT the records themselves here (they are still potentially in the tree or
                // tracked separately).
                bptree_free_range_results((bptree_value_t *)range_results);
            } else if (range_count > 0) {
                fprintf(stderr, "Error: Range count > 0 but results pointer is NULL.\n");
                error_occurred = true;
            }
        } else {
            printf("Range query failed (Status: %s).\n", status_to_string(status));
            // Decide if this constitutes an error stopping execution
            // error_occurred = true;
        }
    }

    // --- Test Remove Operation ---
    if (!error_occurred) {
        printf("Removing record with key 2...\n");
        const bptree_key_t remove_key = 2;
        record_t *record_to_remove = NULL;  // Pointer to the record we might remove

        // 1. Get the pointer to the record BEFORE removing it from the tree
        status = bptree_get(tree, &remove_key, (bptree_value_t *)&record_to_remove);

        if (status == BPTREE_OK && record_to_remove) {
            // 2. Remove the entry (the pointer) from the tree
            status = bptree_remove(tree, &remove_key);
            printf("Removing id=");
            print_key(remove_key);
            printf(": Status = %s\n", status_to_string(status));

            if (status == BPTREE_OK) {
                printf("Record pointer removed successfully from tree.\n");
                // 3. Now that it's out of the tree, free the actual record memory
                printf("Freeing record data for id=%lld [Pointer: %p]\n", (long long)remove_key,
                       (void *)record_to_remove);
                // Find it in our tracking array and remove it (makes emergency cleanup simpler)
                for (int i = 0; i < allocated_count; ++i) {
                    if (allocated_records[i] == record_to_remove) {
                        allocated_records[i] =
                            allocated_records[allocated_count - 1];   // Overwrite with last
                        allocated_records[--allocated_count] = NULL;  // Clear last slot
                        break;
                    }
                }
                free(record_to_remove);
                record_to_remove = NULL;  // Avoid dangling pointer
            } else {
                fprintf(stderr, "Failed to remove record pointer from tree (Status: %s).\n",
                        status_to_string(status));
                error_occurred = true;
                // Do NOT free record_to_remove here, as it's potentially still in the tree /
                // tracked list
            }
        } else {
            printf("Record with key ");
            print_key(remove_key);
            printf(" not found, cannot remove (Get Status: %s).\n", status_to_string(status));
            // Decide if this is an error in this context
            // error_occurred = true;
        }

        // Verify removal
        printf("Verifying removal of record with key 2...\n");
        record_t *found_after_remove = NULL;
        status = bptree_get(tree, &remove_key, (bptree_value_t *)&found_after_remove);
        if (status == BPTREE_KEY_NOT_FOUND) {
            printf("Record with key ");
            print_key(remove_key);
            printf(" correctly not found after removal.\n");
        } else {
            fprintf(stderr, "Error: Removed record 2 still found (Get Status: %s)!\n",
                    status_to_string(status));
            error_occurred = true;
        }
    }

    // --- Test Update Operation (Remove + Insert) ---
    if (!error_occurred) {
        printf("Updating record with key 3 (remove then reinsert)...\n");
        const bptree_key_t update_key = 3;
        record_t *old_record_ptr = NULL;

        // 1. Get the pointer to the old record
        status = bptree_get(tree, &update_key, (bptree_value_t *)&old_record_ptr);
        if (status == BPTREE_OK && old_record_ptr) {
            printf("Found old record id=3 ('%s') [Pointer: %p]. Removing...\n",
                   old_record_ptr->name, (void *)old_record_ptr);
            // 2. Remove pointer from tree
            status = bptree_remove(tree, &update_key);
            if (status == BPTREE_OK) {
                printf("Old record pointer removed from tree. Freeing old record data...\n");
                // 3. Free the old record's memory (and remove from tracking)
                for (int i = 0; i < allocated_count; ++i) {  // Remove from tracking
                    if (allocated_records[i] == old_record_ptr) {
                        allocated_records[i] = allocated_records[allocated_count - 1];
                        allocated_records[--allocated_count] = NULL;
                        break;
                    }
                }
                free(old_record_ptr);
                old_record_ptr = NULL;

                // 4. Create the new record
                printf("Creating updated record for id=3...\n");
                record_t *updated_rec = create_record(update_key, "Charlie Updated");
                if (!updated_rec) {
                    fprintf(stderr, "Failed to allocate updated record for key 3.\n");
                    error_occurred = true;
                } else {
                    // Track allocation
                    if (allocated_count <
                        (int)(sizeof(allocated_records) / sizeof(allocated_records[0]))) {
                        allocated_records[allocated_count++] = updated_rec;
                    } else {
                        fprintf(stderr,
                                "Error: allocated_records tracking array full. Aborting.\n");
                        free(updated_rec);
                        error_occurred = true;
                    }

                    if (!error_occurred) {
                        // 5. Insert the new record pointer into the tree
                        printf("Inserting updated record id=3 ('%s')... ", updated_rec->name);
                        status = bptree_put(tree, &updated_rec->id, updated_rec);
                        printf("Status = %s\n", status_to_string(status));
                        if (status != BPTREE_OK) {
                            fprintf(stderr, "Failed to insert updated record for key 3.\n");
                            error_occurred = true;
                            // Keep updated_rec in allocated_records for emergency cleanup
                        } else {
                            printf("Record 3 updated successfully.\n");
                        }
                    }
                }
            } else {
                fprintf(stderr, "Failed to remove record id=3 for update (Status: %s).\n",
                        status_to_string(status));
                error_occurred = true;
                // Do not free old_record_ptr, it's still in the tree / tracked
            }
        } else {
            printf("Record with key ");
            print_key(update_key);
            printf(" not found; cannot update (Get Status: %s).\n", status_to_string(status));
            // error_occurred = true; // Decide if this is fatal
        }
    }

    // --- Test Final Insert ---
    if (!error_occurred) {
        printf("Inserting record with key 10...\n");
        record_t *new_rec = create_record(10, "Judy");
        if (!new_rec) {
            fprintf(stderr, "Failed to allocate record for key 10.\n");
            error_occurred = true;
        } else {
            // Track allocation
            if (allocated_count < (int)(sizeof(allocated_records) / sizeof(allocated_records[0]))) {
                allocated_records[allocated_count++] = new_rec;
            } else {
                fprintf(stderr, "Error: allocated_records tracking array full. Aborting.\n");
                free(new_rec);
                error_occurred = true;
            }

            if (!error_occurred) {
                printf("Inserting id=");
                print_key(new_rec->id);
                printf(" ('%s')... ", new_rec->name);
                status = bptree_put(tree, &new_rec->id, new_rec);
                printf("Status = %s\n", status_to_string(status));
                if (status != BPTREE_OK) {
                    fprintf(stderr, "Insert for key 10 failed.\n");
                    error_occurred = true;
                    // Keep new_rec in allocated_records for emergency cleanup
                }
            }
        }
    }

    // --- Final Checks ---
    if (!error_occurred) {
        printf("Retrieving record with key 3 after update...\n");
        record_t *found_rec_ptr = NULL;
        status = bptree_get(tree, &((bptree_key_t){3}), (bptree_value_t *)&found_rec_ptr);
        if (status == BPTREE_OK && found_rec_ptr) {
            printf("Found record: id=");
            print_key(found_rec_ptr->id);
            printf(", name=%s [Pointer: %p]\n", found_rec_ptr->name, (void *)found_rec_ptr);
            // Verify it's the updated one
            assert(strcmp(found_rec_ptr->name, "Charlie Updated") == 0);
        } else {
            printf("Record with key 3 not found after update (Status: %s).\n",
                   status_to_string(status));
            error_occurred = true;
        }

        printf("Retrieving record with key 10...\n");
        found_rec_ptr = NULL;
        status = bptree_get(tree, &((bptree_key_t){10}), (bptree_value_t *)&found_rec_ptr);
        if (status == BPTREE_OK && found_rec_ptr) {
            printf("Found record: id=");
            print_key(found_rec_ptr->id);
            printf(", name=%s [Pointer: %p]\n", found_rec_ptr->name, (void *)found_rec_ptr);
            assert(strcmp(found_rec_ptr->name, "Judy") == 0);
        } else {
            printf("Record with key 10 not found (Status: %s).\n", status_to_string(status));
            error_occurred = true;
        }

        printf("Checking tree invariants...\n");
        if (bptree_check_invariants(tree)) {
            printf("Tree invariants OK.\n");
        } else {
            fprintf(stderr, "Error: Tree invariants violated!\n");
            error_occurred = true;
        }

        const bptree_stats stats = bptree_get_stats(tree);
        printf("Tree stats: count=%d, height=%d, node_count=%d\n", stats.count, stats.height,
               stats.node_count);
        printf("Final tree size should be %d records.\n",
               tree->count);  // Use tree's internal count
    }

    // --- Cleanup ---
    if (error_occurred) {
        fprintf(stderr, "\n--- An error occurred during operations ---\n");
        // Attempt to free any records we allocated and tracked
        printf("Performing emergency cleanup of %d known allocated records...\n", allocated_count);
        for (int i = 0; i < allocated_count; ++i) {
            if (allocated_records[i]) {
                // Check if it was successfully removed during a test step, if possible
                // (Difficult without more state tracking) - Assume we should free it.
                free(allocated_records[i]);
                allocated_records[i] =
                    NULL;  // Avoid double free if the list contains duplicates by error
            }
        }
        // Attempt to free the tree structure, nodes might be inconsistent
        printf("Freeing potentially inconsistent tree structure...\n");
        bptree_free(tree);  // This might leak nodes if the structure is corrupted
        return EXIT_FAILURE;
    } else {
        // Normal cleanup: free records via tree traversal, then free tree structure
        cleanup_records_and_tree(tree);
    }

    printf("Example finished successfully.\n");
    return EXIT_SUCCESS;
}
