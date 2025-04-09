/**
 * @file example.c
 * @brief Example usage of the Bptree library.
 *
 * This example demonstrates how to create a B+ tree using both bptree_create
 * and bptree_create_config, insert dynamically allocated records, retrieve records,
 * perform range queries, iterate over the tree, remove records, upsert records,
 * check invariants, obtain tree statistics, and use convenience functions such as
 * bptree_contains and bptree_size.
 */

/* --- Preprocessor definitions --- */

/* Define BPTREE_IMPLEMENTATION before including bptree.h */
#define BPTREE_IMPLEMENTATION

/* Configure the key type to use int64_t */
#define BPTREE_KEY_TYPE_INT
#define BPTREE_INT_TYPE int64_t

/* Configure the value type as a pointer to a record */
#define BPTREE_VALUE_TYPE struct record *

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bptree.h"  // Must follow the above macro definitions

/** Global flag for debug logging */
const bool debug_enabled = false;

/**
 * @brief Structure representing a record.
 */
typedef struct record {
    bptree_key_t id; /**< Unique record ID */
    char name[32];   /**< Record name */
} record_t;

/**
 * @brief Comparison function for integer keys.
 *
 * @param a Pointer to the first key.
 * @param b Pointer to the second key.
 *
 * @return -1 if *a < *b, 1 if *a > *b, 0 if equal.
 */
static int record_compare(const bptree_key_t *a, const bptree_key_t *b) {
    return (*a < *b) ? -1 : ((*a > *b) ? 1 : 0);
}

/**
 * @brief Creates a record.
 *
 * @param id The record's key.
 * @param name The record's name.
 *
 * @return Pointer to a new record or NULL on failure.
 */
static record_t *create_record(bptree_key_t id, const char *name) {
    record_t *rec = malloc(sizeof(record_t));
    if (!rec) {
        perror("Failed to allocate memory for record");
        return NULL;
    }
    rec->id = id;
    strncpy(rec->name, name, sizeof(rec->name) - 1);
    rec->name[sizeof(rec->name) - 1] = '\0';
    return rec;
}

/**
 * @brief Prints a key.
 *
 * @param key The key to print (assumed to be a long integer).
 */
static void print_key(bptree_key_t key) { printf("%ld", key); }

/**
 * @brief Converts a status code to a string.
 *
 * @param status A bptree_status value.
 *
 * @return A string representing the status.
 */
static const char *status_to_string(bptree_status status) {
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
        case BPTREE_BULK_LOAD_NOT_SORTED:
            return "BULK_LOAD_NOT_SORTED";
        case BPTREE_BULK_LOAD_DUPLICATE:
            return "BULK_LOAD_DUPLICATE";
        case BPTREE_INTERNAL_ERROR:
            return "INTERNAL_ERROR";
        default:
            return "UNKNOWN_STATUS";
    }
}

/**
 * @brief Frees all records stored in the tree.
 *
 * Iterates over the tree and frees each record.
 *
 * @param tree The B+ tree.
 */
static void free_all_records(bptree *tree) {
    printf("\n--- Freeing Records ---\n");
    bptree_iterator *iter = bptree_iterator_new(tree);
    if (!iter) {
        fprintf(stderr, "Failed to create iterator for freeing records.\n");
        return;
    }
    record_t *item = NULL;
    int count = 0;
    while (bptree_iterator_next(iter, (bptree_value_t *)&item) == BPTREE_OK) {
        free(item);
        count++;
    }
    printf("Freed %d records.\n", count);
    bptree_iterator_free(iter);
}

/**
 * @brief Main function demonstrating the usage of the B+ tree API.
 *
 * This function creates a B+ tree, inserts records, tests duplicate insertion,
 * retrieves a record, performs a range query, iterates over records,
 * removes a record, upserts records, checks invariants, prints statistics,
 * and finally frees all records and the tree.
 *
 * @return EXIT_SUCCESS if successful, EXIT_FAILURE otherwise.
 */
int main(void) {
    /* Create a B+ tree with a maximum of 4 keys per node */
    bptree *tree = bptree_create(4, record_compare, debug_enabled);
    if (!tree) {
        fprintf(stderr, "Error: failed to create B+ tree\n");
        return EXIT_FAILURE;
    }
    printf("B+ Tree created (max_keys=%d).\n", tree->max_keys);

    printf("Inserting records...\n");
    bptree_status status;
    record_t *rec_ptrs[9];

    rec_ptrs[0] = create_record(1, "Alice");
    rec_ptrs[1] = create_record(2, "Bob");
    rec_ptrs[2] = create_record(3, "Charlie");
    rec_ptrs[3] = create_record(6, "Frank");
    rec_ptrs[4] = create_record(7, "Grace");
    rec_ptrs[5] = create_record(8, "Heidi");
    rec_ptrs[6] = create_record(9, "Ivan");
    rec_ptrs[7] = create_record(4, "David");
    rec_ptrs[8] = create_record(5, "Eve");

    for (int i = 0; i < 9; ++i) {
        if (!rec_ptrs[i]) {
            fprintf(stderr, "Allocation failure for records. Cleaning up.\n");
            for (int j = 0; j < i; ++j) free(rec_ptrs[j]);
            bptree_free(tree);
            return EXIT_FAILURE;
        }
    }

    for (int i = 0; i < 9; ++i) {
        status = bptree_insert(tree, &rec_ptrs[i]->id, rec_ptrs[i]);
        printf("Inserting id=");
        print_key(rec_ptrs[i]->id);
        printf(": Status = %s\n", status_to_string(status));
        if (status != BPTREE_OK && status != BPTREE_DUPLICATE_KEY) {
            fprintf(stderr, "Error inserting record. Aborting.\n");
            free_all_records(tree);
            bptree_free(tree);
            for (int j = i; j < 9; ++j) free(rec_ptrs[j]);
            return EXIT_FAILURE;
        }
    }

    /* Test duplicate insertion */
    printf("Testing duplicate insert...\n");
    record_t *dup_rec = create_record(3, "Charlie Duplicate");
    if (!dup_rec) return EXIT_FAILURE;
    status = bptree_insert(tree, &dup_rec->id, dup_rec);
    if (status == BPTREE_DUPLICATE_KEY) {
        printf("Duplicate insert for id=");
        print_key(dup_rec->id);
        printf(" rejected (Status: %s).\n", status_to_string(status));
        free(dup_rec);
    } else {
        fprintf(stderr, "Error: duplicate key was not rejected.\n");
        free(dup_rec);
        free_all_records(tree);
        bptree_free(tree);
        return EXIT_FAILURE;
    }

    /* Retrieve a record */
    printf("Retrieving record with key 3...\n");
    const bptree_key_t search_key = 3;
    record_t *found = NULL;
    status = bptree_get_value(tree, &search_key, (bptree_value_t *)&found);
    if (status == BPTREE_OK && found) {
        printf("Found record: id=");
        print_key(found->id);
        printf(", name=%s\n", found->name);
    } else {
        printf("Record with key ");
        print_key(search_key);
        printf(" not found.\n");
    }

    /* Perform a range query */
    printf("Performing range query for keys in [4, 7]...\n");
    const bptree_key_t low = 4, high = 7;
    int range_count = 0;
    record_t **range_results = NULL;
    status = bptree_get_range(tree, &low, &high, (bptree_value_t **)&range_results, &range_count);
    if (status == BPTREE_OK && range_results) {
        printf("Range query: count = %d\n", range_count);
        for (int i = 0; i < range_count; i++) {
            record_t *r = range_results[i];
            if (r) {
                printf("  id=");
                print_key(r->id);
                printf(", name=%s\n", r->name);
            }
        }
        free(range_results);
    } else {
        printf("No records found in range [");
        print_key(low);
        printf(", ");
        print_key(high);
        printf("].\n");
    }

    /* Iterate over all records */
    printf("Iterating over records...\n");
    bptree_iterator *iter = bptree_iterator_new(tree);
    if (iter) {
        record_t *item = NULL;
        while (bptree_iterator_next(iter, (bptree_value_t *)&item) == BPTREE_OK) {
            printf("  id=");
            print_key(item->id);
            printf(", name=%s\n", item->name);
        }
        bptree_iterator_free(iter);
    } else {
        printf("Iterator creation failed.\n");
    }

    /* Remove a record */
    printf("Removing record with key 2...\n");
    const bptree_key_t remove_key = 2;
    record_t *record_to_remove = NULL;
    status = bptree_get_value(tree, &remove_key, (bptree_value_t *)&record_to_remove);
    if (status == BPTREE_OK && record_to_remove) {
        status = bptree_remove(tree, &remove_key);
        printf("Removing id=");
        print_key(remove_key);
        printf(": Status = %s\n", status_to_string(status));
        if (status == BPTREE_OK) {
            printf("Record removed successfully.\n");
            free(record_to_remove);
        } else {
            fprintf(stderr, "Failed to remove record.\n");
        }
    } else {
        printf("Record with key ");
        print_key(remove_key);
        printf(" not found, cannot remove.\n");
    }

    /* Attempt to retrieve the removed record */
    printf("Retrieving removed record with key 2...\n");
    found = NULL;
    status = bptree_get_value(tree, &remove_key, (bptree_value_t *)&found);
    if (status == BPTREE_OK && found) {
        printf("Error: Removed record still found: id=");
        print_key(found->id);
        printf(", name=%s\n", found->name);
    } else {
        printf("Record with key ");
        print_key(remove_key);
        printf(" not found (as expected).\n");
    }

    /* Upsert: update an existing record */
    printf("Performing upsert (update) for key 3...\n");
    const bptree_key_t upsert_key = 3;
    record_t *old_record = NULL;
    status = bptree_get_value(tree, &upsert_key, (bptree_value_t *)&old_record);
    record_t *updated_rec = create_record(upsert_key, "Charlie Updated");
    if (!updated_rec) return EXIT_FAILURE;
    status = bptree_upsert(tree, &updated_rec->id, updated_rec);
    printf("Upserting id=");
    print_key(updated_rec->id);
    printf(": Status = %s\n", status_to_string(status));
    if (status == BPTREE_OK) {
        printf("Upsert successful.\n");
        if (old_record && old_record != updated_rec) {
            printf("Freeing old record (id=");
            print_key(old_record->id);
            printf(").\n");
            free(old_record);
        }
    } else {
        fprintf(stderr, "Upsert failed.\n");
        free(updated_rec);
    }

    /* Upsert: insert a new record */
    printf("Performing upsert (insert) for key 10...\n");
    record_t *new_rec = create_record(10, "Judy");
    if (!new_rec) return EXIT_FAILURE;
    status = bptree_upsert(tree, &new_rec->id, new_rec);
    printf("Upserting id=");
    print_key(new_rec->id);
    printf(": Status = %s\n", status_to_string(status));
    if (status != BPTREE_OK) {
        fprintf(stderr, "Upsert insert failed.\n");
        free(new_rec);
    }

    /* Retrieve upserted records */
    printf("Retrieving upserted records...\n");
    found = NULL;
    status = bptree_get_value(tree, &upsert_key, (bptree_value_t *)&found);
    if (status == BPTREE_OK && found) {
        printf("Found record: id=");
        print_key(found->id);
        printf(", name=%s\n", found->name);
    } else {
        printf("Record with key ");
        print_key(upsert_key);
        printf(" not found.\n");
    }
    found = NULL;
    status = bptree_get_value(tree, &new_rec->id, (bptree_value_t *)&found);
    if (status == BPTREE_OK && found) {
        printf("Found record: id=");
        print_key(found->id);
        printf(", name=%s\n", found->name);
    } else {
        printf("Record with key ");
        print_key(new_rec->id);
        printf(" not found.\n");
    }

    /* Check invariants and show statistics */
    printf("Checking tree invariants...\n");
    if (bptree_check_invariants(tree))
        printf("Tree invariants OK.\n");
    else
        fprintf(stderr, "Tree invariants violated!\n");

    bptree_stats stats = bptree_get_stats(tree);
    printf("Tree stats: count=%d, height=%d, node_count=%d\n", stats.count, stats.height,
           stats.node_count);

    /* Demonstrate API usage examples inline */
    printf("\n--- B+ Tree API Examples ---\n");

    /* Check if the tree contains a key */
    if (bptree_contains(tree, &upsert_key))
        printf("Tree contains key ");
    else
        printf("Tree does not contain key ");
    print_key(upsert_key);
    printf(".\n");

    /* Print tree size */
    printf("Tree size: %d records.\n", bptree_size(tree));

    /* Create a tree via configuration */
    bptree_config cfg;
    cfg.max_keys = 4;
    cfg.compare = record_compare;
    cfg.enable_debug = true;
    cfg.alloc = NULL;
    cfg.free = NULL;
    cfg.realloc = NULL;
    cfg.allocator_ctx = NULL;
    bptree *tree2 = bptree_create_config(&cfg);
    if (tree2) {
        printf("Created tree with bptree_create_config (max_keys=%d).\n", cfg.max_keys);
        bptree_free(tree2);
    } else {
        printf("Failed to create tree via bptree_create_config.\n");
    }

    /* Iterate over the tree */
    iter = bptree_iterator_new(tree);
    if (iter) {
        printf("Iterating over records:\n");
        while (bptree_iterator_has_next(iter)) {
            record_t *item = NULL;
            if (bptree_iterator_next(iter, (bptree_value_t *)&item) == BPTREE_OK) {
                printf("  Record: id=");
                print_key(item->id);
                printf(", name=%s\n", item->name);
            }
        }
        bptree_iterator_free(iter);
    } else {
        printf("Failed to create iterator for API examples.\n");
    }

    /* Free all records and the tree */
    printf("Freeing all records and the tree...\n");
    free_all_records(tree);
    bptree_free(tree);
    printf("B+ Tree freed.\n");

    return EXIT_SUCCESS;
}
