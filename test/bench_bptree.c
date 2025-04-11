/**
 * @file bench_bptree.c
 * @brief Performance benchmark for the B+ tree library (bptree.h).
 *
 * This program measures the performance of various B+ tree operations:
 * - Random and sequential insertions.
 * - Random and sequential searches.
 * - Random and sequential deletions.
 * - Leaf node iteration.
 * - Range queries.
 *
 * Benchmark parameters (number of items `N`, tree order `MAX_ITEMS`, random seed `SEED`)
 * can be configured via environment variables.
 *
 * @version 0.4.0-beta
 */

#define BPTREE_IMPLEMENTATION  // Include the bptree implementation

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "bptree.h"  // Include the B+ tree library

/** @brief Global flag to enable/disable debug logging from the bptree library. */
const bool debug_enabled = false;

/**
 * @brief Comparison function for B+ tree keys (numeric).
 *
 * Passed to `bptree_create` for ordering keys within the tree.
 * Assumes the default `bptree_key_t` (int64_t).
 *
 * @param a Pointer to the first key.
 * @param b Pointer to the second key.
 * @return -1 if *a < *b, 0 if *a == *b, 1 if *a > *b.
 */
int compare_keys(const bptree_key_t *a, const bptree_key_t *b) {
    return (*a < *b) ? -1 : ((*a > *b) ? 1 : 0);
}

/**
 * @brief qsort-compatible comparison function for B+ tree keys (numeric).
 *
 * Used for sorting the key array before sequential benchmarks.
 * Assumes the default `bptree_key_t` (int64_t).
 *
 * @param a Pointer to the first key (as `void*`).
 * @param b Pointer to the second key (as `void*`).
 * @return -1 if *a < *b, 0 if *a == *b, 1 if *a > *b.
 */
int compare_keys_qsort(const void *a, const void *b) {
    const bptree_key_t *ka = (const bptree_key_t *)a;
    const bptree_key_t *kb = (const bptree_key_t *)b;
    return (*ka < *kb) ? -1 : ((*ka > *kb) ? 1 : 0);
}

/**
 * @def BENCH(label, count, code_block)
 * @brief Macro to measure and print the execution time of a code block.
 *
 * Executes the `code_block` `count` times, measures the elapsed wall-clock time
 * using `clock()`, and prints the total time and average time per iteration.
 *
 * @param label A string label for the benchmark being run.
 * @param count The number of iterations to perform.
 * @param code_block The code to benchmark. The loop variable `bench_i` is available inside.
 */
#define BENCH(label, count, code_block)                                                           \
    do {                                                                                          \
        clock_t start = clock();                                                                  \
        for (int bench_i = 0; bench_i < (count); bench_i++) {                                     \
            code_block;                                                                           \
        }                                                                                         \
        clock_t end = clock();                                                                    \
        double elapsed = (double)(end - start) / CLOCKS_PER_SEC;                                  \
        printf("%s: %d iterations in %f sec (%f sec per iteration)\n", (label), (count), elapsed, \
               elapsed / (count));                                                                \
    } while (0)

/**
 * @brief Shuffles an array of void pointers using the Fisher-Yates algorithm.
 *
 * @param array The array of pointers to shuffle.
 * @param n The number of elements in the array.
 */
void shuffle(void **array, const int n) {
    for (int i = n - 1; i > 0; i--) {
        const int j = rand() % (i + 1);
        void *temp = array[i];
        array[i] = array[j];
        array[j] = temp;
    }
}

/**
 * @brief Shuffles a key array and a pointer array together, maintaining pairing.
 *
 * Uses the Fisher-Yates algorithm, applying the same swap indices to both arrays.
 *
 * @param keys The array of keys to shuffle.
 * @param pointers The array of pointers to shuffle in parallel.
 * @param n The number of elements in both arrays.
 */
void shuffle_pair(bptree_key_t *keys, void **pointers, const int n) {
    for (int i = n - 1; i > 0; i--) {
        const int j = rand() % (i + 1);

        // Swap keys
        const bptree_key_t temp_key = keys[i];
        keys[i] = keys[j];
        keys[j] = temp_key;

        // Swap pointers
        void *temp_ptr = pointers[i];
        pointers[i] = pointers[j];
        pointers[j] = temp_ptr;
    }
}

/**
 * @brief Main entry point for the benchmark program.
 *
 * Performs the following steps:
 * 1. Reads configuration from environment variables (`SEED`, `MAX_ITEMS`, `N`).
 * 2. Initializes the random seed.
 * 3. Allocates and prepares key and value arrays.
 * 4. Runs benchmark loops for:
 * - Random Insertion
 * - Sequential Insertion
 * - Random Search
 * - Sequential Search
 * - Leaf Iteration
 * - Random Deletion
 * - Sequential Deletion
 * - Range Search
 * 5. Prints timing results for each benchmark using the `BENCH` macro.
 * 6. Frees allocated memory.
 *
 * @param void Takes no arguments.
 * @return EXIT_SUCCESS on successful execution, EXIT_FAILURE on allocation failure.
 */
int main(void) {
    // --- Configuration ---
    int seed = getenv("SEED") ? atoi(getenv("SEED")) : (int)time(NULL);
    // MAX_ITEMS corresponds to bptree's max_keys parameter (tree order - 1)
    int max_keys = getenv("MAX_ITEMS") ? atoi(getenv("MAX_ITEMS")) : 32;
    int N = getenv("N") ? atoi(getenv("N")) : 1000000;  // Number of items
    if (N <= 0) {
        fprintf(stderr, "Invalid N value (%d); defaulting to 1000000\n", N);
        N = 1000000;
    }
    printf("SEED=%d, MAX_ITEMS=%d, N=%d\n", seed, max_keys, N);
    srand(seed);  // Seed the random number generator

    // --- Data Preparation ---
    int *vals = malloc(N * sizeof(int));                          // Actual integer values
    bptree_key_t *keys_array = malloc(N * sizeof(bptree_key_t));  // Keys (same as values here)
    void **pointers = malloc(N * sizeof(void *));                 // Pointers to store in the tree
    if (!vals || !keys_array || !pointers) {
        perror("Allocation failed for base data arrays");
        exit(EXIT_FAILURE);  // Use EXIT_FAILURE for error exit
    }
    // Initialize data sequentially
    for (int i = 0; i < N; i++) {
        vals[i] = i;
        keys_array[i] = (bptree_key_t)i;  // Keys are just the index
        pointers[i] = &vals[i];           // Tree stores pointers to the integers
    }

    // Create copies for shuffling and sorting without modifying the original order
    bptree_key_t *keys_copy = malloc(N * sizeof(bptree_key_t));
    void **pointers_copy = malloc(N * sizeof(void *));
    if (!keys_copy || !pointers_copy) {
        perror("Allocation failed for copy arrays");
        free(vals);
        free(keys_array);
        free(pointers);      // Cleanup previous allocations
        exit(EXIT_FAILURE);  // Use EXIT_FAILURE for error exit
    }

    // --- Benchmark: Random Insertion ---
    // Prepare shuffled data
    memcpy(keys_copy, keys_array, N * sizeof(bptree_key_t));
    memcpy(pointers_copy, pointers, N * sizeof(void *));
    shuffle_pair(keys_copy, pointers_copy, N);  // Shuffle keys and pointers together
    {
        bptree *tree = bptree_create(max_keys, compare_keys, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(EXIT_FAILURE);
        }  // Use EXIT_FAILURE

        BENCH("Insertion (rand)", N, {
            // Insert shuffled keys
            const bptree_status stat =
                bptree_put(tree, &keys_copy[bench_i], pointers_copy[bench_i]);
            assert(stat == BPTREE_OK);  // Ensure insertion succeeds
        });

        bptree_free(tree);  // Free the tree after the benchmark
    }

    // --- Benchmark: Sequential Insertion ---
    {
        bptree *tree = bptree_create(max_keys, compare_keys, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(EXIT_FAILURE);
        }  // Use EXIT_FAILURE

        BENCH("Insertion (seq)", N, {
            // Insert keys sequentially (0, 1, 2, ...)
            const bptree_status stat = bptree_put(tree, &keys_array[bench_i], pointers[bench_i]);
            assert(stat == BPTREE_OK);
        });

        bptree_free(tree);
    }

    // --- Benchmark: Random Search ---
    // Need a tree populated with data first. Use shuffled data for population.
    memcpy(keys_copy, keys_array, N * sizeof(bptree_key_t));
    memcpy(pointers_copy, pointers, N * sizeof(void *));
    shuffle_pair(keys_copy, pointers_copy, N);  // Shuffle again for search order
    {
        bptree *tree = bptree_create(max_keys, compare_keys, debug_enabled);
        if (!tree) exit(EXIT_FAILURE);  // Use EXIT_FAILURE
        // Populate the tree (using shuffled data from previous run is fine)
        for (int i = 0; i < N; i++) {
            const bptree_status stat =
                bptree_put(tree, &keys_array[i], pointers[i]);  // Populate sequentially
            assert(stat == BPTREE_OK);
        }

        // Benchmark searching for keys in random order
        BENCH("Search (rand)", N, {
            bptree_value_t res;
            // Search for keys in the shuffled order
            const bptree_status st = bptree_get(tree, &keys_copy[bench_i], &res);
            assert(st == BPTREE_OK);
            assert(res == pointers_copy[bench_i]);  // Verify correct pointer found
        });
        bptree_free(tree);
    }

    // --- Benchmark: Sequential Search ---
    {
        bptree *tree = bptree_create(max_keys, compare_keys, debug_enabled);
        if (!tree) exit(EXIT_FAILURE);  // Use EXIT_FAILURE
        // Populate the tree sequentially
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_put(tree, &keys_array[i], pointers[i]);
            assert(stat == BPTREE_OK);
        }

        // Benchmark searching for keys in sequential order
        BENCH("Search (seq)", N, {
            bptree_value_t res;
            const bptree_status st = bptree_get(tree, &keys_array[bench_i], &res);
            assert(st == BPTREE_OK);
            assert(res == pointers[bench_i]);  // Verify correct pointer
        });
        bptree_free(tree);
    }

    // --- Benchmark: Leaf Iteration ---
    {
        bptree *tree = bptree_create(max_keys, compare_keys, debug_enabled);
        if (!tree) exit(EXIT_FAILURE);  // Use EXIT_FAILURE
        // Populate sequentially
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_put(tree, &keys_array[i], pointers[i]);
            assert(stat == BPTREE_OK);
        }

        int iter_total = 0;
        int iterations = (N > 10000) ? 100 : 1000;  // Fewer iterations for large N
        printf("Running iterator benchmark with %d iterations...\n", iterations);
        BENCH("Iterator", iterations, {
            // Find the first leaf node
            bptree_node *leaf = tree->root;
            while (leaf && !leaf->is_leaf) {  // Added null check for safety
                leaf = bptree_node_children(leaf, tree->max_keys)[0];
            }
            int count = 0;
            // Iterate through all leaves using the 'next' pointer
            for (bptree_node *cur = leaf; cur != NULL; cur = cur->next) {
                count += cur->num_keys;
                // Optional: Access data to prevent optimization removing the loop
                // if (cur->num_keys > 0) { volatile bptree_key_t k = bptree_node_keys(cur)[0];
                // (void)k; }
            }
            iter_total += count;  // Accumulate total items found across iterations
        });
        // Check if the total count matches expectation
        if (iter_total != iterations * tree->count) {
            fprintf(stderr, "Iterator Warning: Total iterated %d != expected %d\n", iter_total,
                    iterations * tree->count);
        }
        printf("Total iterated elements over %d iterations: %d (expected %d per iteration)\n",
               iterations, iter_total, tree->count);
        bptree_free(tree);
    }

    // --- Benchmark: Random Deletion ---
    // Prepare a shuffled order for deletions
    int *deletion_order = malloc(N * sizeof(int));
    if (!deletion_order) {
        perror("Allocation failed for deletion_order");
        exit(EXIT_FAILURE);
    }  // Use EXIT_FAILURE
    for (int i = 0; i < N; i++) {
        deletion_order[i] = i;
    }
    // Simple shuffle for deletion order (using indices into keys_array)
    for (int i = N - 1; i > 0; i--) {
        int j = rand() % (i + 1);
        int tmp = deletion_order[i];
        deletion_order[i] = deletion_order[j];
        deletion_order[j] = tmp;
    }

    {
        bptree *tree = bptree_create(max_keys, compare_keys, debug_enabled);
        if (!tree) exit(EXIT_FAILURE);  // Use EXIT_FAILURE
        // Populate sequentially first
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_put(tree, &keys_array[i], pointers[i]);
            assert(stat == BPTREE_OK);
        }

        // Benchmark deletion in random order
        BENCH("Deletion (rand)", N, {
            int idx = deletion_order[bench_i];  // Index into the original sequential keys_array
            const bptree_status stat = bptree_remove(tree, &keys_array[idx]);
            assert(stat == BPTREE_OK);  // Ensure deletion succeeds
        });
        assert(tree->count == 0);  // Verify the tree is empty after deleting all
        bptree_free(tree);
    }
    free(deletion_order);  // Free the deletion order array

    // --- Benchmark: Sequential Deletion ---
    {
        bptree *tree = bptree_create(max_keys, compare_keys, debug_enabled);
        if (!tree) exit(EXIT_FAILURE);  // Use EXIT_FAILURE
        // Populate sequentially
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_put(tree, &keys_array[i], pointers[i]);
            assert(stat == BPTREE_OK);
        }

        // Benchmark deletion in sequential order (0, 1, 2, ...)
        BENCH("Deletion (seq)", N, {
            const bptree_status stat = bptree_remove(tree, &keys_array[bench_i]);
            assert(stat == BPTREE_OK);
        });
        assert(tree->count == 0);  // Verify the tree is empty
        bptree_free(tree);
    }

    // --- Benchmark: Range Search ---
    // Use a sequentially populated tree for predictable ranges
    memcpy(keys_copy, keys_array, N * sizeof(bptree_key_t));  // Use sorted keys
    // pointers_copy corresponds to keys_copy if populated sequentially before sort, which we did
    // above.
    {
        bptree *tree = bptree_create(max_keys, compare_keys, debug_enabled);
        if (!tree) exit(EXIT_FAILURE);  // Use EXIT_FAILURE
        // Populate sequentially
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_put(tree, &keys_array[i], pointers[i]);
            assert(stat == BPTREE_OK);
        }

        // Benchmark range queries of a fixed size
        BENCH("Range Search (seq)", N, {
            const int delta = 100;  // Size of range to query
            const int idx =
                bench_i % (N - delta > 0 ? N - delta : 1);  // Ensure start index is valid
            int end_idx = idx + delta;
            if (end_idx >= N) end_idx = N - 1;  // Ensure end index is valid

            int found_count = 0;
            bptree_value_t *res = NULL;
            // Query range from keys_array[idx] to keys_array[end_idx]
            const bptree_status st =
                bptree_get_range(tree, &keys_array[idx], &keys_array[end_idx], &res, &found_count);
            assert(st == BPTREE_OK);
            // Verify count is roughly correct (could be off by one depending on implementation
            // details of range) The range is inclusive, so the expected count is end_idx - idx + 1
            int expected_count = end_idx - idx + 1;
            if (expected_count < 0) expected_count = 0;  // Handle edge case N=0 or delta > N
            assert(found_count == expected_count);
            free(res);  // Free the result array allocated by get_range
        });
        bptree_free(tree);
    }

    // --- Cleanup ---
    printf("Cleaning up benchmark data...\n");
    free(keys_copy);
    free(pointers_copy);
    free(keys_array);
    free(pointers);
    free(vals);

    printf("Benchmark finished.\n");
    return EXIT_SUCCESS;
}
