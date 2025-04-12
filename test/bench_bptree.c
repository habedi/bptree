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
 * @version 0.4.1-beta
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
 * - Range Search (Sequential/Random Start, Varying Sizes)
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
    // Ensure N is large enough for meaningful range tests
    if (N < 1000) {
        printf("Warning: N (%d) is small, range query benchmarks might be less meaningful.\n", N);
    }

    printf("SEED=%d, MAX_ITEMS=%d, N=%d\n", seed, max_keys, N);
    srand(seed);  // Seed the random number generator

    // --- Data Preparation ---
    int *vals = malloc(N * sizeof(int));  // Actual integer values
    bptree_key_t *keys_array =
        malloc(N * sizeof(bptree_key_t));          // Keys (used for sequential access)
    void **pointers = malloc(N * sizeof(void *));  // Pointers to store in the tree
    if (!vals || !keys_array || !pointers) {
        perror("Allocation failed for base data arrays");
        exit(EXIT_FAILURE);
    }
    // Initialize data sequentially (keys are 0 to N-1)
    for (int i = 0; i < N; i++) {
        vals[i] = i;
        keys_array[i] = (bptree_key_t)i;
        pointers[i] = &vals[i];
    }

    // Create copies for shuffling
    bptree_key_t *keys_copy = malloc(N * sizeof(bptree_key_t));
    void **pointers_copy = malloc(N * sizeof(void *));
    if (!keys_copy || !pointers_copy) {
        perror("Allocation failed for copy arrays");
        free(vals);
        free(keys_array);
        free(pointers);
        exit(EXIT_FAILURE);
    }

    // --- Benchmark: Random Insertion ---
    memcpy(keys_copy, keys_array, N * sizeof(bptree_key_t));
    memcpy(pointers_copy, pointers, N * sizeof(void *));
    shuffle_pair(keys_copy, pointers_copy, N);
    {
        bptree *tree = bptree_create(max_keys, compare_keys, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(EXIT_FAILURE);
        }
        BENCH("Insertion (rand)", N, {
            const bptree_status stat =
                bptree_put(tree, &keys_copy[bench_i], pointers_copy[bench_i]);
            assert(stat == BPTREE_OK);
        });
        bptree_free(tree);
    }

    // --- Benchmark: Sequential Insertion ---
    {
        bptree *tree = bptree_create(max_keys, compare_keys, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(EXIT_FAILURE);
        }
        BENCH("Insertion (seq)", N, {
            const bptree_status stat = bptree_put(tree, &keys_array[bench_i], pointers[bench_i]);
            assert(stat == BPTREE_OK);
        });
        bptree_free(tree);
    }

    // --- Prepare Tree for Search/Delete/Range Benchmarks ---
    printf("Populating tree for search/delete/range tests...\n");
    bptree *test_tree = bptree_create(max_keys, compare_keys, debug_enabled);
    if (!test_tree) {
        fprintf(stderr, "Failed to create tree for tests\n");
        exit(EXIT_FAILURE);
    }
    for (int i = 0; i < N; i++) {
        const bptree_status stat = bptree_put(test_tree, &keys_array[i], pointers[i]);
        if (stat != BPTREE_OK) {
            fprintf(stderr, "Failed to populate tree for tests at index %d (status: %d)\n", i,
                    stat);
            bptree_free(test_tree);
            exit(EXIT_FAILURE);
        }
    }
    printf("Tree populated with %d items.\n", test_tree->count);
    assert(test_tree->count == N);

    // --- Benchmark: Random Search ---
    memcpy(keys_copy, keys_array, N * sizeof(bptree_key_t));
    memcpy(pointers_copy, pointers, N * sizeof(void *));
    shuffle_pair(keys_copy, pointers_copy, N);
    BENCH("Search (rand)", N, {
        bptree_value_t res;
        const bptree_status st = bptree_get(test_tree, &keys_copy[bench_i], &res);
        assert(st == BPTREE_OK);
        assert(res == pointers_copy[bench_i]);
    });

    // --- Benchmark: Sequential Search ---
    BENCH("Search (seq)", N, {
        bptree_value_t res;
        const bptree_status st = bptree_get(test_tree, &keys_array[bench_i], &res);
        assert(st == BPTREE_OK);
        assert(res == pointers[bench_i]);
    });

    // --- Benchmark: Leaf Iteration ---
    int iter_total = 0;
    int iterations = (N > 10000) ? 100 : 1000;
    printf("Running iterator benchmark with %d iterations...\n", iterations);
    BENCH("Iterator", iterations, {
        bptree_node *leaf = test_tree->root;  // Use the already populated test_tree
        while (leaf && !leaf->is_leaf) {
            leaf = bptree_node_children(leaf, test_tree->max_keys)[0];
        }
        int count = 0;
        for (bptree_node *cur = leaf; cur != NULL; cur = cur->next) {
            count += cur->num_keys;
        }
        iter_total += count;
    });
    if (iter_total != iterations * test_tree->count) {
        fprintf(stderr, "Iterator Warning: Total iterated %d != expected %d\n", iter_total,
                iterations * test_tree->count);
    }
    printf("Total iterated elements over %d iterations: %d (expected %d per iteration)\n",
           iterations, iter_total, test_tree->count);

    // --- Benchmark: Range Search (Variations) ---
    printf("Running range search benchmarks...\n");
    // Use populated test_tree

    // Original: Sequential Start, Delta=100
    BENCH("Range Search (seq, d=100)", N, {
        const int delta = 100;
        const int max_start_idx = (N > delta) ? (N - delta) : 0;
        const int idx = (max_start_idx > 0) ? (bench_i % max_start_idx) : 0;
        int end_idx = idx + delta - 1;  // Adjust for inclusive range [idx, end_idx]
        if (end_idx >= N) end_idx = N - 1;
        if (idx > end_idx) end_idx = idx;  // Handle N <= delta case

        int found_count = 0;
        bptree_value_t *res = NULL;
        const bptree_status st =
            bptree_get_range(test_tree, &keys_array[idx], &keys_array[end_idx], &res, &found_count);
        assert(st == BPTREE_OK);
        int expected_count = (idx <= end_idx) ? (end_idx - idx + 1) : 0;
        assert(found_count == expected_count);
        free(res);
    });

    // Variation: Sequential Start, Small Delta
    BENCH("Range Search (seq, d=10)", N, {
        const int delta = 10;
        const int max_start_idx = (N > delta) ? (N - delta) : 0;
        const int idx = (max_start_idx > 0) ? (bench_i % max_start_idx) : 0;
        int end_idx = idx + delta - 1;
        if (end_idx >= N) end_idx = N - 1;
        if (idx > end_idx) end_idx = idx;

        int found_count = 0;
        bptree_value_t *res = NULL;
        const bptree_status st =
            bptree_get_range(test_tree, &keys_array[idx], &keys_array[end_idx], &res, &found_count);
        assert(st == BPTREE_OK);
        int expected_count = (idx <= end_idx) ? (end_idx - idx + 1) : 0;
        assert(found_count == expected_count);
        free(res);
    });

    // Variation: Sequential Start, Large Delta (e.g., 5% of N)
    BENCH("Range Search (seq, d=5%)", N, {
        const int delta = (N > 20) ? (N / 20) : 1;  // Approx 5%
        const int max_start_idx = (N > delta) ? (N - delta) : 0;
        const int idx = (max_start_idx > 0) ? (bench_i % max_start_idx) : 0;
        int end_idx = idx + delta - 1;
        if (end_idx >= N) end_idx = N - 1;
        if (idx > end_idx) end_idx = idx;

        int found_count = 0;
        bptree_value_t *res = NULL;
        const bptree_status st =
            bptree_get_range(test_tree, &keys_array[idx], &keys_array[end_idx], &res, &found_count);
        assert(st == BPTREE_OK);
        int expected_count = (idx <= end_idx) ? (end_idx - idx + 1) : 0;
        assert(found_count == expected_count);
        free(res);
    });

    // Variation: Random Start, Fixed Delta=100
    BENCH("Range Search (rand, d=100)", N, {
        const int delta = 100;
        const int max_start_idx = (N > delta) ? (N - delta) : 0;
        const int idx = (max_start_idx > 0) ? (rand() % max_start_idx) : 0;  // Random start index
        int end_idx = idx + delta - 1;
        // end_idx should be < N because idx < N-delta
        if (end_idx >= N) end_idx = N - 1;  // Safety check
        if (idx > end_idx) end_idx = idx;

        int found_count = 0;
        bptree_value_t *res = NULL;
        const bptree_status st =
            bptree_get_range(test_tree, &keys_array[idx], &keys_array[end_idx], &res, &found_count);
        assert(st == BPTREE_OK);
        int expected_count = (idx <= end_idx) ? (end_idx - idx + 1) : 0;
        assert(found_count == expected_count);
        free(res);
    });

    // Variation: Random Start, Small Delta=10
    BENCH("Range Search (rand, d=10)", N, {
        const int delta = 10;
        const int max_start_idx = (N > delta) ? (N - delta) : 0;
        const int idx = (max_start_idx > 0) ? (rand() % max_start_idx) : 0;  // Random start index
        int end_idx = idx + delta - 1;
        if (end_idx >= N) end_idx = N - 1;
        if (idx > end_idx) end_idx = idx;

        int found_count = 0;
        bptree_value_t *res = NULL;
        const bptree_status st =
            bptree_get_range(test_tree, &keys_array[idx], &keys_array[end_idx], &res, &found_count);
        assert(st == BPTREE_OK);
        int expected_count = (idx <= end_idx) ? (end_idx - idx + 1) : 0;
        assert(found_count == expected_count);
        free(res);
    });

    // --- Prepare for Deletion Benchmarks ---
    // Create deletion order using the populated test_tree for random deletion
    int *deletion_order = malloc(N * sizeof(int));
    if (!deletion_order) {
        perror("Allocation failed for deletion_order");
        exit(EXIT_FAILURE);
    }
    for (int i = 0; i < N; i++) {
        deletion_order[i] = i;
    }
    for (int i = N - 1; i > 0; i--) {
        int j = rand() % (i + 1);
        int tmp = deletion_order[i];
        deletion_order[i] = deletion_order[j];
        deletion_order[j] = tmp;
    }

    // --- Benchmark: Random Deletion ---
    // We delete from test_tree which was populated earlier
    BENCH("Deletion (rand)", N, {
        int idx = deletion_order[bench_i];
        const bptree_status stat = bptree_remove(test_tree, &keys_array[idx]);
        assert(stat == BPTREE_OK);
    });
    assert(test_tree->count == 0);
    // Re-populate test_tree for sequential deletion
    printf("Re-populating tree for sequential delete test...\n");
    for (int i = 0; i < N; i++) {
        const bptree_status stat = bptree_put(test_tree, &keys_array[i], pointers[i]);
        assert(stat == BPTREE_OK);
    }
    assert(test_tree->count == N);

    // --- Benchmark: Sequential Deletion ---
    BENCH("Deletion (seq)", N, {
        const bptree_status stat = bptree_remove(test_tree, &keys_array[bench_i]);
        assert(stat == BPTREE_OK);
    });
    assert(test_tree->count == 0);

    // Free the main test tree used for search/delete/range
    bptree_free(test_tree);
    free(deletion_order);

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
