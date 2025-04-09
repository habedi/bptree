/**
 * @file bench_bptree.c
 * @brief Benchmarks and performance tests for the Bptree library.
 *
 * This file benchmarks bulk loading, insertion, upsert, search, iteration,
 * deletion, and range search operations using both random and sequential input data.
 */

#define BPTREE_IMPLEMENTATION

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "bptree.h"

/** Global flag to enable/disable debug logging */
const bool debug_enabled = false;

/**
 * @brief Comparison function for keys.
 *
 * Compares two keys assuming a numeric type.
 *
 * @param a Pointer to the first key.
 * @param b Pointer to the second key.
 * @return -1 if *a < *b, 1 if *a > *b, 0 otherwise.
 */
int compare_keys(const bptree_key_t *a, const bptree_key_t *b) {
    return (*a < *b) ? -1 : ((*a > *b) ? 1 : 0);
}

/**
 * @brief Comparison function for keys used by qsort.
 *
 * @param a Pointer to the first key.
 * @param b Pointer to the second key.
 * @return -1 if *a < *b, 1 if *a > *b, 0 otherwise.
 */
int compare_keys_qsort(const void *a, const void *b) {
    const bptree_key_t *ka = (const bptree_key_t *)a;
    const bptree_key_t *kb = (const bptree_key_t *)b;
    return (*ka < *kb) ? -1 : ((*ka > *kb) ? 1 : 0);
}

/**
 * @brief Comparison function for integer pointers used by qsort.
 *
 * @param a Pointer to the first integer pointer.
 * @param b Pointer to the second integer pointer.
 * @return -1 if **a < **b, 1 if **a > **b, 0 otherwise.
 */
int compare_ints_qsort(const void *a, const void *b) {
    const int *ia = *(const int **)a;
    const int *ib = *(const int **)b;
    return (*ia < *ib) ? -1 : ((*ia > *ib) ? 1 : 0);
}

/**
 * @brief Macro to benchmark a code block.
 *
 * Measures the time for a given number of iterations executing a code block.
 *
 * @param label A label for this benchmark.
 * @param count Number of iterations.
 * @param code_block The code block to be benchmarked.
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
 * @brief Shuffles an array of void pointers.
 *
 * @param array The array to shuffle.
 * @param n Number of elements.
 */
void shuffle(void **array, const int n) {
    for (int i = n - 1; i > 0; i--) {
        int j = rand() % (i + 1);
        void *temp = array[i];
        array[i] = array[j];
        array[j] = temp;
    }
}

/**
 * @brief Shuffles a pair of parallel arrays (keys and pointer values).
 *
 * @param keys Array of keys.
 * @param pointers Array of pointers.
 * @param n Number of elements.
 */
void shuffle_pair(bptree_key_t *keys, void **pointers, const int n) {
    for (int i = n - 1; i > 0; i--) {
        int j = rand() % (i + 1);
        bptree_key_t temp_key = keys[i];
        keys[i] = keys[j];
        keys[j] = temp_key;
        void *temp_ptr = pointers[i];
        pointers[i] = pointers[j];
        pointers[j] = temp_ptr;
    }
}

/**
 * @brief Main function to run the benchmarks.
 *
 * Benchmarks bulk loading, insertion (random and sequential), upsert, search (random
 * and sequential), iteration, deletion (random and sequential), and range search on a B+ tree.
 *
 * The benchmark parameters (seed, maximum keys per node, and number of items) are configurable
 * via the SEED, MAX_ITEMS, and N environment variables.
 *
 * @return 0 on success.
 */
int main(void) {
    /* Initialize benchmark parameters from environment variables or defaults */
    int seed = getenv("SEED") ? atoi(getenv("SEED")) : (int)time(NULL);
    int max_keys = getenv("MAX_ITEMS") ? atoi(getenv("MAX_ITEMS")) : 32;
    int N = getenv("N") ? atoi(getenv("N")) : 1000000;
    if (N <= 0) {
        fprintf(stderr, "Invalid N value (%d); defaulting to 1000000\n", N);
        N = 1000000;
    }
    printf("SEED=%d, MAX_ITEMS=%d, N=%d\n", seed, max_keys, N);
    srand(seed);

    /* Allocate benchmark arrays */
    int *vals = malloc(N * sizeof(int));
    bptree_key_t *keys_array = malloc(N * sizeof(bptree_key_t));
    void **pointers = malloc(N * sizeof(void *));
    if (!vals || !keys_array || !pointers) {
        fprintf(stderr, "Allocation failed\n");
        exit(1);
    }
    for (int i = 0; i < N; i++) {
        vals[i] = i;
        keys_array[i] = i;
        pointers[i] = &vals[i];
    }

    /* --- Bulk Load Benchmark --- */
    /* First, sort the keys */
    qsort(keys_array, N, sizeof(bptree_key_t), compare_keys_qsort);
    BENCH("Bulk Load (sorted)", 1, {
        bptree *tree = bptree_create(max_keys, compare_keys, debug_enabled);
        bptree_status st = bptree_bulk_load(tree, keys_array, pointers, N);
        if (st != BPTREE_OK) {
            fprintf(stderr, "Bulk load failed\n");
            exit(1);
        }
        bptree_free(tree);
    });

    /* Prepare copies and shuffle for random order benchmarks */
    bptree_key_t *keys_copy = malloc(N * sizeof(bptree_key_t));
    void **pointers_copy = malloc(N * sizeof(void *));
    if (!keys_copy || !pointers_copy) {
        fprintf(stderr, "Allocation failed for copies\n");
        exit(1);
    }
    memcpy(keys_copy, keys_array, N * sizeof(bptree_key_t));
    memcpy(pointers_copy, pointers, N * sizeof(void *));
    shuffle_pair(keys_copy, pointers_copy, N);

    /* --- Insertion Benchmark: Random Order --- */
    {
        bptree *tree = bptree_create(max_keys, compare_keys, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(1);
        }
        BENCH("Insertion (rand)", N, {
            const bptree_status stat =
                bptree_insert(tree, &keys_copy[bench_i], pointers_copy[bench_i]);
            assert(stat == BPTREE_OK);
        });
        bptree_free(tree);
    }

    /* --- Insertion Benchmark: Sequential Order --- */
    qsort(keys_copy, N, sizeof(bptree_key_t), compare_keys_qsort);
    {
        bptree *tree = bptree_create(max_keys, compare_keys, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(1);
        }
        BENCH("Insertion (seq)", N, {
            const bptree_status stat =
                bptree_insert(tree, &keys_copy[bench_i], pointers_copy[bench_i]);
            assert(stat == BPTREE_OK);
        });
        bptree_free(tree);
    }

    /* --- Upsert Benchmark --- */
    memcpy(keys_copy, keys_array, N * sizeof(bptree_key_t));
    memcpy(pointers_copy, pointers, N * sizeof(void *));
    {
        bptree *tree = bptree_create(max_keys, compare_keys, debug_enabled);
        if (!tree) {
            exit(1);
        }
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_insert(tree, &keys_copy[i], pointers_copy[i]);
            assert(stat == BPTREE_OK);
        }
        BENCH("Upsert (seq)", N, {
            const bptree_status stat =
                bptree_upsert(tree, &keys_copy[bench_i], pointers_copy[bench_i]);
            assert(stat == BPTREE_OK);
        });
        bptree_free(tree);
    }

    /* --- Search Benchmarks --- */
    memcpy(keys_copy, keys_array, N * sizeof(bptree_key_t));
    memcpy(pointers_copy, pointers, N * sizeof(void *));
    qsort(keys_copy, N, sizeof(bptree_key_t), compare_keys_qsort);
    {
        bptree *tree = bptree_create(max_keys, compare_keys, debug_enabled);
        if (!tree) {
            exit(1);
        }
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_insert(tree, &keys_copy[i], pointers_copy[i]);
            assert(stat == BPTREE_OK);
        }
        BENCH("Search (rand)", N, {
            bptree_value_t res;
            const bptree_status st = bptree_get_value(tree, &keys_copy[bench_i], &res);
            assert(st == BPTREE_OK);
        });
        bptree_free(tree);
    }
    {
        bptree *tree = bptree_create(max_keys, compare_keys, debug_enabled);
        if (!tree) {
            exit(1);
        }
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_insert(tree, &keys_copy[i], pointers_copy[i]);
            assert(stat == BPTREE_OK);
        }
        BENCH("Search (seq)", N, {
            bptree_value_t res;
            const bptree_status st = bptree_get_value(tree, &keys_copy[bench_i], &res);
            assert(st == BPTREE_OK);
        });
        bptree_free(tree);
    }

    /* --- Iterator Benchmark --- */
    qsort(keys_copy, N, sizeof(bptree_key_t), compare_keys_qsort);
    {
        bptree *tree = bptree_create(max_keys, compare_keys, debug_enabled);
        if (!tree) {
            exit(1);
        }
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_insert(tree, &keys_copy[i], pointers_copy[i]);
            assert(stat == BPTREE_OK);
        }
        int iter_total = 0;
        int iterations = 1000;
        printf("Running iterator benchmark with %d iterations...\n", iterations);
        BENCH("Iterator", iterations, {
            bptree_iterator *iter = bptree_iterator_new(tree);
            int count = 0;
            bptree_value_t temp;
            while (bptree_iterator_next(iter, &temp) == BPTREE_OK) count++;
            iter_total += count;
            bptree_iterator_free(iter);
        });
        printf("Total iterated elements over %d iterations: %d (expected %d per iteration)\n",
               iterations, iter_total, tree->count);
        bptree_free(tree);
    }

    /* --- Deletion Benchmarks --- */
    memcpy(keys_copy, keys_array, N * sizeof(bptree_key_t));
    memcpy(pointers_copy, pointers, N * sizeof(void *));
    shuffle_pair(keys_copy, pointers_copy, N);
    {
        bptree *tree = bptree_create(max_keys, compare_keys, debug_enabled);
        if (!tree) {
            exit(1);
        }
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_insert(tree, &keys_copy[i], pointers_copy[i]);
            assert(stat == BPTREE_OK);
        }
        shuffle_pair(keys_copy, pointers_copy, N);
        BENCH("Deletion (rand)", N, {
            const bptree_status stat = bptree_remove(tree, &keys_copy[bench_i]);
            assert(stat == BPTREE_OK);
        });
        bptree_free(tree);
    }
    memcpy(keys_copy, keys_array, N * sizeof(bptree_key_t));
    memcpy(pointers_copy, pointers, N * sizeof(void *));
    qsort(keys_copy, N, sizeof(bptree_key_t), compare_keys_qsort);
    {
        bptree *tree = bptree_create(max_keys, compare_keys, debug_enabled);
        if (!tree) {
            exit(1);
        }
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_insert(tree, &keys_copy[i], pointers_copy[i]);
            assert(stat == BPTREE_OK);
        }
        BENCH("Deletion (seq)", N, {
            const bptree_status stat = bptree_remove(tree, &keys_copy[bench_i]);
            assert(stat == BPTREE_OK);
        });
        bptree_free(tree);
    }

    /* --- Range Search Benchmark --- */
    memcpy(keys_copy, keys_array, N * sizeof(bptree_key_t));
    memcpy(pointers_copy, pointers, N * sizeof(void *));
    qsort(keys_copy, N, sizeof(bptree_key_t), compare_keys_qsort);
    {
        bptree *tree = bptree_create(max_keys, compare_keys, debug_enabled);
        if (!tree) {
            exit(1);
        }
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_insert(tree, &keys_copy[i], pointers_copy[i]);
            assert(stat == BPTREE_OK);
        }
        BENCH("Range Search (seq)", N, {
            const int delta = 100;
            const int idx = bench_i;
            int end_idx = idx + delta;
            if (end_idx >= N) end_idx = N - 1;
            int found_count = 0;
            bptree_value_t *res = NULL;
            const bptree_status st =
                bptree_get_range(tree, &keys_copy[idx], &keys_copy[end_idx], &res, &found_count);
            assert(st == BPTREE_OK);
            free(res);
        });
        bptree_free(tree);
    }

    free(keys_copy);
    free(pointers_copy);
    free(keys_array);
    free(pointers);
    free(vals);

    return 0;
}
