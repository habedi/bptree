/**
 * @file bench_bptree.c
 * @brief Benchmarks and performance tests for the B+Tree library.
 *
 * This file benchmarks bulk loading, insertion, search, iteration, deletion,
 * and range search operations on a B+Tree using both random and sequential
 * input. It also includes helper functions for shuffling arrays and comparing integers.
 */

#define BPTREE_IMPLEMENTATION
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "bptree.h"

/**
 * @brief Global flag to enable or disable debug logging.
 */
const bool debug_enabled = false;

/**
 * @brief Comparison function for integers.
 *
 * Compares two integers pointed by @p a and @p b.
 *
 * @param a Pointer to the first integer.
 * @param b Pointer to the second integer.
 * @param udata Unused user data.
 * @return Negative if *a < *b, zero if equal, positive if *a > *b.
 */
int compare_ints(const void *a, const void *b, const void *udata) {
    (void)udata;
    const int ia = *(const int *)a;
    const int ib = *(const int *)b;
    return (ia > ib) - (ia < ib);
}

/**
 * @brief Comparison function for qsort using integer pointers.
 *
 * This function is used by qsort to compare pointers to integers.
 *
 * @param a Pointer to a pointer to an integer.
 * @param b Pointer to a pointer to an integer.
 * @return Negative if **a < **b, zero if equal, positive if **a > **b.
 */
int compare_ints_qsort(const void *a, const void *b) {
    const int *ia = *(int **)a;
    const int *ib = *(int **)b;
    return (*ia > *ib) - (*ia < *ib);
}

/**
 * @brief Benchmarking macro.
 *
 * Executes a code block @p count times, measuring the total elapsed time,
 * and prints the timing information.
 *
 * @param label A descriptive label for the benchmark.
 * @param count Number of iterations to run.
 * @param code_block Code block to benchmark.
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
 * @brief Shuffles an array in-place.
 *
 * Implements the Fisher-Yates shuffle algorithm to randomize the order
 * of elements in the array.
 *
 * @param array Array of pointers to shuffle.
 * @param n Number of elements in the array.
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
 * @brief Main entry point for the B+Tree benchmark.
 *
 * This function reads environment variables for seed, maximum items per node,
 * and number of elements (N) to test with. It then performs various benchmarks:
 * - Bulk load benchmark
 * - Insertion benchmarks (random and sequential)
 * - Search benchmarks (random and sequential)
 * - Iterator benchmark
 * - Deletion benchmarks (random and sequential)
 * - Range search benchmark
 *
 * @return Exit status.
 */
int main(void) {
    int seed = getenv("SEED") ? atoi(getenv("SEED")) : (int)time(NULL);
    int max_keys = getenv("MAX_ITEMS") ? atoi(getenv("MAX_ITEMS")) : 32;
    int N = getenv("N") ? atoi(getenv("N")) : 1000000;
    if (N <= 0) {
        fprintf(stderr, "Invalid N value (%d); defaulting to 1000000\n", N);
        N = 1000000;
    }
    printf("SEED=%d, MAX_ITEMS=%d, N=%d\n", seed, max_keys, N);
    srand(seed);

    /* Allocate memory for values and pointers arrays */
    int *vals = malloc(N * sizeof(int));
    void **pointers = malloc(N * sizeof(void *));
    if (!vals || !pointers) {
        fprintf(stderr, "Allocation failed\n");
        exit(1);
    }
    for (int i = 0; i < N; i++) {
        vals[i] = i;
        pointers[i] = &vals[i];
    }

    /* --- Bulk Load Benchmark --- */
    qsort(pointers, N, sizeof(void *), compare_ints_qsort);
    BENCH("Bulk Load (sorted)", 1, {
        bptree *tree =
            bptree_bulk_load(max_keys, compare_ints, NULL, NULL, NULL, debug_enabled, pointers, N);
        if (!tree) {
            fprintf(stderr, "Bulk load failed\n");
            exit(1);
        }
        bptree_free(tree);
    });

    /* --- Insertion Benchmarks --- */
    shuffle(pointers, N);
    {
        bptree *tree = bptree_new(max_keys, compare_ints, NULL, NULL, NULL, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(1);
        }
        BENCH("Insertion (rand)", N, {
            const bptree_status stat = bptree_put(tree, pointers[bench_i]);
            assert(stat == BPTREE_OK);
        });
        bptree_free(tree);
    }
    qsort(pointers, N, sizeof(void *), compare_ints_qsort);
    {
        bptree *tree = bptree_new(max_keys, compare_ints, NULL, NULL, NULL, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(1);
        }
        BENCH("Insertion (seq)", N, {
            const bptree_status stat = bptree_put(tree, pointers[bench_i]);
            assert(stat == BPTREE_OK);
        });
        bptree_free(tree);
    }

    /* --- Search Benchmarks --- */
    shuffle(pointers, N);
    {
        bptree *tree = bptree_new(max_keys, compare_ints, NULL, NULL, NULL, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(1);
        }
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_put(tree, pointers[i]);
            assert(stat == BPTREE_OK);
        }
        BENCH("Search (rand)", N, {
            void *res = bptree_get(tree, pointers[bench_i]);
            if (!res) {
                printf("Search failed at index %d, pointer=%p, value=%d\n", bench_i,
                       pointers[bench_i], *(int *)pointers[bench_i]);
            }
            assert(res != NULL);
        });
        bptree_free(tree);
    }
    qsort(pointers, N, sizeof(void *), compare_ints_qsort);
    {
        bptree *tree = bptree_new(max_keys, compare_ints, NULL, NULL, NULL, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(1);
        }
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_put(tree, pointers[i]);
            assert(stat == BPTREE_OK);
        }
        BENCH("Search (seq)", N, {
            void *res = bptree_get(tree, pointers[bench_i]);
            if (!res) {
                printf("Sequential search failed at index %d, pointer=%p, value=%d\n", bench_i,
                       pointers[bench_i], *(int *)pointers[bench_i]);
            }
            assert(res != NULL);
        });
        bptree_free(tree);
    }

    /* --- Iterator Benchmark --- */
    {
        qsort(pointers, N, sizeof(void *), compare_ints_qsort);
        bptree *tree = bptree_new(max_keys, compare_ints, NULL, NULL, NULL, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(1);
        }
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_put(tree, pointers[i]);
            assert(stat == BPTREE_OK);
        }
        int iter_total = 0;
        int iterations = 1000;
        printf("Running iterator benchmark with %d iterations...\n", iterations);
        BENCH("Iterator", iterations, {
            bptree_iterator *iter = bptree_iterator_new(tree);
            int count = 0;
            while (bptree_iterator_next(iter) != NULL) {
                count++;
            }
            iter_total += count;
            bptree_iterator_free(iter, tree->free_fn);
        });
        printf("Total iterated elements over %d iterations: %d (expected %d per iteration)\n",
               iterations, iter_total, tree->count);
        bptree_free(tree);
    }

    /* --- Deletion Benchmarks --- */
    {
        shuffle(pointers, N);
        bptree *tree = bptree_new(max_keys, compare_ints, NULL, NULL, NULL, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(1);
        }
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_put(tree, pointers[i]);
            assert(stat == BPTREE_OK);
        }
        shuffle(pointers, N);
        BENCH("Deletion (rand)", N, {
            const bptree_status stat = bptree_remove(tree, pointers[bench_i]);
            if (stat != BPTREE_OK) {
                printf("Random deletion failed at index %d, pointer=%p, value=%d\n", bench_i,
                       pointers[bench_i], *(int *)pointers[bench_i]);
            }
            assert(stat == BPTREE_OK);
        });
        bptree_free(tree);
    }
    {
        qsort(pointers, N, sizeof(void *), compare_ints_qsort);
        bptree *tree = bptree_new(max_keys, compare_ints, NULL, NULL, NULL, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(1);
        }
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_put(tree, pointers[i]);
            assert(stat == BPTREE_OK);
        }
        BENCH("Deletion (seq)", N, {
            const bptree_status stat = bptree_remove(tree, pointers[bench_i]);
            if (stat != BPTREE_OK) {
                printf("Sequential deletion failed at index %d, pointer=%p, value=%d\n", bench_i,
                       pointers[bench_i], *(int *)pointers[bench_i]);
            }
            assert(stat == BPTREE_OK);
        });
        bptree_free(tree);
    }

    /* --- Range Search Benchmarks --- */
    {
        qsort(pointers, N, sizeof(void *), compare_ints_qsort);
        bptree *tree = bptree_new(max_keys, compare_ints, NULL, NULL, NULL, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(1);
        }
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_put(tree, pointers[i]);
            assert(stat == BPTREE_OK);
        }
        BENCH("Range Search (seq)", N, {
            const int delta = 100;
            const int idx = bench_i;
            int end_idx = idx + delta;
            if (end_idx >= N) {
                end_idx = N - 1;
            }
            int count = 0;
            void **res = bptree_get_range(tree, pointers[idx], pointers[end_idx], &count);
            assert(count >= 0);
            tree->free_fn(res);
        });
        bptree_free(tree);
    }

    free(vals);
    free(pointers);
    return 0;
}
