/**
 * @file bench_bptree.c
 * @brief Benchmarks and performance tests for the Bptree library.
 *
 * This file includes benchmarks for bulk loading, insertion, upsert, search, iteration,
 * deletion, and range search operations using both random and sequential input data.
 */

#define BPTREE_IMPLEMENTATION
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "bptree.h"

/* Global flag to enable or disable debug logging */
const bool debug_enabled = false;

/* Comparison function for integers.
 * bptree_key_t is an int.
 */
int compare_ints(const bptree_key_t *a, const bptree_key_t *b) {
    int ia = *a;
    int ib = *b;
    return (ia > ib) - (ia < ib);
}

/* Comparison function for qsort, comparing int pointers.
 */
int compare_ints_qsort(const void *a, const void *b) {
    const int *ia = *(const int **)a;
    const int *ib = *(const int **)b;
    return (*ia > *ib) - (*ia < *ib);
}

/* Benchmarking macro.
 * Executes the code block count times and prints the total and per-iteration time.
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

/* Fisher-Yates shuffle for an array of pointers */
void shuffle(void **array, const int n) {
    for (int i = n - 1; i > 0; i--) {
        int j = rand() % (i + 1);
        void *temp = array[i];
        array[i] = array[j];
        array[j] = temp;
    }
}

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

    /* Allocate arrays for keys and values.
     * bptree_key_t is int and bptree_value_t is void*.
     */
    int *vals = malloc(N * sizeof(int));
    int *keys_array = malloc(N * sizeof(int));
    void **pointers = malloc(N * sizeof(void *));
    if (!vals || !keys_array || !pointers) {
        fprintf(stderr, "Allocation failed\n");
        exit(1);
    }
    for (int i = 0; i < N; i++) {
        vals[i] = i;
        keys_array[i] = i;       // keys are simply the integer values
        pointers[i] = &vals[i];  // values point to the corresponding integer in vals
    }

    /* --- Bulk Load Benchmark --- */
    // For bulk load, input must be sorted.
    qsort(keys_array, N, sizeof(int), (int (*)(const void *, const void *))compare_ints);
    // We assume values follow the same order.
    BENCH("Bulk Load (sorted)", 1, {
        bptree *tree = bptree_create(max_keys, compare_ints, debug_enabled);
        bptree_status st = bptree_bulk_load(tree, keys_array, pointers, N);
        if (st != BPTREE_OK) {
            fprintf(stderr, "Bulk load failed\n");
            exit(1);
        }
        bptree_free(tree);
    });

    /* --- Insertion Benchmarks --- */
    shuffle((void **)pointers, N);
    {
        bptree *tree = bptree_create(max_keys, compare_ints, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(1);
        }
        BENCH("Insertion (rand)", N, {
            const bptree_status stat =
                bptree_put(tree, (int *)pointers[bench_i], pointers[bench_i]);
            assert(stat == BPTREE_OK);
        });
        bptree_free(tree);
    }
    qsort(pointers, N, sizeof(void *), compare_ints_qsort);
    {
        bptree *tree = bptree_create(max_keys, compare_ints, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(1);
        }
        BENCH("Insertion (seq)", N, {
            const bptree_status stat =
                bptree_put(tree, (int *)pointers[bench_i], pointers[bench_i]);
            assert(stat == BPTREE_OK);
        });
        bptree_free(tree);
    }

    /* --- Upsert Benchmark --- */
    shuffle((void **)pointers, N);
    {
        bptree *tree = bptree_create(max_keys, compare_ints, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(1);
        }
        // Pre-load tree with N keys.
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_put(tree, (int *)pointers[i], pointers[i]);
            assert(stat == BPTREE_OK);
        }
        // Now, update each key with a new value (could be the same pointer for this benchmark).
        BENCH("Upsert (seq)", N, {
            const bptree_status stat =
                bptree_upsert(tree, (int *)pointers[bench_i], pointers[bench_i]);
            assert(stat == BPTREE_OK);
        });
        bptree_free(tree);
    }

    /* --- Search Benchmarks --- */
    shuffle((void **)pointers, N);
    {
        bptree *tree = bptree_create(max_keys, compare_ints, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(1);
        }
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_put(tree, (int *)pointers[i], pointers[i]);
            assert(stat == BPTREE_OK);
        }
        BENCH("Search (rand)", N, {
            void *res = bptree_get(tree, (int *)pointers[bench_i]);
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
        bptree *tree = bptree_create(max_keys, compare_ints, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(1);
        }
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_put(tree, (int *)pointers[i], pointers[i]);
            assert(stat == BPTREE_OK);
        }
        BENCH("Search (seq)", N, {
            void *res = bptree_get(tree, (int *)pointers[bench_i]);
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
        bptree *tree = bptree_create(max_keys, compare_ints, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(1);
        }
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_put(tree, (int *)pointers[i], pointers[i]);
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
            bptree_iterator_destroy(iter);
        });
        printf("Total iterated elements over %d iterations: %d (expected %d per iteration)\n",
               iterations, iter_total, tree->count);
        bptree_free(tree);
    }

    /* --- Deletion Benchmarks --- */
    {
        shuffle((void **)pointers, N);
        bptree *tree = bptree_create(max_keys, compare_ints, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(1);
        }
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_put(tree, (int *)pointers[i], pointers[i]);
            assert(stat == BPTREE_OK);
        }
        shuffle((void **)pointers, N);
        BENCH("Deletion (rand)", N, {
            const bptree_status stat = bptree_remove(tree, (int *)pointers[bench_i]);
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
        bptree *tree = bptree_create(max_keys, compare_ints, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(1);
        }
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_put(tree, (int *)pointers[i], pointers[i]);
            assert(stat == BPTREE_OK);
        }
        BENCH("Deletion (seq)", N, {
            const bptree_status stat = bptree_remove(tree, (int *)pointers[bench_i]);
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
        bptree *tree = bptree_create(max_keys, compare_ints, debug_enabled);
        if (!tree) {
            fprintf(stderr, "Failed to create tree\n");
            exit(1);
        }
        for (int i = 0; i < N; i++) {
            const bptree_status stat = bptree_put(tree, (int *)pointers[i], pointers[i]);
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
            void **res =
                bptree_get_range(tree, (int *)pointers[idx], (int *)pointers[end_idx], &count);
            assert(count >= 0);
            free(res);
        });
        bptree_free(tree);
    }

    free(vals);
    free(keys_array);
    free(pointers);
    return 0;
}
