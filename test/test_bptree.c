/**
 * @file test_bptree.c
 * @brief Unit tests for the Bptree library.
 *
 * This file tests insertion, deletion, upsert, search, range queries,
 * bulk loading, iteration, tree statistics, invariant checking, and
 * additional edge cases for each key type.
 */

#include <assert.h>
#include <limits.h>
#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BPTREE_IMPLEMENTATION
#include "bptree.h"

/* --- Helper for Key Conversion --- */
/* For string keys, bptree_key_t is a struct with a fixed array.
   The helper function make_key() converts a C string into a bptree_key_t.
   For numeric keys, we use the value directly.
*/
#ifdef BPTREE_KEY_TYPE_STRING
static bptree_key_t make_key(const char *s) {
    bptree_key_t key;
    strncpy(key.data, s, BPTREE_KEY_SIZE);
    key.data[BPTREE_KEY_SIZE - 1] = '\0';
    return key;
}
#define KEY(s) (make_key(s))
#else
#define KEY(s) (s)
#endif

/* Global flag for debug logging */
const bool debug_enabled = false;

/* --- Tests --- */

void test_insertion_and_search() {
    printf("Test insertion and search...\n");
    bptree *tree = bptree_create(5, NULL, debug_enabled);
    assert(tree != NULL);
#ifdef BPTREE_KEY_TYPE_STRING
    assert(bptree_put(tree, &KEY("apple"), "apple") == BPTREE_OK);
    assert(bptree_put(tree, &KEY("banana"), "banana") == BPTREE_OK);
    assert(bptree_put(tree, &KEY("cherry"), "cherry") == BPTREE_OK);
    const char *res = bptree_get(tree, &KEY("banana"));
    assert(res && strcmp(res, "banana") == 0);
    res = bptree_get(tree, &KEY("durian"));
    assert(res == NULL);
#else
    const int a = 1, b = 2, c = 3, d = 4;
    assert(bptree_put(tree, &a, (bptree_value_t)(intptr_t)a) == BPTREE_OK);
    assert(bptree_put(tree, &b, (bptree_value_t)(intptr_t)b) == BPTREE_OK);
    assert(bptree_put(tree, &c, (bptree_value_t)(intptr_t)c) == BPTREE_OK);
    bptree_value_t res_val = bptree_get(tree, &b);
    assert(res_val == (bptree_value_t)(intptr_t)b);
    res_val = bptree_get(tree, &d);
    assert(res_val == 0);
#endif
    bptree_assert_invariants(tree);
    bptree_free(tree);
    printf("Insertion and search passed.\n");
}

void test_deletion() {
    printf("Test deletion...\n");
    bptree *tree = bptree_create(5, NULL, debug_enabled);
    assert(tree != NULL);
#ifdef BPTREE_KEY_TYPE_STRING
    const char *arr[] = {"alpha", "beta", "gamma", "delta", "epsilon"};
    int n = sizeof(arr) / sizeof(arr[0]);
    for (int i = 0; i < n; i++) {
        assert(bptree_put(tree, &KEY(arr[i]), arr[i]) == BPTREE_OK);
    }
    assert(bptree_remove(tree, &KEY("gamma")) == BPTREE_OK);
    assert(bptree_get(tree, &KEY("gamma")) == NULL);
    assert(bptree_remove(tree, &KEY("zeta")) == BPTREE_KEY_NOT_FOUND);
#else
    int arr[] = {10, 20, 30, 40, 50};
    int n = sizeof(arr) / sizeof(arr[0]);
    for (int i = 0; i < n; i++) {
        assert(bptree_put(tree, &arr[i], (bptree_value_t)(intptr_t)arr[i]) == BPTREE_OK);
    }
    int key = 30;
    assert(bptree_remove(tree, &key) == BPTREE_OK);
    key = 30;
    assert(bptree_get(tree, &key) == 0);
    key = 60;
    assert(bptree_remove(tree, &key) == BPTREE_KEY_NOT_FOUND);
#endif
    bptree_assert_invariants(tree);
    bptree_free(tree);
    printf("Deletion passed.\n");
}

void test_empty_tree() {
    printf("Test empty tree operations...\n");
    bptree *tree = bptree_create(5, NULL, debug_enabled);
    assert(tree != NULL);
#ifdef BPTREE_KEY_TYPE_STRING
    assert(bptree_get(tree, &KEY("anything")) == NULL);
    assert(bptree_remove(tree, &KEY("anything")) == BPTREE_KEY_NOT_FOUND);
#else
    const int x = 100;
    assert(bptree_get(tree, &x) == 0);
    assert(bptree_remove(tree, &x) == BPTREE_KEY_NOT_FOUND);
#endif
    bptree_free(tree);
    printf("Empty tree operations passed.\n");
}

void test_duplicate_insertion() {
    printf("Test duplicate insertion...\n");
    bptree *tree = bptree_create(5, NULL, debug_enabled);
    assert(tree != NULL);
#ifdef BPTREE_KEY_TYPE_STRING
    assert(bptree_put(tree, &KEY("duplicate"), "duplicate") == BPTREE_OK);
    assert(bptree_put(tree, &KEY("duplicate"), "duplicate") == BPTREE_DUPLICATE_KEY);
    const char *res = bptree_get(tree, &KEY("duplicate"));
    assert(res && strcmp(res, "duplicate") == 0);
#else
    int d = 42;
    assert(bptree_put(tree, &d, (bptree_value_t)(intptr_t)d) == BPTREE_OK);
    assert(bptree_put(tree, &d, (bptree_value_t)(intptr_t)d) == BPTREE_DUPLICATE_KEY);
    assert(bptree_get(tree, &d) == (bptree_value_t)(intptr_t)d);
#endif
    bptree_assert_invariants(tree);
    bptree_free(tree);
    printf("Duplicate insertion passed.\n");
}

void test_single_element() {
    printf("Test single element tree...\n");
    bptree *tree = bptree_create(5, NULL, debug_enabled);
    assert(tree != NULL);
#ifdef BPTREE_KEY_TYPE_STRING
    assert(bptree_put(tree, &KEY("solo"), "solo") == BPTREE_OK);
    const char *res = bptree_get(tree, &KEY("solo"));
    assert(res && strcmp(res, "solo") == 0);
    assert(bptree_remove(tree, &KEY("solo")) == BPTREE_OK);
    assert(bptree_get(tree, &KEY("solo")) == NULL);
#else
    int solo = 7;
    assert(bptree_put(tree, &solo, (bptree_value_t)(intptr_t)solo) == BPTREE_OK);
    assert(bptree_get(tree, &solo) == (bptree_value_t)(intptr_t)solo);
    assert(bptree_remove(tree, &solo) == BPTREE_OK);
    assert(bptree_get(tree, &solo) == 0);
#endif
    bptree_assert_invariants(tree);
    bptree_free(tree);
    printf("Single element tree passed.\n");
}

void test_upsert() {
    printf("Test upsert operation...\n");
    bptree *tree = bptree_create(5, NULL, debug_enabled);
    assert(tree != NULL);
#ifdef BPTREE_KEY_TYPE_STRING
    assert(bptree_upsert(tree, &KEY("upsert"), "initial") == BPTREE_OK);
    const char *res = bptree_get(tree, &KEY("upsert"));
    assert(res && strcmp(res, "initial") == 0);
    assert(bptree_upsert(tree, &KEY("upsert"), "updated") == BPTREE_OK);
    res = bptree_get(tree, &KEY("upsert"));
    assert(res && strcmp(res, "updated") == 0);
#else
    int k = 10;
    assert(bptree_upsert(tree, &k, (bptree_value_t)(intptr_t)100) == BPTREE_OK);
    assert(bptree_get(tree, &k) == (bptree_value_t)(intptr_t)100);
    int k2 = 10;
    assert(bptree_upsert(tree, &k2, (bptree_value_t)(intptr_t)200) == BPTREE_OK);
    assert(bptree_get(tree, &k) == (bptree_value_t)(intptr_t)200);
#endif
    bptree_assert_invariants(tree);
    bptree_free(tree);
    printf("Upsert passed.\n");
}

void test_bulk_load_sorted() {
    printf("Test bulk load (sorted input)...\n");
#ifdef BPTREE_KEY_TYPE_STRING
    const int N = 100;
    bptree_key_t *keys = malloc(N * sizeof(bptree_key_t));
    bptree_value_t *values = malloc(N * sizeof(bptree_value_t));
    char buf[32];
    for (int i = 0; i < N; i++) {
        sprintf(buf, "key%03d", i);
        keys[i] = make_key(buf);
        values[i] = strdup(buf);
    }
    bptree *tree = bptree_create(5, NULL, debug_enabled);
    assert(tree != NULL);
    bptree_status st = bptree_bulk_load(tree, keys, values, N);
    assert(st == BPTREE_OK);
    for (int i = 0; i < N; i++) {
        const char *res = bptree_get(tree, &keys[i]);
        assert(res != NULL);
        sprintf(buf, "key%03d", i);
        assert(strcmp(res, buf) == 0);
    }
    bptree_assert_invariants(tree);
    bptree_free(tree);
    for (int i = 0; i < N; i++) {
        free(values[i]);
    }
    free(keys);
    free(values);
#else
    const int N = 100;
    int *keys = malloc(N * sizeof(int));
    bptree_value_t *values = malloc(N * sizeof(bptree_value_t));
    for (int i = 0; i < N; i++) {
        keys[i] = i;
        values[i] = (bptree_value_t)(intptr_t)i;
    }
    bptree *tree = bptree_create(5, NULL, debug_enabled);
    assert(tree != NULL);
    bptree_status st = bptree_bulk_load(tree, keys, values, N);
    assert(st == BPTREE_OK);
    for (int i = 0; i < N; i++) {
        bptree_value_t res = bptree_get(tree, &keys[i]);
        assert(res == (bptree_value_t)(intptr_t)i);
    }
    bptree_assert_invariants(tree);
    bptree_free(tree);
    free(keys);
    free(values);
#endif
    printf("Bulk load (sorted) passed.\n");
}

void test_bulk_load_empty() {
    printf("Test bulk load (empty array)...\n");
    bptree *tree = bptree_create(5, NULL, debug_enabled);
    assert(tree != NULL);
    bptree_status st = bptree_bulk_load(tree, NULL, NULL, 0);
    assert(st == BPTREE_INVALID_ARGUMENT);
    bptree_free(tree);
    printf("Bulk load (empty array) passed.\n");
}

void test_range_query() {
    printf("Test range query...\n");
    bptree *tree = bptree_create(5, NULL, debug_enabled);
    assert(tree != NULL);
#ifdef BPTREE_KEY_TYPE_STRING
    char *keys[] = {"apple", "banana", "cherry", "date", "fig", "grape"};
    int n = sizeof(keys) / sizeof(keys[0]);
    for (int i = 0; i < n; i++) {
        assert(bptree_put(tree, &KEY(keys[i]), keys[i]) == BPTREE_OK);
    }
    int count = 0;
    bptree_value_t *range = bptree_get_range(tree, &KEY("banana"), &KEY("fig"), &count);
    assert(count == 4);
    assert(strcmp((char *)range[0], "banana") == 0);
    assert(strcmp((char *)range[1], "cherry") == 0);
    assert(strcmp((char *)range[2], "date") == 0);
    assert(strcmp((char *)range[3], "fig") == 0);
    free(range);
#else
    int keys[] = {10, 20, 30, 40, 50, 60};
    int n = sizeof(keys) / sizeof(keys[0]);
    for (int i = 0; i < n; i++) {
        assert(bptree_put(tree, &keys[i], (bptree_value_t)(intptr_t)keys[i]) == BPTREE_OK);
    }
    int count = 0;
    bptree_value_t *range = bptree_get_range(tree, &keys[1], &keys[4], &count);
    assert(count == 4);
    free(range);
#endif
    bptree_free(tree);
    printf("Range query passed.\n");
}

void test_iterator() {
    printf("Test iterator...\n");
    bptree *tree = bptree_create(5, NULL, debug_enabled);
    assert(tree != NULL);
#ifdef BPTREE_KEY_TYPE_STRING
    char *keys[] = {"ant", "bee", "cat", "dog", "eel", "fox"};
    int n = sizeof(keys) / sizeof(keys[0]);
    for (int i = 0; i < n; i++) {
        assert(bptree_put(tree, &KEY(keys[i]), keys[i]) == BPTREE_OK);
    }
#else
    int keys[] = {1, 2, 3, 4, 5, 6};
    int n = sizeof(keys) / sizeof(keys[0]);
    for (int i = 0; i < n; i++) {
        assert(bptree_put(tree, &keys[i], (bptree_value_t)(intptr_t)keys[i]) == BPTREE_OK);
    }
#endif
    bptree_iterator *iter = bptree_iterator_new(tree);
    assert(iter != NULL);
    int count = 0;
    while (bptree_iterator_next(iter) != NULL) {
        count++;
    }
    assert(count == tree->count);
    bptree_iterator_destroy(iter);
    bptree_free(tree);
    printf("Iterator passed.\n");
}

void test_tree_stats() {
    printf("Test tree statistics...\n");
    bptree *tree = bptree_create(5, NULL, debug_enabled);
    assert(tree != NULL);
#ifdef BPTREE_KEY_TYPE_STRING
    char *keys[] = {"a", "b", "c", "d", "e", "f", "g"};
    int n = sizeof(keys) / sizeof(keys[0]);
    for (int i = 0; i < n; i++) {
        assert(bptree_put(tree, &KEY(keys[i]), keys[i]) == BPTREE_OK);
    }
#else
    int keys[] = {1, 2, 3, 4, 5, 6, 7};
    int n = sizeof(keys) / sizeof(keys[0]);
    for (int i = 0; i < n; i++) {
        assert(bptree_put(tree, &keys[i], (bptree_value_t)(intptr_t)keys[i]) == BPTREE_OK);
    }
#endif
    bptree_stats stats = bptree_get_stats(tree);
    assert(stats.count == tree->count);
    assert(stats.height > 0);
    assert(stats.node_count > 0);
    bptree_free(tree);
    printf("Tree statistics passed.\n");
}

void test_invariants() {
    printf("Test invariants checking...\n");
    bptree *tree = bptree_create(5, NULL, debug_enabled);
    assert(tree != NULL);
#ifdef BPTREE_KEY_TYPE_STRING
    char *keys[] = {"alpha", "beta", "gamma", "delta"};
    int n = sizeof(keys) / sizeof(keys[0]);
    for (int i = 0; i < n; i++) {
        assert(bptree_put(tree, &KEY(keys[i]), keys[i]) == BPTREE_OK);
    }
#else
    int keys[] = {30, 10, 20, 40};
    int n = sizeof(keys) / sizeof(keys[0]);
    for (int i = 0; i < n; i++) {
        assert(bptree_put(tree, &keys[i], (bptree_value_t)(intptr_t)keys[i]) == BPTREE_OK);
    }
#endif
    assert(bptree_check_invariants(tree));
    bptree_assert_invariants(tree);
    bptree_free(tree);
    printf("Invariants checking passed.\n");
}

/* --- Additional Edge Cases Tests for All Key Types --- */

#ifdef BPTREE_KEY_TYPE_INT
#include <limits.h>
void test_int_edge_cases() {
    printf("Test int key edge cases...\n");
    bptree *tree = bptree_create(5, NULL, debug_enabled);
    assert(tree != NULL);
    int keys[] = {INT_MIN, -1, 0, 1, INT_MAX};
    int n = sizeof(keys) / sizeof(keys[0]);
    for (int i = 0; i < n; i++) {
        assert(bptree_put(tree, &keys[i], (bptree_value_t)(intptr_t)keys[i]) == BPTREE_OK);
    }
    for (int i = 0; i < n; i++) {
        bptree_value_t val = bptree_get(tree, &keys[i]);
        assert(val == (bptree_value_t)(intptr_t)keys[i]);
    }
    // Test duplicate insertion
    assert(bptree_put(tree, &keys[2], (bptree_value_t)(intptr_t)keys[2]) == BPTREE_DUPLICATE_KEY);
    bptree_free(tree);
    printf("Int key edge cases passed.\n");
}
#endif

#ifdef BPTREE_KEY_TYPE_FLOAT
void test_float_edge_cases() {
    printf("Test float key edge cases...\n");
    bptree *tree = bptree_create(5, NULL, debug_enabled);
    assert(tree != NULL);
    float keys[] = {-INFINITY, -123.456f, -0.0f, 0.0f, 123.456f, INFINITY};
    int n = sizeof(keys) / sizeof(keys[0]);
    for (int i = 0; i < n; i++) {
        // Store the bit representation of the float as the value.
        int rep;
        memcpy(&rep, &keys[i], sizeof(rep));
        assert(bptree_put(tree, &keys[i], (bptree_value_t)(intptr_t)rep) == BPTREE_OK);
    }
    for (int i = 0; i < n; i++) {
        int rep;
        memcpy(&rep, &keys[i], sizeof(rep));
        bptree_value_t res = bptree_get(tree, &keys[i]);
        assert(res == (bptree_value_t)(intptr_t)rep);
    }
    float key = 999.999f;
    assert(bptree_get(tree, &key) == 0);
    bptree_free(tree);
    printf("Float key edge cases passed.\n");
}
#endif

#ifdef BPTREE_KEY_TYPE_DOUBLE
void test_double_edge_cases() {
    printf("Test double key edge cases...\n");
    bptree *tree = bptree_create(5, NULL, debug_enabled);
    assert(tree != NULL);
    double keys[] = {-INFINITY, -123456.789, -0.0, 0.0, 123456.789, INFINITY};
    int n = sizeof(keys) / sizeof(keys[0]);
    for (int i = 0; i < n; i++) {
        long rep;
        memcpy(&rep, &keys[i], sizeof(rep));
        assert(bptree_put(tree, &keys[i], (bptree_value_t)(intptr_t)rep) == BPTREE_OK);
    }
    for (int i = 0; i < n; i++) {
        long rep;
        memcpy(&rep, &keys[i], sizeof(rep));
        bptree_value_t res = bptree_get(tree, &keys[i]);
        assert(res == (bptree_value_t)(intptr_t)rep);
    }
    double key = 999999.999;
    assert(bptree_get(tree, &key) == 0);
    bptree_free(tree);
    printf("Double key edge cases passed.\n");
}
#endif

#ifdef BPTREE_KEY_TYPE_STRING
void test_string_edge_cases() {
    printf("Test string key edge cases...\n");
    bptree *tree = bptree_create(5, NULL, debug_enabled);
    assert(tree != NULL);
    bptree_key_t k1 = make_key("");
    bptree_key_t k2 = make_key("a");
    char longstr[256];
    for (int i = 0; i < 255; i++) longstr[i] = 'z';
    longstr[255] = '\0';
    bptree_key_t k3 = make_key(longstr);
    assert(bptree_put(tree, &k1, "") == BPTREE_OK);
    assert(bptree_put(tree, &k2, "a") == BPTREE_OK);
    assert(bptree_put(tree, &k3, longstr) == BPTREE_OK);
    assert(strcmp(bptree_get(tree, &k1), "") == 0);
    assert(strcmp(bptree_get(tree, &k2), "a") == 0);
    assert(strcmp(bptree_get(tree, &k3), longstr) == 0);
    // Duplicate insertion test:
    assert(bptree_put(tree, &k2, "a") == BPTREE_DUPLICATE_KEY);
    bptree_free(tree);
    printf("String key edge cases passed.\n");
}
#endif

/* --- Main: Run All Tests --- */
int main(void) {
    test_insertion_and_search();
    test_deletion();
    test_empty_tree();
    test_duplicate_insertion();
    test_single_element();
    test_upsert();
    test_bulk_load_sorted();
    test_bulk_load_empty();
    test_range_query();
    test_iterator();
    test_tree_stats();
    test_invariants();
#ifdef BPTREE_KEY_TYPE_INT
    test_int_edge_cases();
#endif
#ifdef BPTREE_KEY_TYPE_FLOAT
    test_float_edge_cases();
#endif
#ifdef BPTREE_KEY_TYPE_DOUBLE
    test_double_edge_cases();
#endif
#ifdef BPTREE_KEY_TYPE_STRING
    test_string_edge_cases();
#endif
    printf("All tests passed.\n");
    return 0;
}
