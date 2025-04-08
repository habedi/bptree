#include <assert.h>
#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifndef DEFAULT_MAX_KEYS
#define DEFAULT_MAX_KEYS 32
#endif

#define BPTREE_IMPLEMENTATION
#include "bptree.h"

const bool global_debug_enabled = false;
static const int test_max_keys_values[] = {3, 4, 7, DEFAULT_MAX_KEYS};
static const int num_test_max_keys = sizeof(test_max_keys_values) / sizeof(test_max_keys_values[0]);
static int tests_run = 0;
static int tests_failed = 0;
#define FAIL(msg, ...)                                                                \
    do {                                                                              \
        fprintf(stderr, "FAIL(%s:%d): " msg "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
        tests_failed++;                                                               \
    } while (0)
#define ASSERT(condition, msg, ...)                                           \
    do {                                                                      \
        if (!(condition)) {                                                   \
            FAIL("Assertion Failed: (" #condition ") - " msg, ##__VA_ARGS__); \
        }                                                                     \
    } while (0)
#define RUN_TEST(test)                             \
    do {                                           \
        fprintf(stderr, "== Running %s\n", #test); \
        test();                                    \
        tests_run++;                               \
    } while (0)
#ifdef BPTREE_KEY_TYPE_STRING
#define MAX_ALLOC_TRACK 256
static void *alloc_track[MAX_ALLOC_TRACK];
static int alloc_track_count = 0;
static void track_alloc(void *ptr) {
    if (alloc_track_count < MAX_ALLOC_TRACK) {
        alloc_track[alloc_track_count++] = ptr;
    }
}
static void cleanup_alloc_track(void) {
    for (int i = 0; i < alloc_track_count; i++) {
        free(alloc_track[i]);
    }
    alloc_track_count = 0;
}
static bptree_key_t make_key_str(const char *s) {
    bptree_key_t key;
    memset(&key, 0, sizeof(key));
    snprintf(key.data, BPTREE_KEY_SIZE, "%s", s);
    return key;
}
#define KEY(s) (make_key_str(s))
#define MAKE_VALUE_STR(s) (strdup(s))
#define GET_VALUE_STR(v) ((const char *)(v))
#define CMP_VALUE_STR(v1, v2) (strcmp(GET_VALUE_STR(v1), GET_VALUE_STR(v2)) == 0)
#define FREE_VALUE_STR(v) (free((void *)(v)))
#else
#define KEY(s) (s)
#define MAKE_VALUE_NUM(k) ((bptree_value_t)(intptr_t)(k))
#ifdef BPTREE_KEY_TYPE_INT
#define GET_VALUE_NUM(v) ((int)(intptr_t)(v))
#endif
#ifdef BPTREE_KEY_TYPE_FLOAT
typedef union {
    float f;
    int i;
} float_int_union;
static int float_to_int_bits(float f) {
    float_int_union u;
    u.f = f;
    return u.i;
}
static float int_bits_to_float(int i) {
    float_int_union u;
    u.i = i;
    return u.f;
}
#define GET_VALUE_NUM(v) (int_bits_to_float((int)(intptr_t)(v)))
#endif
#ifdef BPTREE_KEY_TYPE_DOUBLE
typedef union {
    double d;
    int64_t i;
} double_int64_union;
static int64_t double_to_int64_bits(double d) {
    double_int64_union u;
    u.d = d;
    return u.i;
}
static double int64_bits_to_double(int64_t i) {
    double_int64_union u;
    u.i = i;
    return u.d;
}
#define GET_VALUE_NUM(v) (int64_bits_to_double((int64_t)(intptr_t)(v)))
#endif
#define CMP_VALUE_NUM(v, k) (GET_VALUE_NUM(v) == (k))
#define FREE_VALUE_STR(v) ((void)0)
#endif

#ifdef BPTREE_KEY_TYPE_FLOAT
static bool is_nan_key(const bptree_key_t *k) { return isnan(*k); }
static bool is_inf_key(const bptree_key_t *k) { return isinf(*k); }
#endif
#ifdef BPTREE_KEY_TYPE_DOUBLE
static bool is_nan_key(const bptree_key_t *k) { return isnan(*k); }
static bool is_inf_key(const bptree_key_t *k) { return isinf(*k); }
#endif

#ifdef BPTREE_KEY_TYPE_STRING
static bptree *create_test_tree_with_order(int max_keys) {
    return bptree_create(max_keys, NULL, global_debug_enabled);
}
#else
static bptree *create_test_tree_with_order(int max_keys) {
    return bptree_create(max_keys, NULL, global_debug_enabled);
}
#endif

void test_creation_failure(void) {
    bptree *tree = bptree_create(2, NULL, global_debug_enabled);
    ASSERT(tree == NULL, "bptree_create should fail for max_keys < 3");
}

void test_insertion_and_search(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree *tree = create_test_tree_with_order(order);
        ASSERT(tree != NULL, "Tree creation failed for order %d", order);
        const int N = 10;
#ifdef BPTREE_KEY_TYPE_STRING
        char key_buf[32];
        for (int i = 0; i < N; i++) {
            sprintf(key_buf, "key%d", i);
            bptree_key_t k = KEY(key_buf);
            char *v = strdup(key_buf);
            track_alloc(v);
            ASSERT(bptree_put(tree, &k, v) == BPTREE_OK, "Insert failed for key %s", key_buf);
        }
        for (int i = 0; i < N; i++) {
            sprintf(key_buf, "key%d", i);
            bptree_key_t k = KEY(key_buf);
            const char *res = GET_VALUE_STR(bptree_get(tree, &k));
            ASSERT(res != NULL, "Get failed for existing key %s", key_buf);
            ASSERT(strcmp(res, key_buf) == 0, "Value mismatch for key %s", key_buf);
        }
#else
        for (int i = 0; i < N; i++) {
            int k = i * 10 + 1;
            ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK, "Insert failed for key %d",
                   k);
        }
        for (int i = 0; i < N; i++) {
            int k = i * 10 + 1;
            bptree_value_t res_val = bptree_get(tree, &k);
            ASSERT(res_val != NULL, "Get failed for key %d", k);
            ASSERT(CMP_VALUE_NUM(res_val, k), "Value mismatch for key %d", k);
        }
#endif
        bptree_assert_invariants(tree);
        bptree_free(tree);
#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();
#endif
    }
}

void test_deletion(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree *tree = create_test_tree_with_order(order);
        ASSERT(tree != NULL, "Tree creation failed for order %d", order);
        const int N = 7;
#ifdef BPTREE_KEY_TYPE_STRING
        char key_buf[32];
        for (int i = 0; i < N; i++) {
            sprintf(key_buf, "del%d", i);
            bptree_key_t k = KEY(key_buf);
            char *v = strdup(key_buf);
            track_alloc(v);
            ASSERT(bptree_put(tree, &k, v) == BPTREE_OK, "Insert failed");
        }
        ASSERT(tree->count == N, "Count mismatch after insertion");
        bptree_key_t k_del = KEY("del3");
        ASSERT(bptree_remove(tree, &k_del) == BPTREE_OK, "Remove middle key failed");
        ASSERT(bptree_get(tree, &k_del) == NULL, "Get succeeded for deleted key");
        ASSERT(tree->count == N - 1, "Count mismatch after deletion");
#else
        for (int i = 0; i < N; i++) {
            int k = i + 1;
            ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK, "Insert failed");
        }
        ASSERT(tree->count == N, "Count mismatch after insertion");
        int k_del = 4;
        ASSERT(bptree_remove(tree, &k_del) == BPTREE_OK, "Remove key failed");
        ASSERT(bptree_get(tree, &k_del) == NULL, "Get succeeded for deleted key");
        ASSERT(tree->count == N - 1, "Count mismatch after deletion");
#endif
        bptree_free(tree);
#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();
#endif
    }
}

void test_empty_tree(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree *tree = create_test_tree_with_order(order);
#ifdef BPTREE_KEY_TYPE_STRING
        bptree_key_t k = KEY("anything");
        ASSERT(bptree_get(tree, &k) == NULL, "Get on empty tree failed");
        ASSERT(bptree_remove(tree, &k) == BPTREE_KEY_NOT_FOUND, "Remove on empty tree failed");
#else
        int k = 101;
        ASSERT(bptree_get(tree, &k) == NULL, "Get on empty tree failed");
        ASSERT(bptree_remove(tree, &k) == BPTREE_KEY_NOT_FOUND, "Remove on empty tree failed");
#endif
        bptree_stats stats = bptree_get_stats(tree);
        ASSERT(stats.count == 0, "Empty tree count non-zero");
        ASSERT(stats.height == 1, "Empty tree height not 1");
        ASSERT(stats.node_count == 1, "Empty tree node_count not 1");
        bptree_free(tree);
    }
}

void test_duplicate_insertion(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree *tree = create_test_tree_with_order(order);
#ifdef BPTREE_KEY_TYPE_STRING
        bptree_key_t k = KEY("duplicate");
        char *v1 = strdup("value1");
        char *v2 = strdup("value2");
        track_alloc(v1);
        track_alloc(v2);
        ASSERT(bptree_put(tree, &k, v1) == BPTREE_OK, "First put failed");
        ASSERT(bptree_put(tree, &k, v2) == BPTREE_DUPLICATE_KEY,
               "Second put did not return DUPLICATE_KEY");
        const char *res = GET_VALUE_STR(bptree_get(tree, &k));
        ASSERT(res != NULL, "Get after duplicate put failed");
        ASSERT(strcmp(res, "value1") == 0, "Value was overwritten after duplicate put");
#else
        int k = 42;
        ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(100)) == BPTREE_OK, "First put failed");
        ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(200)) == BPTREE_DUPLICATE_KEY,
               "Second put failed");
        bptree_value_t res_val = bptree_get(tree, &k);
        ASSERT(res_val != NULL, "Get after duplicate put failed");
        ASSERT(GET_VALUE_NUM(res_val) == 100, "Value overwritten after duplicate put");
#endif
        bptree_assert_invariants(tree);
        bptree_free(tree);
#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();
#endif
    }
}

void test_single_element(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree *tree = create_test_tree_with_order(order);
#ifdef BPTREE_KEY_TYPE_STRING
        bptree_key_t k = KEY("solo");
        char *v = strdup("solo_val");
        track_alloc(v);
        ASSERT(bptree_put(tree, &k, v) == BPTREE_OK, "Put failed");
        const char *res = GET_VALUE_STR(bptree_get(tree, &k));
        ASSERT(res && strcmp(res, "solo_val") == 0, "Get failed");
        ASSERT(bptree_remove(tree, &k) == BPTREE_OK, "Remove failed");
        ASSERT(bptree_get(tree, &k) == NULL, "Get after remove failed");
#else
        int k = 7;
        ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK, "Put failed");
        bptree_value_t res_val = bptree_get(tree, &k);
        ASSERT(res_val != NULL && GET_VALUE_NUM(res_val) == k, "Get failed");
        ASSERT(bptree_remove(tree, &k) == BPTREE_OK, "Remove failed");
        ASSERT(bptree_get(tree, &k) == NULL, "Get after remove failed");
#endif
        bptree_stats stats = bptree_get_stats(tree);
        ASSERT(stats.count == 0, "Count non-zero after single element removal");
        bptree_assert_invariants(tree);
        bptree_free(tree);
#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();
#endif
    }
}

void test_upsert(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree *tree = create_test_tree_with_order(order);
#ifdef BPTREE_KEY_TYPE_STRING
        bptree_key_t k = KEY("upsert_key");
        char *v_initial = strdup("initial");
        char *v_updated = strdup("updated");
        track_alloc(v_initial);
        track_alloc(v_updated);
        ASSERT(bptree_upsert(tree, &k, v_initial) == BPTREE_OK, "Initial upsert failed");
        const char *res = GET_VALUE_STR(bptree_get(tree, &k));
        ASSERT(res && strcmp(res, "initial") == 0, "Get after initial upsert failed");
        ASSERT(tree->count == 1, "Count wrong after initial upsert");
        ASSERT(bptree_upsert(tree, &k, v_updated) == BPTREE_OK, "Updating upsert failed");
        res = GET_VALUE_STR(bptree_get(tree, &k));
        ASSERT(res && strcmp(res, "updated") == 0, "Get after updating upsert failed");
#else
        int k = 10;
        ASSERT(bptree_upsert(tree, &k, MAKE_VALUE_NUM(100)) == BPTREE_OK, "Initial upsert failed");
        bptree_value_t res_val = bptree_get(tree, &k);
        ASSERT(res_val != NULL && GET_VALUE_NUM(res_val) == 100, "Get after initial upsert failed");
        ASSERT(tree->count == 1, "Count wrong after initial upsert");
        int k2 = 10;
        ASSERT(bptree_upsert(tree, &k2, MAKE_VALUE_NUM(200)) == BPTREE_OK,
               "Updating upsert failed");
        res_val = bptree_get(tree, &k);
        ASSERT(res_val != NULL && GET_VALUE_NUM(res_val) == 200,
               "Get after updating upsert failed");
        ASSERT(tree->count == 1, "Count wrong after updating upsert");
#endif
        bptree_assert_invariants(tree);
        bptree_free(tree);
#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();
#endif
    }
}

void test_bulk_load_sorted(void) {
    const int N = 153;
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree *tree = create_test_tree_with_order(order);
#ifdef BPTREE_KEY_TYPE_STRING
        bptree_key_t *keys = malloc(N * sizeof(bptree_key_t));
        bptree_value_t *values = malloc(N * sizeof(bptree_value_t));
        ASSERT(keys && values, "Allocation for bulk load arrays failed");
        char buf[32];
        for (int i = 0; i < N; i++) {
            sprintf(buf, "bulk%04d", i);
            keys[i] = KEY(buf);
            values[i] = MAKE_VALUE_STR(buf);
            track_alloc(values[i]);
            ASSERT(values[i] != NULL, "strdup failed for value %d", i);
        }
        bptree_status st = bptree_bulk_load(tree, keys, values, N);
        ASSERT(st == BPTREE_OK, "Bulk load failed with status %d", st);
        ASSERT(tree->count == N, "Tree count mismatch after bulk load");
        bptree_assert_invariants(tree);
        for (int i = 0; i < N; i++) {
            const char *res = GET_VALUE_STR(bptree_get(tree, &keys[i]));
            ASSERT(res != NULL, "Get failed for bulk-loaded key index %d", i);
            ASSERT(strcmp(res, GET_VALUE_STR(values[i])) == 0, "Value mismatch for key index %d",
                   i);
        }
        free(keys);
        free(values);
#else
        bptree_key_t *keys = malloc(N * sizeof(bptree_key_t));
        bptree_value_t *values = malloc(N * sizeof(bptree_value_t));
        ASSERT(keys && values, "Allocation for bulk load arrays failed");
        for (int i = 0; i < N; i++) {
            keys[i] = i * 10 + 1;
            values[i] = MAKE_VALUE_NUM(keys[i]);
        }
        bptree_status st = bptree_bulk_load(tree, keys, values, N);
        ASSERT(st == BPTREE_OK, "Bulk load failed with status %d", st);
        ASSERT(tree->count == N, "Tree count mismatch after bulk load");
        bptree_assert_invariants(tree);
        for (int i = 0; i < N; i++) {
            bptree_value_t res = bptree_get(tree, &keys[i]);
            ASSERT(res != NULL, "Get failed for bulk-loaded key index %d", i);
            ASSERT(CMP_VALUE_NUM(res, keys[i]), "Value mismatch for key index %d", i);
        }
        free(keys);
        free(values);
#endif
        bptree_free(tree);
#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();
#endif
    }
}

void test_bulk_load_failures(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree *tree = create_test_tree_with_order(order);
#ifdef BPTREE_KEY_TYPE_STRING
        bptree_key_t *keys = malloc(10 * sizeof(bptree_key_t));
        bptree_value_t *values = malloc(10 * sizeof(bptree_value_t));
        char buf[32];
        sprintf(buf, "key%d", 5);
        keys[0] = KEY(buf);
        values[0] = MAKE_VALUE_STR(buf);
        sprintf(buf, "key%d", 1);
        keys[1] = KEY(buf);
        values[1] = MAKE_VALUE_STR(buf);
        bptree_status st = bptree_bulk_load(tree, keys, values, 2);
        ASSERT(st == BPTREE_BULK_LOAD_NOT_SORTED, "Bulk load did not detect unsorted input");
        FREE_VALUE_STR(values[0]);
        FREE_VALUE_STR(values[1]);
        ASSERT(tree->count == 0 && (tree->root == NULL || tree->root->num_keys == 0),
               "Tree not empty after failed bulk load");
        sprintf(buf, "key%d", 1);
        keys[0] = KEY(buf);
        values[0] = MAKE_VALUE_STR(buf);
        sprintf(buf, "key%d", 1);
        keys[1] = KEY(buf);
        values[1] = MAKE_VALUE_STR(buf);
        sprintf(buf, "key%d", 2);
        keys[2] = KEY(buf);
        values[2] = MAKE_VALUE_STR(buf);
        st = bptree_bulk_load(tree, keys, values, 3);
        ASSERT(st == BPTREE_BULK_LOAD_DUPLICATE, "Bulk load did not detect duplicate input");
        FREE_VALUE_STR(values[0]);
        FREE_VALUE_STR(values[1]);
        FREE_VALUE_STR(values[2]);
        ASSERT(tree->count == 0 && (tree->root == NULL || tree->root->num_keys == 0),
               "Tree not empty after failed bulk load");
        free(keys);
        free(values);
#else
        bptree_key_t *keys = malloc(10 * sizeof(bptree_key_t));
        bptree_value_t *values = malloc(10 * sizeof(bptree_value_t));
        keys[0] = 51;
        values[0] = MAKE_VALUE_NUM(51);
        keys[1] = 11;
        values[1] = MAKE_VALUE_NUM(11);
        bptree_status st = bptree_bulk_load(tree, keys, values, 2);
        ASSERT(st == BPTREE_BULK_LOAD_NOT_SORTED, "Bulk load did not detect unsorted input");
        ASSERT(tree->count == 0 && (tree->root == NULL || tree->root->num_keys == 0),
               "Tree not empty after failed bulk load");
        keys[0] = 11;
        values[0] = MAKE_VALUE_NUM(11);
        keys[1] = 11;
        values[1] = MAKE_VALUE_NUM(11);
        keys[2] = 21;
        values[2] = MAKE_VALUE_NUM(21);
        st = bptree_bulk_load(tree, keys, values, 3);
        ASSERT(st == BPTREE_BULK_LOAD_DUPLICATE, "Bulk load did not detect duplicate input");
        ASSERT(tree->count == 0 && (tree->root == NULL || tree->root->num_keys == 0),
               "Tree not empty after failed bulk load");
        free(keys);
        free(values);
#endif
        bptree_free(tree);
#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();
#endif
    }
}

void test_bulk_load_empty(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree *tree = create_test_tree_with_order(order);
        bptree_status st = bptree_bulk_load(tree, NULL, NULL, 0);
        ASSERT(st == BPTREE_INVALID_ARGUMENT,
               "Bulk load with 0 items did not return INVALID_ARGUMENT");
        bptree_free(tree);
    }
}

void test_range_query(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree *tree = create_test_tree_with_order(order);
        const int N = 10;
#ifdef BPTREE_KEY_TYPE_STRING
        bptree_value_t *allocated_values[N];
        char key_buf[32];
        for (int i = 0; i < N; i++) {
            sprintf(key_buf, "range%02d", i);
            bptree_key_t k = KEY(key_buf);
            allocated_values[i] = MAKE_VALUE_STR(key_buf);
            track_alloc(allocated_values[i]);
            ASSERT(bptree_put(tree, &k, allocated_values[i]) == BPTREE_OK, "Insert failed");
        }
        bptree_key_t start_k = KEY("range03");
        bptree_key_t end_k = KEY("range07");
        int count = 0;
        bptree_value_t *range = bptree_get_range(tree, &start_k, &end_k, &count);
        ASSERT(range != NULL, "Range query returned NULL (Case 1)");
        ASSERT(count == 5, "Range count mismatch (Case 1): expected 5, got %d", count);
        ASSERT(strcmp(GET_VALUE_STR(range[0]), "range03") == 0,
               "Range value mismatch (Case 1, Idx 0)");
        ASSERT(strcmp(GET_VALUE_STR(range[4]), "range07") == 0,
               "Range value mismatch (Case 1, Idx 4)");
        free(range);
#else
        for (int i = 0; i < N; i++) {
            int k = i * 10 + 1;
            ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK, "Insert failed");
        }
        int start_k = 21;
        int end_k = 61;
        int count = 0;
        bptree_value_t *range = bptree_get_range(tree, &start_k, &end_k, &count);
        ASSERT(range != NULL, "Range query returned NULL (Case 1)");
        ASSERT(count == 5, "Range count mismatch (Case 1): expected 5, got %d", count);
        ASSERT(GET_VALUE_NUM(range[0]) == 21, "Range value mismatch (Case 1, Idx 0)");
        ASSERT(GET_VALUE_NUM(range[4]) == 61, "Range value mismatch (Case 1, Idx 4)");
        free(range);
#endif
        bptree_free(tree);
#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();
#endif
    }
}

void test_iterator(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree *tree = create_test_tree_with_order(order);
        const int N = 6;
#ifdef BPTREE_KEY_TYPE_STRING
        char *keys[] = {"ant", "bee", "cat", "dog", "eel", "fox"};
        bptree_value_t allocated_values[N];
        char *expected_str_values[N];
        for (int i = 0; i < N; i++) {
            bptree_key_t k = KEY(keys[i]);
            allocated_values[i] = MAKE_VALUE_STR(keys[i]);
            track_alloc(allocated_values[i]);
            expected_str_values[i] = keys[i];
            ASSERT(bptree_put(tree, &k, allocated_values[i]) == BPTREE_OK, "Insert failed");
        }
#else
        int keys[] = {11, 22, 33, 44, 55, 66};
        int expected_values[N];
        for (int i = 0; i < N; i++) {
            expected_values[i] = keys[i];
            ASSERT(bptree_put(tree, &keys[i], MAKE_VALUE_NUM(keys[i])) == BPTREE_OK,
                   "Insert failed");
        }
#endif
        bptree_iterator *iter = bptree_iterator_new(tree);
        ASSERT(iter != NULL, "Iterator creation failed");
        int count = 0;
        bptree_value_t val;
        while ((val = bptree_iterator_next(iter)) != NULL) {
#ifdef BPTREE_KEY_TYPE_STRING
            ASSERT(CMP_VALUE_STR(val, expected_str_values[count]),
                   "Iterator value mismatch at index %d: expected '%s', got '%s'", count,
                   expected_str_values[count], GET_VALUE_STR(val));
#else
            ASSERT(CMP_VALUE_NUM(val, expected_values[count]),
                   "Iterator value mismatch at index %d", count);
#endif
            count++;
        }
        ASSERT(count == N, "Iterator returned wrong number of items: expected %d, got %d", N,
               count);
        ASSERT(bptree_iterator_next(iter) == NULL, "Iterator did not return NULL after finishing");
        bptree_iterator_destroy(iter);
        bptree_free(tree);
#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();
#endif
    }
}

void test_tree_stats(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree *tree = create_test_tree_with_order(order);
        bptree_stats stats = bptree_get_stats(tree);
        ASSERT(stats.count == 0, "Initial count wrong");
        ASSERT(stats.height == 1, "Initial height wrong");
        ASSERT(stats.node_count == 1, "Initial node_count wrong");
        const int N = 150;
#ifdef BPTREE_KEY_TYPE_STRING
        char key_buf[32];
        for (int i = 0; i < N; i++) {
            sprintf(key_buf, "stat%d", i);
            ASSERT(bptree_put(tree, &KEY(key_buf), NULL) == BPTREE_OK, "Insert failed");
        }
#else
        for (int i = 0; i < N; i++) {
            int k = i + 1;
            ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK, "Insert failed");
        }
#endif
        stats = bptree_get_stats(tree);
        ASSERT(stats.count == N, "Final count wrong: expected %d, got %d", N, stats.count);
        ASSERT(stats.height > 1, "Final height not greater than 1");
        ASSERT(stats.node_count > 1, "Final node_count not greater than 1");
        int expected_min_nodes = N / DEFAULT_MAX_KEYS;
        ASSERT(stats.node_count >= expected_min_nodes,
               "Node count %d is lower than expected minimum %d", stats.node_count,
               expected_min_nodes);
        bptree_free(tree);
    }
}

#ifdef BPTREE_KEY_TYPE_FLOAT
void test_comparator_edge_cases_float(void) {
    bptree *tree = create_test_tree_with_order(5);
    float nan = NAN, inf = INFINITY, ninf = -INFINITY, negzero = -0.0f, poszero = 0.0f,
          normal = 1.23f;
    ASSERT(bptree_put(tree, &inf, MAKE_VALUE_NUM(100)) == BPTREE_OK, "Insert inf failed");
    ASSERT(bptree_put(tree, &ninf, MAKE_VALUE_NUM(200)) == BPTREE_OK, "Insert -inf failed");
    ASSERT(bptree_put(tree, &normal, MAKE_VALUE_NUM(300)) == BPTREE_OK, "Insert normal failed");
    ASSERT(bptree_put(tree, &negzero, MAKE_VALUE_NUM(400)) == BPTREE_OK, "Insert -0.0 failed");
    ASSERT(bptree_put(tree, &poszero, MAKE_VALUE_NUM(500)) == BPTREE_OK, "Insert 0.0 failed");
    ASSERT(bptree_get(tree, &inf) != NULL, "Get inf failed");
    ASSERT(bptree_get(tree, &ninf) != NULL, "Get -inf failed");
    ASSERT(bptree_get(tree, &normal) != NULL, "Get normal failed");
    ASSERT(bptree_get(tree, &negzero) != NULL, "Get -0.0 failed");
    ASSERT(bptree_get(tree, &poszero) != NULL, "Get 0.0 failed");
    if (!is_nan_key(&nan)) {
    }
    bptree_free(tree);
}
#endif

#ifdef BPTREE_KEY_TYPE_DOUBLE
void test_comparator_edge_cases_double(void) {
    bptree *tree = create_test_tree_with_order(5);
    double nan = NAN, inf = INFINITY, ninf = -INFINITY, negzero = -0.0, poszero = 0.0,
           normal = 4.56;
    ASSERT(bptree_put(tree, &inf, MAKE_VALUE_NUM(100)) == BPTREE_OK, "Insert inf failed");
    ASSERT(bptree_put(tree, &ninf, MAKE_VALUE_NUM(200)) == BPTREE_OK, "Insert -inf failed");
    ASSERT(bptree_put(tree, &normal, MAKE_VALUE_NUM(300)) == BPTREE_OK, "Insert normal failed");
    ASSERT(bptree_put(tree, &negzero, MAKE_VALUE_NUM(400)) == BPTREE_OK, "Insert -0.0 failed");
    ASSERT(bptree_put(tree, &poszero, MAKE_VALUE_NUM(500)) == BPTREE_OK, "Insert 0.0 failed");
    ASSERT(bptree_get(tree, &inf) != NULL, "Get inf failed");
    ASSERT(bptree_get(tree, &ninf) != NULL, "Get -inf failed");
    ASSERT(bptree_get(tree, &normal) != NULL, "Get normal failed");
    ASSERT(bptree_get(tree, &negzero) != NULL, "Get -0.0 failed");
    ASSERT(bptree_get(tree, &poszero) != NULL, "Get 0.0 failed");
    if (!is_nan_key(&nan)) {
    }
    bptree_free(tree);
}
#endif

void test_precise_boundary_conditions(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree *tree = create_test_tree_with_order(order);
        const int N = order * 3;
#ifdef BPTREE_KEY_TYPE_STRING
        char key_buf[32];
        for (int i = 0; i < N; i++) {
            sprintf(key_buf, "bound%03d", i);
            bptree_key_t k = KEY(key_buf);
            char *v = strdup(key_buf);
            track_alloc(v);
            ASSERT(bptree_put(tree, &k, v) == BPTREE_OK, "Insert failed at boundary condition");
        }
        bptree_key_t first = KEY("bound000");
        bptree_key_t last;
        sprintf(key_buf, "bound%03d", N - 1);
        last = KEY(key_buf);
        ASSERT(bptree_get(tree, &first) != NULL, "Get first key failed");
        ASSERT(bptree_get(tree, &last) != NULL, "Get last key failed");
#else
        for (int i = 0; i < N; i++) {
            int k = i + 1;
            ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK,
                   "Insert failed at boundary condition");
        }
        int first = 1;
        int last = N;
        ASSERT(bptree_get(tree, &first) != NULL, "Get first key failed");
        ASSERT(bptree_get(tree, &last) != NULL, "Get last key failed");
#endif
        bptree_free(tree);
#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();
#endif
    }
}

void test_stress(void) {
    int N = 10000;
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree *tree = create_test_tree_with_order(order);
#ifdef BPTREE_KEY_TYPE_STRING
        char key_buf[32];
        for (int i = 0; i < N; i++) {
            sprintf(key_buf, "stress%05d", i);
            bptree_key_t k = KEY(key_buf);
            char *v = strdup(key_buf);
            track_alloc(v);
            ASSERT(bptree_put(tree, &k, v) == BPTREE_OK, "Stress insert failed");
        }
        for (int i = 0; i < N; i++) {
            sprintf(key_buf, "stress%05d", i);
            bptree_key_t k = KEY(key_buf);
            const char *res = GET_VALUE_STR(bptree_get(tree, &k));
            ASSERT(res != NULL, "Stress get failed");
            ASSERT(strcmp(res, key_buf) == 0, "Stress value mismatch");
        }
#else
        for (int i = 0; i < N; i++) {
            int k = i + 1;
            ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK, "Stress insert failed");
        }
        for (int i = 0; i < N; i++) {
            int k = i + 1;
            bptree_value_t res = bptree_get(tree, &k);
            ASSERT(res != NULL, "Stress get failed");
            ASSERT(CMP_VALUE_NUM(res, k), "Stress value mismatch");
        }
#endif
        bptree_free(tree);
#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();
#endif
    }
}

int main(void) {
    fprintf(stderr, "Starting advanced B+ tree tests\n");
    RUN_TEST(test_creation_failure);
    RUN_TEST(test_insertion_and_search);
    RUN_TEST(test_deletion);
    RUN_TEST(test_empty_tree);
    RUN_TEST(test_duplicate_insertion);
    RUN_TEST(test_single_element);
    RUN_TEST(test_upsert);
    RUN_TEST(test_bulk_load_sorted);
    RUN_TEST(test_bulk_load_failures);
    RUN_TEST(test_bulk_load_empty);
    RUN_TEST(test_range_query);
    RUN_TEST(test_iterator);
    RUN_TEST(test_tree_stats);
#ifdef BPTREE_KEY_TYPE_FLOAT
    RUN_TEST(test_comparator_edge_cases_float);
#endif
#ifdef BPTREE_KEY_TYPE_DOUBLE
    RUN_TEST(test_comparator_edge_cases_double);
#endif
    RUN_TEST(test_precise_boundary_conditions);
    RUN_TEST(test_stress);
    fprintf(stderr, "Tests run: %d, failures: %d\n", tests_run, tests_failed);
    return tests_failed ? EXIT_FAILURE : EXIT_SUCCESS;
}
