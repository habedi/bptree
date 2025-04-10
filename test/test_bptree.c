/**
 * @file test_bptree.c
 * @brief Unit tests for the Bptree library.
 *
 * This file includes a collection of tests that verify the correctness
 * and robustness of the Bptree library. It covers operations such as tree creation,
 * insertion (including duplicate handling), deletion, range queries, and gathering tree statistics.
 */

#include <assert.h>
#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define DEFAULT_MAX_KEYS 32

/* Define BPTREE_IMPLEMENTATION so that the implementation is compiled */
#define BPTREE_IMPLEMENTATION
#include "bptree.h"

/* Global flag for enabling/disabling debug logging */
const bool global_debug_enabled = false;

#ifndef BPTREE_KEY_TYPE_STRING
#define MAKE_VALUE_NUM(k) ((bptree_value_t)((intptr_t)(k)))
#endif

/* Revised ASSERT macro */
#define ASSERT(cond, ...)                                         \
    do {                                                          \
        if (!(cond)) {                                            \
            fprintf(stderr, "FAIL(%s:%d): ", __FILE__, __LINE__); \
            fprintf(stderr, __VA_ARGS__);                         \
            fprintf(stderr, "\n");                                \
            tests_failed++;                                       \
        }                                                         \
    } while (0)

/* Macro to run a test */
#define RUN_TEST(test)                             \
    do {                                           \
        fprintf(stderr, "== Running %s\n", #test); \
        test();                                    \
        tests_run++;                               \
    } while (0)

#ifdef BPTREE_KEY_TYPE_STRING
/* --- String key helper definitions --- */
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
static int string_key_compare(const bptree_key_t *a, const bptree_key_t *b) {
    return strcmp(a->data, b->data);
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
/* --- Numeric key helper definitions --- */
#define KEY(s) (s)
#endif

#ifdef BPTREE_KEY_TYPE_STRING
static bptree *create_test_tree_with_order(int max_keys) {
    return bptree_create(max_keys, string_key_compare, global_debug_enabled);
}
#else
static bptree *create_test_tree_with_order(int max_keys) {
    return bptree_create(max_keys, NULL, global_debug_enabled);
}
#endif

static const int test_max_keys_values[] = {3, 4, 7, DEFAULT_MAX_KEYS};
static const int num_test_max_keys = sizeof(test_max_keys_values) / sizeof(test_max_keys_values[0]);

static int tests_run = 0;
static int tests_failed = 0;

/* Test: Creation failure for invalid order */
void test_creation_failure(void) {
    bptree *tree = bptree_create(2, NULL, global_debug_enabled);
    ASSERT(tree == NULL, "bptree_create should fail for max_keys < 3");
}

/* Test: Insertion and search operations */
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
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_OK, "Get failed for key %s", key_buf);
            ASSERT(strcmp(GET_VALUE_STR(res), key_buf) == 0, "Value mismatch for key %s", key_buf);
        }
#else
        for (int i = 0; i < N; i++) {
            bptree_key_t k = (bptree_key_t)(i * 10 + 1);
            ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK,
                   "Insert failed for key %lld", (long long)k);
        }
        for (int i = 0; i < N; i++) {
            bptree_key_t k = (bptree_key_t)(i * 10 + 1);
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_OK, "Get failed for key %lld",
                   (long long)k);
            ASSERT(((intptr_t)res) == (intptr_t)k, "Value mismatch for key %lld", (long long)k);
        }
#endif
        ASSERT(bptree_check_invariants(tree) == true, "Invariants check failed");
        bptree_free(tree);
#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();
#endif
    }
}

/* Test: Deletion operations */
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
            ASSERT(bptree_put(tree, &k, v) == BPTREE_OK, "Insert failed for key %s", key_buf);
        }
        ASSERT(tree->count == N, "Count mismatch after insertion");
        bptree_key_t k_del = KEY("del3");
        ASSERT(bptree_remove(tree, &k_del) == BPTREE_OK, "Removal failed for key 'del3'");
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k_del, &res) == BPTREE_KEY_NOT_FOUND,
                   "Get succeeded for deleted key");
        }
        ASSERT(tree->count == N - 1, "Count mismatch after deletion");
#else
        for (int i = 0; i < N; i++) {
            bptree_key_t k = (bptree_key_t)(i + 1);
            ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK,
                   "Insert failed for key %lld", (long long)k);
        }
        ASSERT(tree->count == N, "Count mismatch after insertion");
        bptree_key_t k_del = (bptree_key_t)4;
        ASSERT(bptree_remove(tree, &k_del) == BPTREE_OK, "Removal failed for key 4");
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k_del, &res) == BPTREE_KEY_NOT_FOUND,
                   "Get succeeded for deleted key 4");
        }
        ASSERT(tree->count == N - 1, "Count mismatch after deletion");
#endif
        bptree_free(tree);
#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();
#endif
    }
}

/* Test: Operations on an empty tree */
void test_empty_tree(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree *tree = create_test_tree_with_order(order);
#ifdef BPTREE_KEY_TYPE_STRING
        bptree_key_t k = KEY("anything");
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_KEY_NOT_FOUND, "Get on empty tree failed");
        }
        ASSERT(bptree_remove(tree, &k) == BPTREE_KEY_NOT_FOUND, "Remove on empty tree failed");
#else
        bptree_key_t k = (bptree_key_t)101;
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_KEY_NOT_FOUND,
                   "Get on empty tree failed for key %lld", (long long)k);
        }
        ASSERT(bptree_remove(tree, &k) == BPTREE_KEY_NOT_FOUND,
               "Remove on empty tree failed for key %lld", (long long)k);
#endif
        bptree_stats stats = bptree_get_stats(tree);
        ASSERT(stats.count == 0, "Empty tree count non-zero");
        ASSERT(stats.height == 1, "Empty tree height not 1");
        ASSERT(stats.node_count == 1, "Empty tree node_count not 1");
        bptree_free(tree);
    }
}

/* Test: Duplicate insertion handling */
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
        ASSERT(bptree_put(tree, &k, v1) == BPTREE_OK, "First insert failed");
        ASSERT(bptree_put(tree, &k, v2) == BPTREE_DUPLICATE_KEY,
               "Second insert did not return DUPLICATE_KEY");
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_OK, "Get after duplicate insert failed");
            ASSERT(strcmp(GET_VALUE_STR(res), "value1") == 0,
                   "Value overwritten on duplicate insert");
        }
#else
        bptree_key_t k = (bptree_key_t)42;
        ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK,
               "First insert failed for key %lld", (long long)k);
        ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k + 100)) == BPTREE_DUPLICATE_KEY,
               "Second insert did not return DUPLICATE_KEY for key %lld", (long long)k);
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_OK,
                   "Get after duplicate insert failed for key %lld", (long long)k);
            ASSERT(((intptr_t)res) == (intptr_t)k,
                   "Value overwritten on duplicate insert for key %lld", (long long)k);
        }
#endif
        ASSERT(bptree_check_invariants(tree) == true, "Invariants check failed");
        bptree_free(tree);
#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();
#endif
    }
}

/* Test: Operations on a tree with a single element */
void test_single_element(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree *tree = create_test_tree_with_order(order);
#ifdef BPTREE_KEY_TYPE_STRING
        bptree_key_t k = KEY("solo");
        char *v = strdup("solo_val");
        track_alloc(v);
        ASSERT(bptree_put(tree, &k, v) == BPTREE_OK, "Insert failed for solo element");
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_OK &&
                       strcmp(GET_VALUE_STR(res), "solo_val") == 0,
                   "Get failed for solo element");
        }
        ASSERT(bptree_remove(tree, &k) == BPTREE_OK, "Remove failed for solo element");
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_KEY_NOT_FOUND, "Get after remove failed");
        }
#else
        bptree_key_t k = (bptree_key_t)7;
        ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK, "Insert failed for key %lld",
               (long long)k);
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_OK && ((intptr_t)res) == (intptr_t)k,
                   "Get failed for key %lld", (long long)k);
        }
        ASSERT(bptree_remove(tree, &k) == BPTREE_OK, "Remove failed for key %lld", (long long)k);
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_KEY_NOT_FOUND,
                   "Get after remove failed for key %lld", (long long)k);
        }
#endif
        bptree_stats stats = bptree_get_stats(tree);
        ASSERT(stats.count == 0, "Count non-zero after single element removal");
        ASSERT(bptree_check_invariants(tree) == true, "Invariants check failed");
        bptree_free(tree);
#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();
#endif
    }
}

/* Test: Range query functionality */
void test_range_query(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree *tree = create_test_tree_with_order(order);
        const int N = 10;
#ifdef BPTREE_KEY_TYPE_STRING
        bptree_value_t *allocated_values = malloc(N * sizeof(bptree_value_t));
        ASSERT(allocated_values, "Allocation for range query arrays failed");
        char key_buf[32];
        for (int i = 0; i < N; i++) {
            sprintf(key_buf, "range%02d", i);
            bptree_key_t k = KEY(key_buf);
            allocated_values[i] = MAKE_VALUE_STR(key_buf);
            track_alloc(allocated_values[i]);
            ASSERT(bptree_put(tree, &k, allocated_values[i]) == BPTREE_OK,
                   "Insert failed for key %s", key_buf);
        }
        bptree_key_t start_k = KEY("range03");
        bptree_key_t end_k = KEY("range07");
        int count = 0;
        bptree_value_t *range = NULL;
        ASSERT(bptree_get_range(tree, &start_k, &end_k, &range, &count) == BPTREE_OK,
               "Range query failed");
        ASSERT(range != NULL, "Range query returned NULL");
        ASSERT(count == 5, "Range count mismatch: expected 5, got %d", count);
        free(range);
        free(allocated_values);
#else
        bptree_key_t *keys = malloc(N * sizeof(bptree_key_t));
        bptree_value_t *values = malloc(N * sizeof(bptree_value_t));
        ASSERT(keys && values, "Allocation for range query arrays failed");
        for (int i = 0; i < N; i++) {
            keys[i] = (bptree_key_t)(i * 10 + 1);
            values[i] = MAKE_VALUE_NUM(keys[i]);
            ASSERT(bptree_put(tree, &keys[i], values[i]) == BPTREE_OK, "Insert failed for key %lld",
                   (long long)keys[i]);
        }
        bptree_key_t start_k = (bptree_key_t)21;
        bptree_key_t end_k = (bptree_key_t)61;
        int count = 0;
        bptree_value_t *range = NULL;
        ASSERT(bptree_get_range(tree, &start_k, &end_k, &range, &count) == BPTREE_OK,
               "Range query failed");
        ASSERT(range != NULL, "Range query returned NULL");
        ASSERT(count == 5, "Range count mismatch: expected 5, got %d", count);
        free(range);
        free(keys);
        free(values);
#endif
        bptree_free(tree);
#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();
#endif
    }
}

/* Test: Mixed insertion and deletion stress test */
void test_mixed_insert_delete(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree *tree = create_test_tree_with_order(order);
        const int N = 100;
        /* Insert keys 1..N */
        for (int i = 1; i <= N; i++) {
            bptree_key_t key;
#ifdef BPTREE_KEY_TYPE_STRING
            char buf[16];
            sprintf(buf, "mix%d", i);
            key = KEY(buf);
            char *v = strdup(buf);
            track_alloc(v);
            ASSERT(bptree_put(tree, &key, v) == BPTREE_OK, "Mixed insert failed for key %s", buf);
#else
            key = i;
            ASSERT(bptree_put(tree, &key, MAKE_VALUE_NUM(key)) == BPTREE_OK,
                   "Mixed insert failed for key %lld", (long long)key);
#endif
        }
        ASSERT(bptree_check_invariants(tree) == true, "Invariants check failed after mixed insert");
        /* Delete even keys */
        for (int i = 2; i <= N; i += 2) {
            bptree_key_t key;
#ifdef BPTREE_KEY_TYPE_STRING
            char buf[16];
            sprintf(buf, "mix%d", i);
            key = KEY(buf);
#else
            key = i;
#endif
            ASSERT(bptree_remove(tree, &key) == BPTREE_OK, "Mixed delete failed for key %lld",
                   (long long)key);
        }
        /* Check that odd keys remain */
        for (int i = 1; i <= N; i += 2) {
            bptree_key_t key;
#ifdef BPTREE_KEY_TYPE_STRING
            char buf[16];
            sprintf(buf, "mix%d", i);
            key = KEY(buf);
#else
            key = i;
#endif
            bptree_value_t res;
            ASSERT(bptree_get(tree, &key, &res) == BPTREE_OK, "Mixed get failed for key %lld",
                   (long long)key);
        }
        /* Further delete some keys (every 3rd key) if present */
        for (int i = 1; i <= N; i += 3) {
            bptree_key_t key;
#ifdef BPTREE_KEY_TYPE_STRING
            char buf[16];
            sprintf(buf, "mix%d", i);
            key = KEY(buf);
#else
            key = i;
#endif
            bptree_status st = bptree_remove(tree, &key);
            if (st != BPTREE_OK && st != BPTREE_KEY_NOT_FOUND) {
                ASSERT(false, "Unexpected deletion status for key %lld", (long long)key);
            }
        }
        ASSERT(bptree_check_invariants(tree) == true, "Invariants check failed after mixed delete");
        bptree_free(tree);
#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();
#endif
    }
}

/* Test: Tree statistics */
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
            ASSERT(bptree_put(tree, &KEY(key_buf), NULL) == BPTREE_OK,
                   "Insert failed at boundary condition");
        }
        bptree_key_t first = KEY("bound000");
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &first, &res) == BPTREE_OK, "Get first key failed");
        }
#else
        for (int i = 0; i < N; i++) {
            bptree_key_t k = (bptree_key_t)(i + 1);
            ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK,
                   "Insert failed for key %lld", (long long)k);
        }
        bptree_key_t first = (bptree_key_t)1;
        bptree_key_t last = (bptree_key_t)N;
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &first, &res) == BPTREE_OK, "Get first key failed for key %lld",
                   (long long)first);
            ASSERT(bptree_get(tree, &last, &res) == BPTREE_OK, "Get last key failed for key %lld",
                   (long long)last);
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

/* Test: Precise boundary conditions */
void test_precise_boundary_conditions(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree *tree = create_test_tree_with_order(order);
        const int N = order * 3;
#ifdef BPTREE_KEY_TYPE_STRING
        char key_buf[32];
        for (int i = 0; i < N; i++) {
            sprintf(key_buf, "bound%03d", i);
            ASSERT(bptree_put(tree, &KEY(key_buf), NULL) == BPTREE_OK,
                   "Insert failed for key %s at boundary condition", key_buf);
        }
        bptree_key_t first = KEY("bound000");
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &first, &res) == BPTREE_OK, "Get first key failed");
        }
#else
        for (int i = 0; i < N; i++) {
            bptree_key_t k = (bptree_key_t)(i + 1);
            ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK,
                   "Insert failed for key %lld at boundary condition", (long long)k);
        }
        bptree_key_t first = (bptree_key_t)1;
        bptree_key_t last = (bptree_key_t)N;
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &first, &res) == BPTREE_OK, "Get first key failed for key %lld",
                   (long long)first);
            ASSERT(bptree_get(tree, &last, &res) == BPTREE_OK, "Get last key failed for key %lld",
                   (long long)last);
        }
#endif
        bptree_free(tree);
#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();
#endif
    }
}

/* Test: Stress test */
void test_stress(void) {
    int N = 10000;
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree *tree = create_test_tree_with_order(order);
#ifdef BPTREE_KEY_TYPE_STRING
        char key_buf[32];
        for (int i = 0; i < N; i++) {
            sprintf(key_buf, "stress%05d", i);
            ASSERT(bptree_put(tree, &KEY(key_buf), MAKE_VALUE_STR(key_buf)) == BPTREE_OK,
                   "Stress insert failed for key %s", key_buf);
        }
        for (int i = 0; i < N; i++) {
            sprintf(key_buf, "stress%05d", i);
            bptree_value_t res;
            ASSERT(bptree_get(tree, &KEY(key_buf), &res) == BPTREE_OK,
                   "Stress get failed for key %s", key_buf);
            ASSERT(strcmp(GET_VALUE_STR(res), key_buf) == 0, "Stress value mismatch for key %s",
                   key_buf);
        }
#else
        for (int i = 0; i < N; i++) {
            bptree_key_t k = (bptree_key_t)(i + 1);
            ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK,
                   "Stress insert failed for key %lld", (long long)k);
        }
        for (int i = 0; i < N; i++) {
            bptree_key_t k = (bptree_key_t)(i + 1);
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_OK, "Stress get failed for key %lld",
                   (long long)k);
            ASSERT(((intptr_t)res) == (intptr_t)k, "Stress value mismatch for key %lld",
                   (long long)k);
        }
#endif
        bptree_free(tree);
    }
}

/**
 * @brief Main entry point for the test suite.
 *
 * This function runs all the tests and demonstrates a simple API usage example inline.
 */
int main(void) {
    fprintf(stderr, "Starting advanced B+ tree tests\n");

    /* --- Inline API Usage Example --- */
#ifdef BPTREE_KEY_TYPE_STRING
    {
        bptree *tree = bptree_create(5, string_key_compare, global_debug_enabled);
        if (tree) {
            printf("API Usage Example: Created tree with max_keys = 5 using string keys.\n");
            ASSERT(bptree_contains(tree, (bptree_key_t[]){KEY("example")}) == false,
                   "Tree should be empty");
            ASSERT(tree->count == 0, "Tree count should be 0");
            bptree_free(tree);
        } else {
            fprintf(stderr, "API usage example: tree creation failed.\n");
        }
    }
#else
    {
        bptree *tree = bptree_create(5, NULL, global_debug_enabled);
        if (tree) {
            printf("API Usage Example: Created tree with max_keys = 5 using numeric keys.\n");
            bptree_key_t example = 42;
            ASSERT(bptree_contains(tree, &example) == false, "Tree should be empty");
            ASSERT(tree->count == 0, "Tree count should be 0");
            bptree_free(tree);
        } else {
            fprintf(stderr, "API usage example: tree creation failed.\n");
        }
    }
#endif

    /* --- Run tests --- */
    RUN_TEST(test_creation_failure);
    RUN_TEST(test_insertion_and_search);
    RUN_TEST(test_deletion);
    RUN_TEST(test_empty_tree);
    RUN_TEST(test_duplicate_insertion);
    RUN_TEST(test_single_element);
    RUN_TEST(test_range_query);
    RUN_TEST(test_tree_stats);
    RUN_TEST(test_precise_boundary_conditions);
    RUN_TEST(test_stress);
    RUN_TEST(test_mixed_insert_delete);

    fprintf(stderr, "Tests run: %d, failures: %d\n", tests_run, tests_failed);
    return tests_failed ? EXIT_FAILURE : EXIT_SUCCESS;
}
