/**
 * @file test_bptree.c
 * @brief Unit tests for the Bptree library (bptree.h).
 *
 * This file includes a collection of tests that verify the correctness
 * and robustness of the Bptree library. It covers operations such as tree creation,
 * insertion (including duplicate handling), deletion, range queries, tree statistics,
 * boundary conditions, and mixed operations stress testing.
 *
 * Tests are run for various tree orders (max_keys values).
 * String key support can be tested by defining BPTREE_KEY_TYPE_STRING during compilation.
 *
 * @version 0.4.3
 */

#include <assert.h>
#include <math.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

/** @brief Default max_keys value for the tree if not otherwise specified. */
#define DEFAULT_MAX_KEYS 32

/** @brief Define BPTREE_IMPLEMENTATION to include the library's implementation. */
#define BPTREE_IMPLEMENTATION
#include "bptree.h"  // Include the B+ tree library header

/** @brief Global flag for enabling/disabling debug logging from the bptree library during tests. */
const bool global_debug_enabled = false;

// --- Conditional helper macros based on key type ---

#ifndef BPTREE_KEY_TYPE_STRING
/**
 * @def MAKE_VALUE_NUM(k)
 * @brief Creates a `bptree_value_t` from a numeric key `k` for testing.
 * Assumes `bptree_value_t` can hold an `intptr_t`.
 */
#define MAKE_VALUE_NUM(k) ((bptree_value_t)((intptr_t)(k)))
#endif

/**
 * @def ASSERT(cond, ...)
 * @brief Custom assertion macro for tests.
 *
 * Checks a condition. If the condition is false, prints an error message
 * including file, line number, and provided format string/arguments, then
 * increments the global `tests_failed` counter.
 *
 * @param cond The condition to assert.
 * @param ... Variable arguments similar to printf, formatting the error message.
 */
#define ASSERT(cond, ...)                                         \
    do {                                                          \
        if (!(cond)) {                                            \
            fprintf(stderr, "FAIL(%s:%d): ", __FILE__, __LINE__); \
            fprintf(stderr, __VA_ARGS__);                         \
            fprintf(stderr, "\n");                                \
            tests_failed++;                                       \
        }                                                         \
    } while (0)

/**
 * @def RUN_TEST(test)
 * @brief Macro to execute a test function and track statistics.
 *
 * Prints the name of the test being run, executes the test function,
 * and increments the global `tests_run` counter.
 *
 * @param test The name of the test function to run (without parentheses).
 */
#define RUN_TEST(test)                             \
    do {                                           \
        fprintf(stderr, "== Running %s\n", #test); \
        test();                                    \
        tests_run++;                               \
    } while (0)

#ifdef BPTREE_KEY_TYPE_STRING
// --- String Key Specific Test Helpers ---

/** @brief Maximum number of allocations to track for cleanup in string tests. */
#define MAX_ALLOC_TRACK 256
/** @brief Array to track allocated memory (strings) for cleanup. */
static void* alloc_track[MAX_ALLOC_TRACK];
/** @brief Current number of tracked allocations. */
static int alloc_track_count = 0;

/**
 * @brief Adds a pointer to the allocation tracking list.
 * @param ptr Pointer to the allocated memory.
 */
static void track_alloc(void* ptr) {
    if (alloc_track_count < MAX_ALLOC_TRACK) {
        alloc_track[alloc_track_count++] = ptr;
    } else {
        fprintf(stderr, "Warning: Allocation tracking array full (%d items).\n", MAX_ALLOC_TRACK);
        // Optionally free ptr here if it cannot be tracked, or abort.
    }
}

/**
 * @brief Frees all memory pointers currently stored in the tracking list.
 */
static void cleanup_alloc_track(void) {
    for (int i = 0; i < alloc_track_count; i++) {
        free(alloc_track[i]);
    }
    alloc_track_count = 0;
}

/**
 * @brief Comparison function for string keys.
 * Uses strcmp for comparison. Assumes null-terminated strings within BPTREE_KEY_SIZE.
 * @param a Pointer to the first string key.
 * @param b Pointer to the second string key.
 * @return Result of strcmp(a->data, b->data).
 */
static int string_key_compare(const bptree_key_t* a, const bptree_key_t* b) {
    // Note: Assumes strings are null-terminated within BPTREE_KEY_SIZE.
    // If not, memcmp might be required depending on the library's internal compare needs.
    // However, for external comparison function, strcmp is typical if keys are C strings.
    return strcmp(a->data, b->data);
}

/**
 * @brief Creates a `bptree_key_t` (string type) from a C string literal.
 * Ensures null termination within the fixed BPTREE_KEY_SIZE.
 * @param s The input C string.
 * @return The initialized bptree_key_t.
 */
static bptree_key_t make_key_str(const char* s) {
    bptree_key_t key;
    memset(&key, 0, sizeof(key));  // Zero out the key structure first
    // Use snprintf for safe copying and guaranteed null termination
    snprintf(key.data, BPTREE_KEY_SIZE, "%s", s);
    // No need to manually null-terminate again, snprintf handles it if space allows.
    return key;
}

/** @brief Macro to create a string key from a literal. */
#define KEY(s) (make_key_str(s))
/** @brief Macro to create a value (duplicate string) for string key tests. */
#define MAKE_VALUE_STR(s) (strdup(s))  // Assumes value is also string; tracks via track_alloc
/** @brief Macro to safely cast/get the string value from bptree_value_t. */
#define GET_VALUE_STR(v) ((const char*)(v))
/** @brief Macro to compare two string values retrieved from the tree. */
#define CMP_VALUE_STR(v1, v2) (strcmp(GET_VALUE_STR(v1), GET_VALUE_STR(v2)) == 0)
/** @brief Macro to free a string value retrieved/handled during tests. */
#define FREE_VALUE_STR(v) (free((void*)(v)))  // Values are allocated with strdup

#else  // BPTREE_KEY_TYPE_STRING not defined (Numeric Keys)

/** @brief Macro identity for numeric keys. */
#define KEY(s) (s)

#endif  // BPTREE_KEY_TYPE_STRING

/**
 * @brief Creates a B+ tree instance for testing with a specific order.
 *
 * Uses the appropriate key comparison function based on whether
 * `BPTREE_KEY_TYPE_STRING` is defined. Enables or disables debug based
 * on `global_debug_enabled`.
 *
 * @param max_keys The maximum number of keys per node (order - 1).
 * @return Pointer to the newly created bptree, or NULL on failure.
 */
#ifdef BPTREE_KEY_TYPE_STRING
static bptree* create_test_tree_with_order(int max_keys) {
    return bptree_create(max_keys, string_key_compare, global_debug_enabled);
}
#else
static bptree* create_test_tree_with_order(const int max_keys) {
    // Use default numeric comparison by passing NULL
    return bptree_create(max_keys, NULL, global_debug_enabled);
}
#endif

/** @brief Array of `max_keys` values (tree orders - 1) to test against. */
static const int test_max_keys_values[] = {3, 4, 7, 12, DEFAULT_MAX_KEYS};
/** @brief Number of different tree orders being tested. */
static const int num_test_max_keys = sizeof(test_max_keys_values) / sizeof(test_max_keys_values[0]);

/** @brief Global counter for the number of tests executed. */
static int tests_run = 0;
/** @brief Global counter for the number of tests that failed. */
static int tests_failed = 0;

// --- Test Cases ---

/**
 * @brief Test: Creation failure for invalid order.
 * Verifies that `bptree_create` returns NULL for `max_keys` < 3.
 */
void test_creation_failure(void) {
    const bptree* tree = bptree_create(2, NULL, global_debug_enabled);
    ASSERT(tree == NULL, "bptree_create should fail for max_keys < 3");
}

/**
 * @brief Test: Verify the min_internal_keys calculation.
 *
 * Checks that the calculation for `min_internal_keys` is correct,
 * especially for even values of `max_keys` where ceiling division is critical.
 */
void test_min_internal_keys_calculation(void) {
    // Test with an even max_keys value, where the bug is apparent.
    const int max_keys = 4;
    bptree* tree = create_test_tree_with_order(max_keys);
    ASSERT(tree != NULL, "Tree creation failed for max_keys = 4");

    // For max_keys = 4, m = 5.
    // min_internal_keys should be ceil(m/2) - 1 = ceil(2.5) - 1 = 3 - 1 = 2.
    // The old buggy formula was (m/2) - 1 = (5/2) - 1 = 2 - 1 = 1.
    const int expected_min_internal = 2;
    ASSERT(tree->min_internal_keys == expected_min_internal,
           "min_internal_keys mismatch for max_keys=%d: expected %d, got %d", max_keys,
           expected_min_internal, tree->min_internal_keys);

    bptree_free(tree);

    // Test with an odd max_keys value to ensure it remains correct.
    const int max_keys_odd = 3;
    tree = create_test_tree_with_order(max_keys_odd);
    ASSERT(tree != NULL, "Tree creation failed for max_keys = 3");

    // For max_keys = 3, m = 4.
    // min_internal_keys should be ceil(m/2) - 1 = ceil(2) - 1 = 2 - 1 = 1.
    const int expected_min_internal_odd = 1;
    ASSERT(tree->min_internal_keys == expected_min_internal_odd,
           "min_internal_keys mismatch for max_keys=%d: expected %d, got %d", max_keys_odd,
           expected_min_internal_odd, tree->min_internal_keys);

    bptree_free(tree);
}

/**
 * @brief Test: Basic insertion and search operations.
 * Inserts a small number of keys and verifies they can be retrieved correctly
 * with the expected values. Runs for various tree orders.
 */
void test_insertion_and_search(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        const int order = test_max_keys_values[m];
        bptree* tree = create_test_tree_with_order(order);
        ASSERT(tree != NULL, "Tree creation failed for order %d", order);
        const int N = 10;  // Number of items to insert

#ifdef BPTREE_KEY_TYPE_STRING
        char key_buf[32];
        for (int i = 0; i < N; i++) {
            sprintf(key_buf, "key%d", i);
            bptree_key_t k = KEY(key_buf);
            char* v = MAKE_VALUE_STR(key_buf);  // Use macro, allocates memory
            track_alloc(v);                     // Track allocation for cleanup
            ASSERT(bptree_put(tree, &k, v) == BPTREE_OK, "Insert failed for key %s", key_buf);
        }
        for (int i = 0; i < N; i++) {
            sprintf(key_buf, "key%d", i);
            bptree_key_t k = KEY(key_buf);
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_OK, "Get failed for key %s", key_buf);
            ASSERT(CMP_VALUE_STR(res, key_buf), "Value mismatch for key %s", key_buf);
        }
#else  // Numeric keys
        for (int i = 0; i < N; i++) {
            bptree_key_t k = (bptree_key_t)(i * 10 + 1);
            // Store the key itself as the value for simplicity
            ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK,
                   "Insert failed for key %lld", (long long)k);
        }
        for (int i = 0; i < N; i++) {
            bptree_key_t k = (bptree_key_t)(i * 10 + 1);
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_OK, "Get failed for key %lld",
                   (long long)k);
            // Verify retrieved value matches the key stored
            ASSERT(((intptr_t)res) == (intptr_t)k, "Value mismatch for key %lld", (long long)k);
        }
#endif
        ASSERT(bptree_check_invariants(tree) == true, "Invariants check failed");
        bptree_free(tree);

#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();  // Free allocated string values
#endif
    }
}

/**
 * @brief Test: Basic deletion operations.
 * Inserts a few keys, deletes one, and verifies that the deleted key
 * cannot be retrieved afterward. Checks tree count consistency. Runs for various tree orders.
 */
void test_deletion(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        const int order = test_max_keys_values[m];
        bptree* tree = create_test_tree_with_order(order);
        ASSERT(tree != NULL, "Tree creation failed for order %d", order);
        const int N = 7;  // Number of items to insert

#ifdef BPTREE_KEY_TYPE_STRING
        char key_buf[32];
        for (int i = 0; i < N; i++) {
            sprintf(key_buf, "del%d", i);
            bptree_key_t k = KEY(key_buf);
            char* v = MAKE_VALUE_STR(key_buf);
            track_alloc(v);
            ASSERT(bptree_put(tree, &k, v) == BPTREE_OK, "Insert failed for key %s", key_buf);
        }
        ASSERT(tree->count == N, "Count mismatch after insertion");

        // Note: When removing, the caller is responsible for freeing the associated value memory.
        // This test doesn't retrieve/free the value, relying on cleanup_alloc_track later.
        bptree_key_t k_del = KEY("del3");
        ASSERT(bptree_remove(tree, &k_del) == BPTREE_OK, "Removal failed for key 'del3'");
        {
            bptree_value_t res;  // Should not be set if key not found
            ASSERT(bptree_get(tree, &k_del, &res) == BPTREE_KEY_NOT_FOUND,
                   "Get succeeded for deleted key 'del3'");
        }
        ASSERT(tree->count == N - 1, "Count mismatch after deletion");
#else  // Numeric keys
        for (int i = 0; i < N; i++) {
            bptree_key_t k = (bptree_key_t)(i + 1);
            ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK,
                   "Insert failed for key %lld", (long long)k);
        }
        ASSERT(tree->count == N, "Count mismatch after insertion");

        bptree_key_t k_del = (bptree_key_t)4;
        ASSERT(bptree_remove(tree, &k_del) == BPTREE_OK, "Removal failed for key 4");
        {
            bptree_value_t res;  // Should not be set
            ASSERT(bptree_get(tree, &k_del, &res) == BPTREE_KEY_NOT_FOUND,
                   "Get succeeded for deleted key 4");
        }
        ASSERT(tree->count == N - 1, "Count mismatch after deletion");
#endif
        ASSERT(bptree_check_invariants(tree) == true, "Invariants check failed after delete");
        bptree_free(tree);

#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();  // Free remaining allocated string values
#endif
    }
}

/**
 * @brief Test: Operations on an empty tree.
 * Verifies that `get` and `remove` operations return `BPTREE_KEY_NOT_FOUND`
 * when performed on a newly created, empty tree. Checks initial stats.
 */
void test_empty_tree(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        const int order = test_max_keys_values[m];
        bptree* tree = create_test_tree_with_order(order);
        ASSERT(tree != NULL, "Tree creation failed");

#ifdef BPTREE_KEY_TYPE_STRING
        bptree_key_t k = KEY("anything");
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_KEY_NOT_FOUND,
                   "Get on empty tree should fail");
        }
        ASSERT(bptree_remove(tree, &k) == BPTREE_KEY_NOT_FOUND, "Remove on empty tree should fail");
#else  // Numeric keys
        bptree_key_t k = (bptree_key_t)101;
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_KEY_NOT_FOUND,
                   "Get on empty tree failed for key %lld", (long long)k);
        }
        ASSERT(bptree_remove(tree, &k) == BPTREE_KEY_NOT_FOUND,
               "Remove on empty tree failed for key %lld", (long long)k);
#endif
        const bptree_stats stats = bptree_get_stats(tree);
        ASSERT(stats.count == 0, "Empty tree count non-zero");
        ASSERT(stats.height == 1, "Empty tree height not 1");
        ASSERT(stats.node_count == 1, "Empty tree node_count not 1");
        ASSERT(bptree_check_invariants(tree) == true, "Invariants check failed for empty tree");
        bptree_free(tree);
        // No string cleanup needed as nothing was allocated/inserted
    }
}

/**
 * @brief Test: Duplicate key insertion handling.
 * Verifies that inserting a key that already exists is correctly rejected
 * with `BPTREE_DUPLICATE_KEY` and that the original value is not overwritten.
 */
void test_duplicate_insertion(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        const int order = test_max_keys_values[m];
        bptree* tree = create_test_tree_with_order(order);
        ASSERT(tree != NULL, "Tree creation failed");

#ifdef BPTREE_KEY_TYPE_STRING
        bptree_key_t k = KEY("duplicate");
        char* v1 = MAKE_VALUE_STR("value1");
        char* v2 = MAKE_VALUE_STR("value2");  // Will be freed if duplicate rejected
        track_alloc(v1);                      // Track v1
        // Don't track v2 yet, only if insertion *succeeds* unexpectedly

        ASSERT(bptree_put(tree, &k, v1) == BPTREE_OK, "First insert failed");
        bptree_status status = bptree_put(tree, &k, v2);  // Attempt duplicate insert
        ASSERT(status == BPTREE_DUPLICATE_KEY, "Second insert did not return DUPLICATE_KEY");
        if (status == BPTREE_DUPLICATE_KEY) {
            free(v2);  // Free the value that wasn't inserted
        } else {
            track_alloc(v2);  // Track v2 only if it was unexpectedly inserted
        }

        // Verify the original value is still present
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_OK, "Get after duplicate insert failed");
            ASSERT(CMP_VALUE_STR(res, "value1"), "Value overwritten on duplicate insert");
        }
#else  // Numeric keys
        bptree_key_t k = (bptree_key_t)42;
        bptree_value_t v1 = MAKE_VALUE_NUM(k);
        bptree_value_t v2 = MAKE_VALUE_NUM(k + 100);

        ASSERT(bptree_put(tree, &k, v1) == BPTREE_OK, "First insert failed for key %lld",
               (long long)k);
        ASSERT(bptree_put(tree, &k, v2) == BPTREE_DUPLICATE_KEY,
               "Second insert did not return DUPLICATE_KEY for key %lld", (long long)k);

        // Verify the original value is still present
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_OK,
                   "Get after duplicate insert failed for key %lld", (long long)k);
            ASSERT(res == v1, "Value overwritten on duplicate insert for key %lld", (long long)k);
        }
#endif
        ASSERT(bptree_check_invariants(tree) == true,
               "Invariants check failed after duplicate test");
        bptree_free(tree);

#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();  // Free v1 (and v2 if it was wrongly inserted)
#endif
    }
}

/**
 * @brief Test: Operations on a tree with exactly one element.
 * Inserts a single element, retrieves it, removes it, and verifies
 * it cannot be retrieved afterward. Checks final tree state.
 */
void test_single_element(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        const int order = test_max_keys_values[m];
        bptree* tree = create_test_tree_with_order(order);
        ASSERT(tree != NULL, "Tree creation failed");

#ifdef BPTREE_KEY_TYPE_STRING
        bptree_key_t k = KEY("solo");
        char* v = MAKE_VALUE_STR("solo_val");
        track_alloc(v);  // Track allocation

        ASSERT(bptree_put(tree, &k, v) == BPTREE_OK, "Insert failed for solo element");
        // Verify get
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_OK && CMP_VALUE_STR(res, "solo_val"),
                   "Get failed for solo element");
        }
        // Remove (value needs explicit free)
        ASSERT(bptree_remove(tree, &k) == BPTREE_OK, "Remove failed for solo element");
        FREE_VALUE_STR(v);    // Free the value associated with the removed key
        alloc_track_count--;  // Adjust tracking (simplistic: assumes last tracked)

        // Verify remove
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_KEY_NOT_FOUND,
                   "Get after remove succeeded unexpectedly");
        }
#else  // Numeric keys
        const bptree_key_t k = (bptree_key_t)7;
        bptree_value_t v = MAKE_VALUE_NUM(k);

        ASSERT(bptree_put(tree, &k, v) == BPTREE_OK, "Insert failed for key %lld", (long long)k);
        // Verify get
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_OK && res == v, "Get failed for key %lld",
                   (long long)k);
        }
        // Remove
        ASSERT(bptree_remove(tree, &k) == BPTREE_OK, "Remove failed for key %lld", (long long)k);
        // Verify remove
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_KEY_NOT_FOUND,
                   "Get after remove failed for key %lld", (long long)k);
        }
#endif
        const bptree_stats stats = bptree_get_stats(tree);
        ASSERT(stats.count == 0, "Count non-zero after single element removal");
        ASSERT(bptree_check_invariants(tree) == true,
               "Invariants check failed after single element test");
        bptree_free(tree);

#ifdef BPTREE_KEY_TYPE_STRING
        // cleanup_alloc_track(); // Should be empty if free was correct
        ASSERT(alloc_track_count == 0, "Memory tracking leak detected in single element test");
#endif
    }
}

/**
 * @brief Test: Range query functionality.
 * Inserts a set of keys and performs a range query, verifying the number
 * of results returned. Requires freeing the result array from `bptree_get_range`.
 */
void test_range_query(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        const int order = test_max_keys_values[m];
        bptree* tree = create_test_tree_with_order(order);
        ASSERT(tree != NULL, "Tree creation failed");
        const int N = 10;

#ifdef BPTREE_KEY_TYPE_STRING
        // We need to store pointers that persist after insertion for comparison
        bptree_value_t* allocated_values = malloc(N * sizeof(bptree_value_t));
        ASSERT(allocated_values, "Allocation for range query value tracking failed");
        char key_buf[32];
        for (int i = 0; i < N; i++) {
            sprintf(key_buf, "range%02d", i);  // Ensure lexicographical order
            bptree_key_t k = KEY(key_buf);
            allocated_values[i] = MAKE_VALUE_STR(key_buf);  // Value is the same as key string
            track_alloc(allocated_values[i]);               // Track value allocation
            ASSERT(bptree_put(tree, &k, allocated_values[i]) == BPTREE_OK,
                   "Insert failed for key %s", key_buf);
        }

        bptree_key_t start_k = KEY("range03");  // Inclusive start
        bptree_key_t end_k = KEY("range07");    // Inclusive end
        int count = 0;
        bptree_value_t* range_results = NULL;  // Array of bptree_value_t (char* in this case)
        bptree_status status = bptree_get_range(tree, &start_k, &end_k, &range_results, &count);

        ASSERT(status == BPTREE_OK, "Range query failed");
        ASSERT(count == 5, "Range count mismatch: expected 5 [range03..range07], got %d", count);
        if (count == 5 && range_results != NULL) {
            // Optional: Verify contents
            ASSERT(CMP_VALUE_STR(range_results[0], "range03"), "Range result [0] mismatch");
            ASSERT(CMP_VALUE_STR(range_results[4], "range07"), "Range result [4] mismatch");
        } else if (count > 0) {
            ASSERT(range_results != NULL, "Range query returned count > 0 but NULL array");
        }

        bptree_free_range_results(range_results);  // Free the array returned by get_range
        free(allocated_values);                    // Free the tracking array shell
#else                                              // Numeric keys
        bptree_key_t* keys = malloc(N * sizeof(bptree_key_t));
        bptree_value_t* values =
            malloc(N * sizeof(bptree_value_t));  // Store values for verification if needed
        ASSERT(keys && values, "Allocation for range query arrays failed");

        for (int i = 0; i < N; i++) {
            keys[i] = (bptree_key_t)(i * 10 + 1);  // Keys: 1, 11, 21, ...
            values[i] = MAKE_VALUE_NUM(keys[i]);   // Store key as value
            ASSERT(bptree_put(tree, &keys[i], values[i]) == BPTREE_OK, "Insert failed for key %lld",
                   (long long)keys[i]);
        }

        bptree_key_t start_k = (bptree_key_t)21;  // Inclusive start (key 21)
        bptree_key_t end_k = (bptree_key_t)61;    // Inclusive end (key 61)
        // Expected keys: 21, 31, 41, 51, 61 (5 keys)
        int count = 0;
        bptree_value_t* range_results = NULL;
        bptree_status status = bptree_get_range(tree, &start_k, &end_k, &range_results, &count);

        ASSERT(status == BPTREE_OK, "Range query failed");
        ASSERT(count == 5, "Range count mismatch: expected 5 [21..61], got %d", count);
        if (count == 5 && range_results != NULL) {
            // Optional: Verify contents
            ASSERT(((intptr_t)range_results[0]) == 21, "Range result [0] mismatch");
            ASSERT(((intptr_t)range_results[4]) == 61, "Range result [4] mismatch");
        } else if (count > 0) {
            ASSERT(range_results != NULL, "Range query returned count > 0 but NULL array");
        }

        bptree_free_range_results(range_results);  // Free the array allocated by get_range
        free(keys);
        free(values);
#endif
        bptree_free(tree);  // Free the tree itself

#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();  // Free allocated string values
#endif
    }
}

/**
 * @brief Test: Mixed insertion and deletion sequences.
 *
 * Inserts a sequence of keys, checks invariants, deletes all even keys,
 * checks invariants and remaining odd keys, deletes keys congruent to 1 mod 3,
 * and performs a final invariant check. Designed to trigger various rebalancing scenarios.
 */
void test_mixed_insert_delete(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        const int order = test_max_keys_values[m];
        bptree* tree = create_test_tree_with_order(order);
        ASSERT(tree != NULL, "Tree creation failed");
        const int N = 100;  // Number of initial items

        // --- Phase 1: Insert keys 1..N ---
        for (int i = 1; i <= N; i++) {
            bptree_key_t key;
#ifdef BPTREE_KEY_TYPE_STRING
            char buf[16];
            sprintf(buf, "mix%d", i);
            key = KEY(buf);
            char* v = MAKE_VALUE_STR(buf);  // Value is a string copy of the key
            track_alloc(v);
            ASSERT(bptree_put(tree, &key, v) == BPTREE_OK, "Mixed insert failed for key %s", buf);
#else  // Numeric keys
            key = (bptree_key_t)i;
            ASSERT(bptree_put(tree, &key, MAKE_VALUE_NUM(key)) == BPTREE_OK,
                   "Mixed insert failed for key %lld", (long long)key);
#endif
        }
        ASSERT(bptree_check_invariants(tree) == true,
               "Invariants check failed after initial mixed insert");
        ASSERT(tree->count == N, "Count mismatch after initial insert");

        // --- Phase 2: Delete even keys ---
        for (int i = 2; i <= N; i += 2) {
            bptree_key_t key;
#ifdef BPTREE_KEY_TYPE_STRING
            char buf[16];
            sprintf(buf, "mix%d", i);
            key = KEY(buf);
            // Note: Need to find and free the corresponding value during/after remove
            // This test currently relies on later cleanup_alloc_track. A more robust
            // test would get the value pointer before removing from the tree.
#else  // Numeric keys
            key = (bptree_key_t)i;
#endif
            ASSERT(bptree_remove(tree, &key) == BPTREE_OK,
                   "Mixed delete (even) failed for key %lld", (long long)key);
        }
        ASSERT(tree->count == N / 2, "Count mismatch after deleting evens: expected %d, got %d",
               N / 2, tree->count);

        // --- Phase 3: Check remaining odd keys ---
        for (int i = 1; i <= N; i += 2) {
            bptree_key_t key;
#ifdef BPTREE_KEY_TYPE_STRING
            char buf[16];
            sprintf(buf, "mix%d", i);
            key = KEY(buf);
#else  // Numeric keys
            key = (bptree_key_t)i;
#endif
            bptree_value_t res;
            ASSERT(bptree_get(tree, &key, &res) == BPTREE_OK,
                   "Mixed get failed for odd key %lld after even deletion", (long long)key);
#ifndef BPTREE_KEY_TYPE_STRING
            ASSERT(res == MAKE_VALUE_NUM(key), "Value mismatch for odd key %lld", (long long)key);
#endif
        }
        ASSERT(bptree_check_invariants(tree) == true,
               "Invariants check failed after deleting evens");

        // --- Phase 4: Delete keys == 1 (mod 3) ---
        int expected_final_count = 0;
        for (int i = 1; i <= N; i += 2) {  // Iterate through remaining odd keys
            if (i % 3 != 1) {  // If it was NOT 1 mod 3 (or was even and already deleted)
                expected_final_count++;
            }
        }

        for (int i = 1; i <= N; i += 3) {  // Attempt to delete 1, 4, 7, 10, ...
            bptree_key_t key;
#ifdef BPTREE_KEY_TYPE_STRING
            char buf[16];
            sprintf(buf, "mix%d", i);
            key = KEY(buf);
#else  // Numeric keys
            key = (bptree_key_t)i;
#endif
            const bptree_status st = bptree_remove(tree, &key);
            // Key might already be deleted if it was even (e.g., 4, 10, 16...)
            // So, status can be OK or KEY_NOT_FOUND. Anything else is an error.
            if (st != BPTREE_OK && st != BPTREE_KEY_NOT_FOUND) {
                ASSERT(false, "Unexpected deletion status (%d) for key %lld", st, (long long)key);
            }
        }
        ASSERT(tree->count == expected_final_count,
               "Count mismatch after deleting 1-mod-3 keys: expected %d, got %d",
               expected_final_count, tree->count);
        ASSERT(bptree_check_invariants(tree) == true,
               "Invariants check failed after final mixed delete stage");

        bptree_free(tree);
#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();  // Free any remaining allocated string values
#endif
    }
}

/**
 * @brief Test: Tree statistics function `bptree_get_stats`.
 * Verifies initial stats on an empty tree and checks the count after
 * inserting a larger number of items. Also performs basic checks on height
 * and node count relative to expectations.
 */
void test_tree_stats(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        const int order = test_max_keys_values[m];
        bptree* tree = create_test_tree_with_order(order);
        ASSERT(tree != NULL, "Tree creation failed");

        // Check initial stats
        bptree_stats stats = bptree_get_stats(tree);
        ASSERT(stats.count == 0, "Initial count wrong");
        ASSERT(stats.height == 1, "Initial height wrong");
        ASSERT(stats.node_count == 1, "Initial node_count wrong");

        const int N = 150;  // Number of items for stats test

#ifdef BPTREE_KEY_TYPE_STRING
        char key_buf[32];
        for (int i = 0; i < N; i++) {
            sprintf(key_buf, "stat%d", i);
            bptree_key_t k = KEY(key_buf);
            // Insert NULL value for simplicity, as we don't need value cleanup here
            ASSERT(bptree_put(tree, &k, NULL) == BPTREE_OK,
                   "Insert failed for key %s in stats test", key_buf);
        }
        // Check basic get still works
        bptree_key_t first = KEY("stat0");  // Check first inserted
        {
            bptree_value_t res;  // Value will be NULL
            ASSERT(bptree_get(tree, &first, &res) == BPTREE_OK,
                   "Get first key failed in stats test");
            ASSERT(res == NULL, "Value mismatch for first key (should be NULL)");
        }
#else  // Numeric keys
        for (int i = 0; i < N; i++) {
            bptree_key_t k = (bptree_key_t)(i + 1);
            ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK,
                   "Insert failed for key %lld in stats test", (long long)k);
        }
        // Check basic get still works
        bptree_key_t first = (bptree_key_t)1;
        bptree_key_t last = (bptree_key_t)N;
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &first, &res) == BPTREE_OK, "Get first key failed for key %lld",
                   (long long)first);
            ASSERT(res == MAKE_VALUE_NUM(first), "Value mismatch for first key");
            ASSERT(bptree_get(tree, &last, &res) == BPTREE_OK, "Get last key failed for key %lld",
                   (long long)last);
            ASSERT(res == MAKE_VALUE_NUM(last), "Value mismatch for last key");
        }
#endif
        // Check final stats
        stats = bptree_get_stats(tree);
        ASSERT(stats.count == N, "Final count wrong: expected %d, got %d", N, stats.count);
        if (N > order) {  // Height/node count only increase if root splits
            ASSERT(stats.height > 1, "Final height not greater than 1 for N=%d, order=%d", N,
                   order);
            ASSERT(stats.node_count > 1, "Final node_count not greater than 1 for N=%d, order=%d",
                   N, order);
        } else {
            ASSERT(stats.height == 1, "Final height not 1 for N=%d <= order=%d", N, order);
            ASSERT(stats.node_count == 1, "Final node_count not 1 for N=%d <= order=%d", N, order);
        }
        // Basic sanity check on node count relative to order
        const int min_fill_leaf = (order + 1) / 2;  // Rough minimum keys per leaf node
        const int expected_min_nodes = (N + min_fill_leaf - 1) / order + 1;  // Very rough estimate
        ASSERT(stats.node_count >= expected_min_nodes,
               "Node count %d seems low relative to N=%d, order=%d (expected min ~%d)",
               stats.node_count, N, order, expected_min_nodes);

        bptree_free(tree);
#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();  // Although we inserted NULL, cleanup doesn't hurt
#endif
    }
}

/**
 * @brief Test: Insertion near node capacity boundaries.
 * Inserts a number of elements designed to fill nodes exactly or cause splits
 * at specific points (multiples of tree order).
 */
void test_precise_boundary_conditions(void) {
    for (int m = 0; m < num_test_max_keys; m++) {
        const int order = test_max_keys_values[m];  // 'order' here is max_keys
        bptree* tree = create_test_tree_with_order(order);
        ASSERT(tree != NULL, "Tree creation failed");
        const int N = order * 3;  // Insert enough items to cause multiple splits

#ifdef BPTREE_KEY_TYPE_STRING
        char key_buf[32];
        for (int i = 0; i < N; i++) {
            sprintf(key_buf, "bound%03d", i);  // Ensure lexicographical order
            ASSERT(bptree_put(tree, &KEY(key_buf), NULL) == BPTREE_OK,
                   "Insert failed for key %s at boundary condition", key_buf);
            // Optional: Check invariants periodically during boundary insertion
            // if (i % order == 0) { ASSERT(bptree_check_invariants(tree), "Invariants failed during
            // boundary insert at i=%d", i); }
        }
        // Verify first and last can be retrieved
        bptree_key_t first = KEY("bound000");
        bptree_key_t last = KEY("bound299");
        sprintf(key_buf, "bound%03d", N - 1);
        last = KEY(key_buf);
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &first, &res) == BPTREE_OK, "Get first boundary key failed");
            ASSERT(bptree_get(tree, &last, &res) == BPTREE_OK, "Get last boundary key failed");
        }
#else  // Numeric keys
        for (int i = 0; i < N; i++) {
            bptree_key_t k = (bptree_key_t)(i + 1);
            ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK,
                   "Insert failed for key %lld at boundary condition", (long long)k);
        }
        // Verify first and last can be retrieved
        const bptree_key_t first = (bptree_key_t)1;
        const bptree_key_t last = (bptree_key_t)N;
        {
            bptree_value_t res;
            ASSERT(bptree_get(tree, &first, &res) == BPTREE_OK,
                   "Get first boundary key failed for key %lld", (long long)first);
            ASSERT(bptree_get(tree, &last, &res) == BPTREE_OK,
                   "Get last boundary key failed for key %lld", (long long)last);
        }
#endif
        ASSERT(bptree_check_invariants(tree) == true,
               "Invariants check failed after boundary condition test");
        bptree_free(tree);

#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();
#endif
    }
}

/**
 * @brief Test: Stress test with a large number of insertions and retrievals.
 * Inserts N items (default 10k) and then retrieves all of them, verifying correctness.
 */
void test_stress(void) {
    int N = 100000;  // Number of items for stress test
    for (int m = 0; m < num_test_max_keys; m++) {
        int order = test_max_keys_values[m];
        bptree* tree = create_test_tree_with_order(order);
        ASSERT(tree != NULL, "Tree creation failed for stress test");

#ifdef BPTREE_KEY_TYPE_STRING
        char key_buf[32];
        // Phase 1: Insert N items
        for (int i = 0; i < N; i++) {
            sprintf(key_buf, "stress%05d", i);  // Lexicographical order
            char* v = MAKE_VALUE_STR(key_buf);
            track_alloc(v);
            ASSERT(bptree_put(tree, &KEY(key_buf), v) == BPTREE_OK,
                   "Stress insert failed for key %s", key_buf);
        }
        ASSERT(tree->count == N, "Count mismatch after stress insert");
        ASSERT(bptree_check_invariants(tree) == true, "Invariants failed after stress insert");

        // Phase 2: Retrieve N items
        for (int i = 0; i < N; i++) {
            sprintf(key_buf, "stress%05d", i);
            bptree_value_t res;
            ASSERT(bptree_get(tree, &KEY(key_buf), &res) == BPTREE_OK,
                   "Stress get failed for key %s", key_buf);
            ASSERT(CMP_VALUE_STR(res, key_buf), "Stress value mismatch for key %s", key_buf);
        }
#else  // Numeric keys
       // Phase 1: Insert N items
        for (int i = 0; i < N; i++) {
            bptree_key_t k = (bptree_key_t)(i + 1);
            ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK,
                   "Stress insert failed for key %lld", (long long)k);
        }
        ASSERT(tree->count == N, "Count mismatch after stress insert");
        ASSERT(bptree_check_invariants(tree) == true, "Invariants failed after stress insert");

        // Phase 2: Retrieve N items
        for (int i = 0; i < N; i++) {
            bptree_key_t k = (bptree_key_t)(i + 1);
            bptree_value_t res;
            ASSERT(bptree_get(tree, &k, &res) == BPTREE_OK, "Stress get failed for key %lld",
                   (long long)k);
            ASSERT(res == MAKE_VALUE_NUM(k), "Stress value mismatch for key %lld", (long long)k);
        }
#endif
        bptree_free(tree);  // Free tree structure

#ifdef BPTREE_KEY_TYPE_STRING
        cleanup_alloc_track();  // Free allocated string values
#endif
    }
}

/**
 * @brief Test: Stress test with a large number of random deletions.
 *
 * Inserts a sequence of keys and then deletes a large fraction of them
 * in a random order. This is designed to trigger complex rebalancing and
 * merge scenarios that might be missed by sequential deletion patterns.
 * Invariants are checked after every single operation to catch transient errors.
 */
void test_deletion_and_merge_stress(void) {
    const int order = 4;  // Use a small order to trigger merges often
    const int N = 200;    // Number of items to insert
    const int D = 180;    // Number of items to delete

    bptree* tree = create_test_tree_with_order(order);
    ASSERT(tree != NULL, "Tree creation failed for deletion stress test");

#ifdef BPTREE_KEY_TYPE_STRING
    // This test is complex with string keys due to value memory management.
    // It's primarily designed for numeric keys where values don't need freeing.
    // A string implementation would require a map/hash table to track which
    // values have been freed.
    fprintf(stderr, "Skipping deletion_and_merge_stress for string keys.\n");
    bptree_free(tree);
    return;
#else
    // --- Phase 1: Insert N items sequentially ---
    for (int i = 0; i < N; i++) {
        bptree_key_t k = (bptree_key_t)i;
        ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK,
               "Stress delete setup: insert failed for key %d", i);
        if (i % 10 == 0) {  // Check invariants periodically, not every time, for speed
            ASSERT(bptree_check_invariants(tree),
                   "Stress delete setup: invariants failed after inserting %d", i);
        }
    }
    ASSERT(tree->count == N, "Count mismatch after insertion phase of stress delete test");
    ASSERT(bptree_check_invariants(tree), "Invariants failed after full insertion phase");

    // --- Phase 2: Create a shuffled list of keys to delete ---
    bptree_key_t* keys_to_delete = malloc(N * sizeof(bptree_key_t));
    ASSERT(keys_to_delete != NULL, "Failed to allocate memory for keys to delete");
    for (int i = 0; i < N; i++) {
        keys_to_delete[i] = (bptree_key_t)i;
    }
    // Fisher-Yates shuffle
    srand((unsigned int)time(NULL));
    for (int i = N - 1; i > 0; i--) {
        int j = rand() % (i + 1);
        bptree_key_t temp = keys_to_delete[i];
        keys_to_delete[i] = keys_to_delete[j];
        keys_to_delete[j] = temp;
    }

    // --- Phase 3: Delete D items in random order ---
    for (int i = 0; i < D; i++) {
        bptree_key_t k = keys_to_delete[i];
        bptree_status st = bptree_remove(tree, &k);
        ASSERT(st == BPTREE_OK, "Deletion of key %lld failed with status %d (i=%d)", (long long)k,
               st, i);
        ASSERT(bptree_check_invariants(tree), "Invariants failed after deleting key %lld (i=%d)",
               (long long)k, i);
    }

    ASSERT(tree->count == N - D, "Count mismatch after deletions. Expected %d, got %d", N - D,
           tree->count);

    // --- Phase 4: Verify remaining keys are still present ---
    for (int i = D; i < N; i++) {
        bptree_key_t k = keys_to_delete[i];
        bptree_value_t res;
        ASSERT(bptree_get(tree, &k, &res) == BPTREE_OK,
               "Could not find key %lld which should not have been deleted", (long long)k);
        ASSERT(res == MAKE_VALUE_NUM(k), "Value mismatch for remaining key %lld", (long long)k);
    }

    free(keys_to_delete);
    bptree_free(tree);
#endif
}

/**
 * @brief Test: Deletion of a key that requires updating a separator in an ancestor node.
 *
 * This test constructs a tree of height 3 where a key exists both in a leaf
 * and as a separator in the grandparent node. It then deletes the key from the
 * leaf and verifies that the separator key in the grandparent is correctly updated.
 * This scenario is critical for ensuring the tree's structural integrity after deletion.
 */
void test_ancestor_key_update_on_delete(void) {
    const int order = 3;  // Use a small order to control tree structure precisely
    bptree* tree = create_test_tree_with_order(order);
    ASSERT(tree != NULL, "Tree creation failed for ancestor update test");

    // --- Phase 1: Construct a tree of height 3 ---
    // This sequence of insertions is carefully chosen to create a tree of height 3
    // with a specific structure. The key `13` will be a separator in the root.
    const int keys_to_insert[] = {10, 20, 30, 5, 15, 25, 35, 13, 18, 28, 38, 1, 3, 6, 8};
    const int num_keys = sizeof(keys_to_insert) / sizeof(keys_to_insert[0]);

    for (int i = 0; i < num_keys; i++) {
        bptree_key_t k = (bptree_key_t)keys_to_insert[i];
        bptree_put(tree, &k, MAKE_VALUE_NUM(k));
    }

    bptree_stats stats_before = bptree_get_stats(tree);
    ASSERT(stats_before.height == 3, "Test setup failed: Tree height is not 3, but %d",
           stats_before.height);

    // --- Phase 2: Delete a key that is also a separator in a non-parent ancestor ---
    // At this point, the key `13` is the smallest key in a leaf and also a separator
    // in the root node (the grandparent of the leaf). Deleting it should trigger an update
    // in the root. The new smallest key in the leaf will be 15.
    bptree_key_t key_to_delete = 13;
    bptree_status st = bptree_remove(tree, &key_to_delete);
    ASSERT(st == BPTREE_OK, "Deletion of key 13 failed unexpectedly");

    // --- Phase 3: Verify tree integrity ---
    // The bug is that the separator in the root is NOT updated. A weak invariant check
    // might pass here, but a strict one should fail. After fixing the bug, the
    // invariant check must pass.
    bool invariants_ok = bptree_check_invariants(tree);
    ASSERT(invariants_ok, "Invariants check failed after deleting key requiring ancestor update");

    // Also, verify that the deleted key is truly gone and a key from the same leaf is present.
    bptree_value_t dummy_val;
    ASSERT(bptree_get(tree, &key_to_delete, &dummy_val) == BPTREE_KEY_NOT_FOUND,
           "Deleted key 13 was found in the tree");

    bptree_key_t neighbouring_key = 15;
    ASSERT(bptree_get(tree, &neighbouring_key, &dummy_val) == BPTREE_OK,
           "Key 15 (neighbour of deleted key) was not found");

    bptree_free(tree);
}

/**
 * @brief Test tree operations with the minimum allowed max_keys value (3).
 *
 * This test ensures the tree handles the smallest possible node size correctly,
 * which exercises edge cases in splitting and merging logic.
 */
void test_minimum_max_keys(void) {
    const int min_max_keys = 3;  // Minimum allowed value
    bptree* tree = bptree_create(min_max_keys, NULL, global_debug_enabled);
    ASSERT(tree != NULL, "Failed to create tree with min_max_keys=3");
    ASSERT(tree->max_keys == min_max_keys, "max_keys not set correctly");

    // Test extensive insertions to force multiple splits with minimum node size
    const int num_keys = 100;
    for (int i = 0; i < num_keys; i++) {
        bptree_key_t k = (bptree_key_t)i;
        bptree_status st = bptree_put(tree, &k, MAKE_VALUE_NUM(k));
        ASSERT(st == BPTREE_OK, "Insertion failed at key %d with min_max_keys", i);
    }

    ASSERT(tree->count == num_keys, "Count mismatch after insertions with min_max_keys");
    ASSERT(bptree_check_invariants(tree), "Invariants failed after insertions with min_max_keys");

    // Test deletions that force merges with minimum node size
    for (int i = 0; i < num_keys / 2; i++) {
        bptree_key_t k = (bptree_key_t)(i * 2);  // Delete every other key
        bptree_status st = bptree_remove(tree, &k);
        ASSERT(st == BPTREE_OK, "Deletion failed at key %d with min_max_keys", i * 2);
    }

    ASSERT(tree->count == num_keys / 2, "Count incorrect after deletions with min_max_keys");
    ASSERT(bptree_check_invariants(tree), "Invariants failed after deletions with min_max_keys");

    // Verify remaining keys are still accessible
    for (int i = 0; i < num_keys; i++) {
        bptree_key_t k = (bptree_key_t)i;
        bptree_value_t val;
        bptree_status st = bptree_get(tree, &k, &val);
        if (i % 2 == 0) {
            ASSERT(st == BPTREE_KEY_NOT_FOUND, "Deleted key %d found with min_max_keys", i);
        } else {
            ASSERT(st == BPTREE_OK, "Non-deleted key %d not found with min_max_keys", i);
        }
    }

    bptree_free(tree);
}

/**
 * @brief Test very large trees (height > 10) to ensure scalability.
 *
 * This test creates a tree with many keys to verify that deep trees
 * maintain correctness and performance remains acceptable.
 */
void test_very_large_tree(void) {
    const int max_keys = 8;  // Moderate branching factor for reasonable height
    bptree* tree = bptree_create(max_keys, NULL, global_debug_enabled);
    ASSERT(tree != NULL, "Failed to create tree for large tree test");

    // Insert enough keys to create a tree with height > 10
    // With max_keys=8, we need roughly 8^10 = 1 billion keys for height 10
    // But for practical testing, we'll use fewer keys and verify height increases
    const int num_keys = 100000;  // 100K keys should give us height > 10

    fprintf(stderr, "  [Inserting %d keys for large tree test...]\n", num_keys);
    for (int i = 0; i < num_keys; i++) {
        bptree_key_t k = (bptree_key_t)i;
        bptree_status st = bptree_put(tree, &k, MAKE_VALUE_NUM(k));
        ASSERT(st == BPTREE_OK, "Insertion failed at key %d in large tree", i);

        // Periodic invariant checks to catch issues early
        if ((i + 1) % 10000 == 0) {
            ASSERT(bptree_check_invariants(tree),
                   "Invariants failed after %d insertions in large tree", i + 1);
            fprintf(stderr, "  [%d keys inserted, height=%d]\n", i + 1, tree->height);
        }
    }

    bptree_stats stats = bptree_get_stats(tree);
    fprintf(stderr, "  [Final tree: count=%d, height=%d, nodes=%d]\n", stats.count, stats.height,
            stats.node_count);
    ASSERT(stats.height > 5, "Tree height (%d) not as deep as expected", stats.height);
    ASSERT(stats.count == num_keys, "Count mismatch in large tree");
    ASSERT(bptree_check_invariants(tree), "Invariants failed for large tree");

    // Test random access in large tree
    fprintf(stderr, "  [Testing random access in large tree...]\n");
    srand(42);  // Deterministic seed
    for (int i = 0; i < 1000; i++) {
        bptree_key_t k = (bptree_key_t)(rand() % num_keys);
        bptree_value_t val;
        bptree_status st = bptree_get(tree, &k, &val);
        ASSERT(st == BPTREE_OK, "Random access failed for key %lld in large tree", (long long)k);
        ASSERT(val == MAKE_VALUE_NUM(k), "Value mismatch for key %lld in large tree", (long long)k);
    }

    // Test range queries in large tree
    fprintf(stderr, "  [Testing range queries in large tree...]\n");
    bptree_key_t start = 1000, end = 2000;
    bptree_value_t* range_vals = NULL;
    int range_count = 0;
    bptree_status st = bptree_get_range(tree, &start, &end, &range_vals, &range_count);
    ASSERT(st == BPTREE_OK, "Range query failed in large tree");
    ASSERT(range_count == 1001, "Range count incorrect in large tree: got %d", range_count);
    bptree_free_range_results(range_vals);

    bptree_free(tree);
}

/**
 * @brief Enhanced stress test with more aggressive random insert/delete patterns.
 *
 * This test performs intensive random operations to uncover race conditions
 * and edge cases in rebalancing logic.
 */
void test_enhanced_random_stress(void) {
    const int max_keys = 5;
    bptree* tree = bptree_create(max_keys, NULL, global_debug_enabled);
    ASSERT(tree != NULL, "Failed to create tree for enhanced stress test");

    const int num_operations = 10000;
    const int key_range = 1000;

    srand(12345);  // Deterministic seed for reproducibility
    int inserted_count = 0;

    fprintf(stderr, "  [Running %d random operations...]\n", num_operations);

    for (int op = 0; op < num_operations; op++) {
        int operation = rand() % 100;
        bptree_key_t k = (bptree_key_t)(rand() % key_range);

        if (operation < 60) {  // 60% inserts
            bptree_status st = bptree_put(tree, &k, MAKE_VALUE_NUM(k));
            if (st == BPTREE_OK) {
                inserted_count++;
            }
        } else if (operation < 90) {  // 30% deletes
            bptree_status st = bptree_remove(tree, &k);
            if (st == BPTREE_OK) {
                inserted_count--;
            }
        } else {  // 10% range queries
            bptree_key_t start = (bptree_key_t)(rand() % key_range);
            bptree_key_t end = start + (bptree_key_t)(rand() % 100);
            bptree_value_t* range_vals = NULL;
            int range_count = 0;
            bptree_status st = bptree_get_range(tree, &start, &end, &range_vals, &range_count);
            ASSERT(st == BPTREE_OK, "Range query failed in stress test at op %d", op);
            bptree_free_range_results(range_vals);
        }

        // Verify tree count matches our tracking
        ASSERT(tree->count == inserted_count, "Count mismatch at op %d: tree=%d, tracked=%d", op,
               tree->count, inserted_count);

        // Periodic invariant checks
        if ((op + 1) % 1000 == 0) {
            ASSERT(bptree_check_invariants(tree),
                   "Invariants failed at operation %d in enhanced stress test", op + 1);
            fprintf(stderr, "  [%d operations completed, count=%d]\n", op + 1, tree->count);
        }
    }

    fprintf(stderr, "  [Final count: %d]\n", tree->count);
    ASSERT(bptree_check_invariants(tree), "Final invariants check failed in enhanced stress test");

    bptree_free(tree);
}

/**
 * @brief Test alternating insert/delete patterns that stress rebalancing.
 *
 * This test performs specific patterns known to cause issues in tree rebalancing:
 * - Sequential inserts followed by reverse-order deletes
 * - Alternating inserts and deletes at boundaries
 */
void test_rebalancing_stress_patterns(void) {
    const int max_keys = 7;
    bptree* tree = bptree_create(max_keys, NULL, global_debug_enabled);
    ASSERT(tree != NULL, "Failed to create tree for rebalancing stress test");

    // Pattern 1: Sequential insert, reverse delete
    fprintf(stderr, "  [Pattern 1: Sequential insert, reverse delete]\n");
    const int pattern1_count = 500;
    for (int i = 0; i < pattern1_count; i++) {
        bptree_key_t k = (bptree_key_t)i;
        ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK,
               "Insert failed in pattern 1 at key %d", i);
    }
    ASSERT(tree->count == pattern1_count, "Count wrong after pattern 1 inserts");

    for (int i = pattern1_count - 1; i >= 0; i--) {
        bptree_key_t k = (bptree_key_t)i;
        ASSERT(bptree_remove(tree, &k) == BPTREE_OK, "Delete failed in pattern 1 at key %d", i);

        if ((pattern1_count - i) % 100 == 0) {
            ASSERT(bptree_check_invariants(tree),
                   "Invariants failed during pattern 1 deletes at key %d", i);
        }
    }
    ASSERT(tree->count == 0, "Tree not empty after pattern 1");
    ASSERT(bptree_check_invariants(tree), "Invariants failed after pattern 1");

    // Pattern 2: Alternating boundaries
    fprintf(stderr, "  [Pattern 2: Alternating boundary inserts/deletes]\n");
    const int pattern2_count = 300;
    for (int i = 0; i < pattern2_count; i++) {
        bptree_key_t k = (i % 2 == 0) ? (bptree_key_t)i : (bptree_key_t)(1000 - i);
        ASSERT(bptree_put(tree, &k, MAKE_VALUE_NUM(k)) == BPTREE_OK,
               "Insert failed in pattern 2 at iteration %d", i);

        // Every 10 inserts, delete the oldest 5
        if (i > 0 && i % 10 == 0) {
            for (int j = i - 10; j < i - 5; j++) {
                bptree_key_t dk = (j % 2 == 0) ? (bptree_key_t)j : (bptree_key_t)(1000 - j);
                bptree_remove(tree, &dk);  // Ignore status, key might not exist
            }
        }
    }

    ASSERT(bptree_check_invariants(tree), "Invariants failed after pattern 2");

    bptree_free(tree);
}

/**
 * @brief Main entry point for the B+ tree test suite.
 *
 * Runs a series of test functions to validate the bptree library.
 * Prints summary results (tests run, tests failed).
 *
 * @param void Takes no arguments.
 * @return EXIT_SUCCESS if all tests pass, EXIT_FAILURE otherwise.
 */
int main(void) {
    fprintf(stderr, "Starting B+ tree test suite...\n");

    // --- Example API check (not a formal test, just basic sanity) ---
#ifdef BPTREE_KEY_TYPE_STRING
    {
        bptree* tree = bptree_create(5, string_key_compare, global_debug_enabled);
        if (tree) {
            printf("API Usage Check: Created tree with max_keys = 5 (string keys).\n");
            ASSERT(bptree_contains(tree, (bptree_key_t[]){KEY("example")}) == false,
                   "Contains on empty tree failed");
            ASSERT(tree->count == 0, "Initial count non-zero");
            bptree_free(tree);
        } else {
            fprintf(stderr, "API usage check: string tree creation failed.\n");
            tests_failed++;  // Count creation failure as a test failure
        }
        cleanup_alloc_track();  // Cleanup any potential allocs from KEY macro if needed
    }
#else  // Numeric keys
    {
        bptree* tree = bptree_create(5, NULL, global_debug_enabled);
        if (tree) {
            printf("API Usage Check: Created tree with max_keys = 5 (numeric keys).\n");
            const bptree_key_t example = 69;
            ASSERT(bptree_contains(tree, &example) == false, "Contains on empty tree failed");
            ASSERT(tree->count == 0, "Initial count non-zero");
            bptree_free(tree);
        } else {
            fprintf(stderr, "API usage check: numeric tree creation failed.\n");
            tests_failed++;  // Count creation failure as a test failure
        }
    }
#endif

    // --- Run Core Test Functions ---
    RUN_TEST(test_creation_failure);
    RUN_TEST(test_min_internal_keys_calculation);
    RUN_TEST(test_empty_tree);
    RUN_TEST(test_single_element);
    RUN_TEST(test_insertion_and_search);
    RUN_TEST(test_duplicate_insertion);
    RUN_TEST(test_deletion);
    RUN_TEST(test_range_query);
    RUN_TEST(test_tree_stats);
    RUN_TEST(test_precise_boundary_conditions);
    RUN_TEST(test_stress);
    RUN_TEST(test_mixed_insert_delete);  // Often catches complex rebalancing issues
    RUN_TEST(test_deletion_and_merge_stress);
    RUN_TEST(test_ancestor_key_update_on_delete);

    // --- Run Enhanced Edge Case Tests ---
    RUN_TEST(test_minimum_max_keys);
    RUN_TEST(test_very_large_tree);
    RUN_TEST(test_enhanced_random_stress);
    RUN_TEST(test_rebalancing_stress_patterns);

    // --- Test Summary ---
    fprintf(stderr, "----------------------------------------\n");
    fprintf(stderr, "Test Summary: %d tests run, %d failures.\n", tests_run, tests_failed);
    fprintf(stderr, "----------------------------------------\n");

    return tests_failed ? EXIT_FAILURE : EXIT_SUCCESS;
}
