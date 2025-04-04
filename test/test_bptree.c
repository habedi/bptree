/**
 * @file test_bptree.c
 * @brief Unit tests for the B+Tree library.
 *
 * This file implements a series of tests to verify the behavior of the
 * B+Tree library. It tests insertion, deletion, search, range queries,
 * bulk loading, iteration, and tree statistics.
 */

#define BPTREE_IMPLEMENTATION
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bptree.h"

/**
 * @brief Global flag for enabling/disabling debug logging.
 */
const bool debug_enabled = false;

/**
 * @brief Comparison function for strings.
 *
 * This function compares two strings using strcmp.
 *
 * @param a Pointer to the first string.
 * @param b Pointer to the second string.
 * @param udata Unused user data.
 * @return Negative value if a < b, zero if a equals b, positive value if a > b.
 */
int str_compare(const void *a, const void *b, const void *udata) {
    (void)udata;
    return strcmp(a, b);
}

/**
 * @brief Tests insertion and search functionality.
 *
 * This test creates a B+Tree, inserts three string items, verifies that
 * searching for an inserted key returns the correct value, and that a non-existent
 * key returns NULL.
 */
void test_insertion_and_search() {
    printf("Test insertion and search...\n");
    bptree *tree = bptree_new(5, str_compare, NULL, NULL, NULL, debug_enabled);
    assert(tree != NULL);
    char *s1 = "apple", *s2 = "banana", *s3 = "cherry";
    assert(bptree_put(tree, s1) == BPTREE_OK);
    assert(bptree_put(tree, s2) == BPTREE_OK);
    assert(bptree_put(tree, s3) == BPTREE_OK);
    const char *res = bptree_get(tree, "banana");
    assert(res && strcmp(res, "banana") == 0);
    res = bptree_get(tree, "durian");
    assert(res == NULL);
    bptree_free(tree);
    printf("Insertion and search passed.\n");
}

/**
 * @brief Tests deletion functionality.
 *
 * This test verifies that keys can be correctly removed from the tree.
 * It checks that a removed key cannot be retrieved and that attempting to remove
 * a non-existent key returns the proper status.
 */
void test_deletion() {
    printf("Test deletion...\n");
    bptree *tree = bptree_new(5, str_compare, NULL, NULL, NULL, debug_enabled);
    assert(tree != NULL);
    char *arr[] = {"alpha", "beta", "gamma", "delta", "epsilon"};
    for (size_t i = 0; i < 5; i++) {
        assert(bptree_put(tree, arr[i]) == BPTREE_OK);
    }
    assert(bptree_remove(tree, "gamma") == BPTREE_OK);
    assert(bptree_get(tree, "gamma") == NULL);
    assert(bptree_remove(tree, "zeta") == BPTREE_NOT_FOUND);
    bptree_free(tree);
    printf("Deletion passed.\n");
}

/**
 * @brief Tests operations on an empty B+Tree.
 *
 * This test creates an empty B+Tree and confirms that searching for any key
 * returns NULL and that removal attempts yield the BPTREE_NOT_FOUND status.
 */
void test_empty_tree() {
    printf("Test operations on an empty tree...\n");
    bptree *tree = bptree_new(5, str_compare, NULL, NULL, NULL, debug_enabled);
    assert(tree != NULL);
    assert(bptree_get(tree, "anything") == NULL);
    assert(bptree_remove(tree, "anything") == BPTREE_NOT_FOUND);
    bptree_free(tree);
    printf("Empty tree operations passed.\n");
}

/**
 * @brief Tests handling of duplicate insertions.
 *
 * This test verifies that attempting to insert a duplicate key results in the
 * BPTREE_DUPLICATE status and that the tree still returns the original key.
 */
void test_duplicate_insertion() {
    printf("Test duplicate insertion...\n");
    bptree *tree = bptree_new(5, str_compare, NULL, NULL, NULL, debug_enabled);
    assert(tree != NULL);
    char *dup = "duplicate";
    assert(bptree_put(tree, dup) == BPTREE_OK);
    assert(bptree_put(tree, dup) == BPTREE_DUPLICATE);
    const char *res = bptree_get(tree, dup);
    assert(res && strcmp(res, dup) == 0);
    bptree_free(tree);
    printf("Duplicate insertion passed.\n");
}

/**
 * @brief Tests B+Tree functionality with a single element.
 *
 * This test inserts a single element into the tree, verifies that it can be
 * retrieved, then removes it and confirms that it is no longer present.
 */
void test_single_element() {
    printf("Test single element tree...\n");
    bptree *tree = bptree_new(5, str_compare, NULL, NULL, NULL, debug_enabled);
    assert(tree != NULL);
    char *solo = "solo";
    assert(bptree_put(tree, solo) == BPTREE_OK);
    const char *res = bptree_get(tree, solo);
    assert(res && strcmp(res, solo) == 0);
    assert(bptree_remove(tree, solo) == BPTREE_OK);
    assert(bptree_get(tree, solo) == NULL);
    bptree_free(tree);
    printf("Single element tree passed.\n");
}

/**
 * @brief Tests insertion and retrieval using long string keys.
 *
 * This test creates two long strings, inserts them into the tree, and checks
 * that they are correctly retrievable. It also tests deletion of one long string.
 */
void test_long_string_keys() {
    printf("Test long string keys...\n");
    bptree *tree = bptree_new(5, str_compare, NULL, NULL, NULL, debug_enabled);
    assert(tree != NULL);
    char long1[1024], long2[1024];
    memset(long1, 'a', sizeof(long1) - 1);
    long1[sizeof(long1) - 1] = '\0';
    memset(long2, 'b', sizeof(long2) - 1);
    long2[sizeof(long2) - 1] = '\0';
    assert(bptree_put(tree, long1) == BPTREE_OK);
    assert(bptree_put(tree, long2) == BPTREE_OK);
    const char *res = bptree_get(tree, long1);
    assert(res && strcmp(res, long1) == 0);
    res = bptree_get(tree, long2);
    assert(res && strcmp(res, long2) == 0);
    assert(bptree_remove(tree, long1) == BPTREE_OK);
    assert(bptree_get(tree, long1) == NULL);
    bptree_free(tree);
    printf("Long string keys passed.\n");
}

/**
 * @brief Tests mixed operations including insertion, deletion, and retrieval.
 *
 * This test performs a series of mixed operations on the B+Tree. It first inserts a set of keys,
 * deletes some, then inserts additional keys, and finally verifies that all operations produce
 * the expected results.
 */
void test_mixed_operations() {
    printf("Test mixed operations...\n");
    bptree *tree = bptree_new(5, str_compare, NULL, NULL, NULL, debug_enabled);
    assert(tree != NULL);
    char *keys1[] = {"one", "two", "three", "four", "five"};
    const size_t count1 = sizeof(keys1) / sizeof(keys1[0]);
    for (size_t i = 0; i < count1; i++) {
        assert(bptree_put(tree, keys1[i]) == BPTREE_OK);
    }
    assert(bptree_remove(tree, "three") == BPTREE_OK);
    assert(bptree_remove(tree, "five") == BPTREE_OK);
    assert(bptree_get(tree, "three") == NULL);
    assert(bptree_get(tree, "five") == NULL);
    char *keys2[] = {"six", "seven"};
    const size_t count2 = sizeof(keys2) / sizeof(keys2[0]);
    for (size_t i = 0; i < count2; i++) {
        assert(bptree_put(tree, keys2[i]) == BPTREE_OK);
    }
    assert(bptree_put(tree, "three") == BPTREE_OK);
    const char *res = bptree_get(tree, "two");
    assert(res && strcmp(res, "two") == 0);
    res = bptree_get(tree, "seven");
    assert(res && strcmp(res, "seven") == 0);
    res = bptree_get(tree, "three");
    assert(res && strcmp(res, "three") == 0);
    bptree_free(tree);
    printf("Mixed operations passed.\n");
}

/**
 * @brief Tests repeated deletion of non-existent keys.
 *
 * This test inserts a few keys and then attempts to remove keys that are not present,
 * verifying that the proper not-found status is returned.
 */
void test_repeated_nonexistent_deletion() {
    printf("Test repeated deletion of non-existent keys...\n");
    bptree *tree = bptree_new(5, str_compare, NULL, NULL, NULL, debug_enabled);
    assert(tree != NULL);
    assert(bptree_put(tree, "alpha") == BPTREE_OK);
    assert(bptree_put(tree, "beta") == BPTREE_OK);
    assert(bptree_remove(tree, "gamma") == BPTREE_NOT_FOUND);
    assert(bptree_remove(tree, "delta") == BPTREE_NOT_FOUND);
    bptree_free(tree);
    printf("Repeated non-existent deletion passed.\n");
}

/**
 * @brief Tests insertion, retrieval, and deletion of an empty string key.
 *
 * This test verifies that an empty string can be used as a key by the B+Tree.
 */
void test_empty_string_key() {
    printf("Test empty string key...\n");
    bptree *tree = bptree_new(5, str_compare, NULL, NULL, NULL, debug_enabled);
    assert(tree != NULL);
    char *empty = "";
    assert(bptree_put(tree, empty) == BPTREE_OK);
    const char *res = bptree_get(tree, empty);
    assert(res && strcmp(res, empty) == 0);
    assert(bptree_remove(tree, empty) == BPTREE_OK);
    assert(bptree_get(tree, empty) == NULL);
    bptree_free(tree);
    printf("Empty string key passed.\n");
}

/**
 * @brief Tests reinsertion of a key after it has been deleted.
 *
 * This test verifies that a key can be inserted again after being removed.
 */
void test_reinsertion_after_deletion() {
    printf("Test reinsertion after deletion...\n");
    bptree *tree = bptree_new(5, str_compare, NULL, NULL, NULL, debug_enabled);
    assert(tree != NULL);
    char *key = "reinsertion";
    assert(bptree_put(tree, key) == BPTREE_OK);
    assert(bptree_remove(tree, key) == BPTREE_OK);
    assert(bptree_put(tree, key) == BPTREE_OK);
    const char *res = bptree_get(tree, key);
    assert(res && strcmp(res, key) == 0);
    bptree_free(tree);
    printf("Reinsertion after deletion passed.\n");
}

/**
 * @brief Tests basic range search functionality.
 *
 * This test inserts several keys and then performs a range search from "banana" to "fig",
 * verifying that the correct keys are returned.
 */
void test_range_search_basic() {
    printf("Test range search (basic)...\n");
    bptree *tree = bptree_new(5, str_compare, NULL, NULL, NULL, debug_enabled);
    assert(tree != NULL);
    char *keys[] = {"apple", "banana", "cherry", "date", "fig", "grape"};
    const size_t n = sizeof(keys) / sizeof(keys[0]);
    for (size_t i = 0; i < n; i++) {
        assert(bptree_put(tree, keys[i]) == BPTREE_OK);
    }
    int count = 0;
    void **range = bptree_get_range(tree, "banana", "fig", &count);
    assert(count == 4);
    assert(strcmp(range[0], "banana") == 0);
    assert(strcmp(range[1], "cherry") == 0);
    assert(strcmp(range[2], "date") == 0);
    assert(strcmp(range[3], "fig") == 0);
    tree->free_fn(range);
    bptree_free(tree);
    printf("Basic range search passed.\n");
}

/**
 * @brief Tests range search when no keys fall within the specified range.
 *
 * This test ensures that a range search returns an empty result when the range
 * does not match any keys in the tree.
 */
void test_range_search_empty() {
    printf("Test range search (empty range)...\n");
    bptree *tree = bptree_new(5, str_compare, NULL, NULL, NULL, debug_enabled);
    assert(tree != NULL);
    char *keys[] = {"apple", "banana", "cherry"};
    const size_t n = sizeof(keys) / sizeof(keys[0]);
    for (size_t i = 0; i < n; i++) {
        assert(bptree_put(tree, keys[i]) == BPTREE_OK);
    }
    int count = 0;
    void **range = bptree_get_range(tree, "date", "fig", &count);
    assert(count == 0);
    tree->free_fn(range);
    bptree_free(tree);
    printf("Empty range search passed.\n");
}

/**
 * @brief Tests range search over the full set of keys.
 *
 * This test inserts several keys and performs a range search covering the entire range,
 * verifying that all keys are returned.
 */
void test_range_search_full() {
    printf("Test range search (full range)...\n");
    bptree *tree = bptree_new(5, str_compare, NULL, NULL, NULL, debug_enabled);
    assert(tree != NULL);
    char *keys[] = {"apple", "banana", "cherry", "date", "fig", "grape"};
    const size_t n = sizeof(keys) / sizeof(keys[0]);
    for (size_t i = 0; i < n; i++) {
        assert(bptree_put(tree, keys[i]) == BPTREE_OK);
    }
    int count = 0;
    void **range = bptree_get_range(tree, "apple", "grape", &count);
    assert(count == 6);
    tree->free_fn(range);
    bptree_free(tree);
    printf("Full range search passed.\n");
}

/**
 * @brief Tests range search with boundary conditions.
 *
 * This test verifies that range searches correctly handle cases where the
 * lower and upper boundaries match keys in the tree.
 */
void test_range_search_boundaries() {
    printf("Test range search (boundary conditions)...\n");
    bptree *tree = bptree_new(5, str_compare, NULL, NULL, NULL, debug_enabled);
    assert(tree != NULL);
    char *keys[] = {"apple", "banana", "cherry", "date", "fig", "grape"};
    const size_t n = sizeof(keys) / sizeof(keys[0]);
    for (size_t i = 0; i < n; i++) {
        assert(bptree_put(tree, keys[i]) == BPTREE_OK);
    }
    int count = 0;
    void **range = bptree_get_range(tree, "cherry", "cherry", &count);
    assert(count == 1);
    assert(strcmp(range[0], "cherry") == 0);
    tree->free_fn(range);
    count = 0;
    range = bptree_get_range(tree, "aardvark", "blueberry", &count);
    assert(count == 2);
    assert(strcmp(range[0], "apple") == 0);
    assert(strcmp(range[1], "banana") == 0);
    tree->free_fn(range);
    bptree_free(tree);
    printf("Boundary range search passed.\n");
}

/**
 * @brief Tests the bulk loading of a sorted array of keys.
 *
 * This test allocates an array of keys, bulk loads them into the B+Tree, and
 * then verifies that each key can be retrieved.
 */
void test_bulk_load_sorted() {
    printf("Test bulk load (sorted input)...\n");
    int N = 100;
    char **keys = malloc(N * sizeof(char *));
    for (int i = 0; i < N; i++) {
        keys[i] = malloc(16);
        sprintf(keys[i], "key%03d", i);
    }
    bptree *tree =
        bptree_bulk_load(5, str_compare, NULL, NULL, NULL, debug_enabled, (void **)keys, N);
    assert(tree != NULL);
    for (int i = 0; i < N; i++) {
        const char *res = bptree_get(tree, keys[i]);
        assert(res != NULL && strcmp(res, keys[i]) == 0);
    }
    bptree_free(tree);
    for (int i = 0; i < N; i++) {
        free(keys[i]);
    }
    free(keys);
    printf("Bulk load (sorted) passed.\n");
}

/**
 * @brief Tests bulk loading with an empty array.
 *
 * This test verifies that attempting to bulk load an empty array returns NULL.
 */
void test_bulk_load_empty() {
    printf("Test bulk load (empty array)...\n");
    bptree *tree = bptree_bulk_load(5, str_compare, NULL, NULL, NULL, debug_enabled, NULL, 0);
    assert(tree == NULL);
    printf("Bulk load (empty array) passed.\n");
}

/**
 * @brief Tests the iterator functionality of the B+Tree.
 *
 * This test inserts several keys, creates an iterator, and then iterates through
 * the tree, verifying that the number of iterated elements matches the tree's count.
 */
void test_iterator() {
    printf("Test iterator...\n");
    bptree *tree = bptree_new(5, str_compare, NULL, NULL, NULL, debug_enabled);
    assert(tree != NULL);
    char *keys[] = {"ant", "bee", "cat", "dog", "eel", "fox"};
    const size_t n = sizeof(keys) / sizeof(keys[0]);
    for (size_t i = 0; i < n; i++) {
        assert(bptree_put(tree, keys[i]) == BPTREE_OK);
    }
    bptree_iterator *iter = bptree_iterator_new(tree);
    assert(iter != NULL);
    int count = 0;
    while (bptree_iterator_next(iter) != NULL) {
        count++;
    }
    assert(count == tree->count);
    bptree_iterator_free(iter, tree->free_fn);
    bptree_free(tree);
    printf("Iterator passed.\n");
}

/**
 * @brief Tests retrieval of B+Tree statistics.
 *
 * This test inserts several keys into the tree and then verifies that the
 * statistics (item count, tree height, and node count) are correctly reported.
 */
void test_tree_stats() {
    printf("Test tree stats...\n");
    bptree *tree = bptree_new(5, str_compare, NULL, NULL, NULL, debug_enabled);
    assert(tree != NULL);
    bptree_stats stats = bptree_get_stats(tree);
    assert(stats.count == 0);
    char *keys[] = {"a", "b", "c", "d", "e", "f", "g"};
    const size_t n = sizeof(keys) / sizeof(keys[0]);
    for (size_t i = 0; i < n; i++) {
        assert(bptree_put(tree, keys[i]) == BPTREE_OK);
    }
    stats = bptree_get_stats(tree);
    assert((size_t)stats.count == n);
    assert(stats.height > 0);
    assert(stats.node_count > 0);
    bptree_free(tree);
    printf("Tree stats passed.\n");
}

/**
 * @brief Main function to run all B+Tree tests.
 *
 * This function calls each test function in sequence and prints a success
 * message if all tests pass.
 *
 * @return Exit status.
 */
int main(void) {
    // Run all tests
    test_insertion_and_search();
    test_deletion();
    test_empty_tree();
    test_duplicate_insertion();
    test_single_element();
    test_long_string_keys();
    test_mixed_operations();
    test_repeated_nonexistent_deletion();
    test_empty_string_key();
    test_reinsertion_after_deletion();
    test_range_search_basic();
    test_range_search_empty();
    test_range_search_full();
    test_range_search_boundaries();
    test_bulk_load_sorted();
    test_bulk_load_empty();
    test_iterator();
    test_tree_stats();
    printf("All tests passed.\n");
    return 0;
}
