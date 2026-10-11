#include <stdio.h>
#include <stdlib.h>

typedef struct {
    int fields[3];
#ifdef BPTREE_TEST_WIDE_VALUE
    long double wide;
#endif
} test_value_t;

static int fail_after = -1;
static int allocation_calls;

static void* test_aligned_alloc(size_t alignment, size_t size) {
    allocation_calls++;
    if (fail_after == 0) return NULL;
    if (fail_after > 0) fail_after--;
    return aligned_alloc(alignment, size);
}

#define aligned_alloc test_aligned_alloc
#define BPTREE_VALUE_TYPE test_value_t
#define BPTREE_IMPLEMENTATION
#include "bptree.h"
#undef aligned_alloc

#define ASSERT(condition)                                                   \
    do {                                                                    \
        if (!(condition)) {                                                 \
            fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition); \
            abort();                                                        \
        }                                                                   \
    } while (0)

static bptree_key_t make_key(int n) {
#ifdef BPTREE_KEY_TYPE_STRING
    bptree_key_t key = {0};
    snprintf(key.data, sizeof(key.data), "%04d", n);
    return key;
#else
    return n;
#endif
}

static test_value_t make_value(int n) {
    test_value_t value = {0};
    value.fields[0] = n;
    value.fields[1] = n + 1;
    value.fields[2] = n + 2;
    return value;
}

static void check_entries(bptree* tree, int count) {
    ASSERT(bptree_count(tree) == count);
    ASSERT(bptree_check_invariants(tree));
    bptree_iter it = bptree_iter_begin(tree);
    for (int n = 0; n < count; ++n) {
        bptree_key_t key = make_key(n);
        test_value_t value;
        ASSERT(bptree_get(tree, &key, &value) == BPTREE_OK);
        ASSERT(value.fields[0] == n && value.fields[1] == n + 1 && value.fields[2] == n + 2);
        ASSERT(bptree_iter_valid(&it));
        bptree_key_t iter_key = bptree_iter_key(&it);
        ASSERT(tree->compare(&key, &iter_key) == 0);
        ASSERT(bptree_iter_value(&it).fields[0] == n);
        bptree_iter_next(&it);
    }
    ASSERT(!bptree_iter_valid(&it));
}

static void test_split_failures(int order) {
    for (int count = 0; count < 80; ++count) {
        for (int failure = 0; failure < 8; ++failure) {
            fail_after = -1;
            bptree* tree = bptree_create(order, NULL, false);
            ASSERT(tree);
            for (int n = 0; n < count; ++n) {
                bptree_key_t key = make_key(n);
                ASSERT(bptree_put(tree, &key, make_value(n)) == BPTREE_OK);
            }
            bptree_key_t key = make_key(count);
            fail_after = failure;
            bptree_status status = bptree_put(tree, &key, make_value(count));
            fail_after = -1;
            ASSERT(status == BPTREE_OK || status == BPTREE_ALLOCATION_FAILURE);
            check_entries(tree, count + (status == BPTREE_OK));
            if (status == BPTREE_ALLOCATION_FAILURE) {
                ASSERT(!bptree_contains(tree, &key));
                ASSERT(bptree_put(tree, &key, make_value(count)) == BPTREE_OK);
                check_entries(tree, count + 1);
            }
            fail_after = 0;
            ASSERT(bptree_put(tree, &key, make_value(count)) == BPTREE_DUPLICATE_KEY);
            int calls_before_clear = allocation_calls;
            bptree_clear(tree);
            ASSERT(allocation_calls == calls_before_clear);
            check_entries(tree, 0);
            ASSERT(bptree_put(tree, &key, make_value(count)) == BPTREE_OK);
            fail_after = -1;
            ASSERT(bptree_check_invariants(tree));
            bptree_free(tree);
        }
    }
}

static void test_custom_values(int order) {
    bptree* tree = bptree_create(order, NULL, false);
    ASSERT(tree);
    for (int n = 0; n < 80; ++n) {
        bptree_key_t key = make_key(n);
        ASSERT(bptree_put(tree, &key, make_value(n)) == BPTREE_OK);
    }
    check_entries(tree, 80);
    bptree_key_t start = make_key(0), end = make_key(79);
    test_value_t* values = NULL;
    int count = 0;
    ASSERT(bptree_get_range(tree, &start, &end, &values, &count) == BPTREE_OK);
    ASSERT(count == 80);
    for (int n = 0; n < count; ++n) ASSERT(values[n].fields[0] == n);
    bptree_free_range_results(values);
    for (int n = 0; n < 80; ++n) {
        bptree_key_t key = make_key(n);
        ASSERT(bptree_remove(tree, &key) == BPTREE_OK);
        ASSERT(bptree_check_invariants(tree));
    }
    bptree_clear(tree);
    bptree_clear(tree);
    check_entries(tree, 0);
    bptree_free(tree);
}

int main(void) {
    const int orders[] = {3, 4, 7, 12, 32};
    for (size_t i = 0; i < sizeof(orders) / sizeof(orders[0]); ++i) {
        test_split_failures(orders[i]);
        test_custom_values(orders[i]);
    }
    puts("Allocation failure, clear, and custom value regression tests passed.");
    return 0;
}
