#include <assert.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define BPTREE_VALUE_TYPE int64_t
#define BPTREE_IMPLEMENTATION
#include "bptree.h"

static uint64_t next_rand(uint64_t state) {
    return state * 6364136223846793005ULL + 1442695040888963407ULL;
}

static const char* format_status(bptree_status st) {
    switch (st) {
        case BPTREE_OK:
            return "OK";
        case BPTREE_DUPLICATE_KEY:
            return "DUPLICATE_KEY";
        case BPTREE_KEY_NOT_FOUND:
            return "KEY_NOT_FOUND";
        case BPTREE_ALLOCATION_FAILURE:
            return "ALLOCATION_FAILURE";
        case BPTREE_INVALID_ARGUMENT:
            return "INVALID_ARGUMENT";
        case BPTREE_INTERNAL_ERROR:
            return "INTERNAL_ERROR";
    }
    return "UNKNOWN";
}

static void serialize_node(const bptree* tree, const bptree_node* node, FILE* out) {
    const bptree_key_t* keys = bptree_node_keys(node);
    if (node->is_leaf) {
        const bptree_value_t* vals = bptree_node_values((bptree_node*)node, tree->max_keys);
        fputs("(leaf [", out);
        for (int i = 0; i < node->num_keys; i++) {
            if (i > 0) fputc(',', out);
            fprintf(out, "%" PRId64 ":%" PRId64, (int64_t)keys[i], (int64_t)vals[i]);
        }
        fputs("])", out);
    } else {
        bptree_node** children = bptree_node_children((bptree_node*)node, tree->max_keys);
        fputs("(internal [", out);
        for (int i = 0; i < node->num_keys; i++) {
            if (i > 0) fputc(',', out);
            fprintf(out, "%" PRId64, (int64_t)keys[i]);
        }
        fputs("] [", out);
        for (int i = 0; i <= node->num_keys; i++) {
            if (i > 0) fputc(',', out);
            serialize_node(tree, children[i], out);
        }
        fputs("])", out);
    }
}

static void run_step(bptree* tree, int step_idx, int64_t k, int64_t v) {
    bptree_status st = bptree_put(tree, &k, v);
    assert(bptree_check_invariants(tree));
    bptree_stats stats = bptree_get_stats(tree);
    printf("STEP %d PUT %" PRId64 " %" PRId64 " -> %s count=%d height=%d nodes=%d tree=",
           step_idx, k, v, format_status(st), stats.count, stats.height, stats.node_count);
    serialize_node(tree, tree->root, stdout);
    putchar('\n');
}

static void run_queries(const bptree* tree) {
    for (int i = 0; i < 20; i++) {
        int64_t k = (int64_t)(i * 3 + 1);
        int64_t v = 0;
        bptree_status st = bptree_get(tree, &k, &v);
        if (st == BPTREE_OK) {
            printf("QUERY GET %" PRId64 " -> FOUND %" PRId64 "\n", k, v);
        } else {
            printf("QUERY GET %" PRId64 " -> NOT_FOUND\n", k);
        }
    }
    for (int i = 0; i < 10; i++) {
        int64_t lo = (int64_t)(i * 5 + 1);
        int64_t hi = lo + 12;
        int64_t* vals = NULL;
        int n = 0;
        bptree_status st = bptree_get_range(tree, &lo, &hi, &vals, &n);
        assert(st == BPTREE_OK);
        printf("QUERY RANGE %" PRId64 " %" PRId64 " -> [", lo, hi);
        for (int j = 0; j < n; j++) {
            if (j > 0) fputc(',', stdout);
            printf("%" PRId64, vals[j]);
        }
        puts("]");
        bptree_free_range_results(vals);
    }
}

int main(void) {
    const int orders[] = {3, 4, 7, 12};
    const int num_orders = (int)(sizeof(orders) / sizeof(orders[0]));

    for (int o = 0; o < num_orders; o++) {
        const int max_keys = orders[o];

        // Workload 1: Sequential + duplicates
        printf("WORKLOAD sequential max_keys=%d\n", max_keys);
        bptree* t1 = bptree_create(max_keys, NULL, false);
        assert(t1 != NULL);
        int step = 0;
        for (int i = 0; i < 40; i++) {
            int64_t k = i + 1;
            run_step(t1, step++, k, k * 10);
        }
        for (int i = 0; i < 10; i++) {
            int64_t k = i + 1;
            run_step(t1, step++, k, k * 10 + 1);
        }
        run_queries(t1);
        bptree_free(t1);

        // Workload 2: Reverse
        printf("WORKLOAD reverse max_keys=%d\n", max_keys);
        bptree* t2 = bptree_create(max_keys, NULL, false);
        assert(t2 != NULL);
        step = 0;
        for (int i = 0; i < 40; i++) {
            int64_t k = 40 - i;
            run_step(t2, step++, k, k * 10);
        }
        run_queries(t2);
        bptree_free(t2);

        // Workload 3: Pseudo-random with collisions
        printf("WORKLOAD random max_keys=%d\n", max_keys);
        bptree* t3 = bptree_create(max_keys, NULL, false);
        assert(t3 != NULL);
        uint64_t rng = 42;
        for (int s = 0; s < 100; s++) {
            rng = next_rand(rng);
            int64_t k = (int64_t)((rng >> 16) % 60) + 1;
            int64_t v = k * 100 + s;
            run_step(t3, s, k, v);
        }
        run_queries(t3);
        bptree_free(t3);
    }

    return 0;
}
