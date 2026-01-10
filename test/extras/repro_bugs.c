#define BPTREE_IMPLEMENTATION
#include "bptree.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <assert.h>
#include <string.h>

#define MAX_ITEMS 2000
#define OPERATIONS 10000

// Simple numeric key comparison
int compare_ints(const bptree_key_t* a, const bptree_key_t* b) {
    return (*a < *b) ? -1 : ((*a > *b) ? 1 : 0);
}

void print_tree_structure(bptree_node* node, int depth, int max_keys) {
    if (!node) return;
    for (int i = 0; i < depth; i++) printf("  ");
    printf("%s [", node->is_leaf ? "LEAF" : "INTERNAL");
    bptree_key_t* keys = (bptree_key_t*)node->data;
    for (int i = 0; i < node->num_keys; i++) {
        printf(" %lld", (long long)keys[i]);
    }
    printf(" ]\n");
    
    if (!node->is_leaf) {
        size_t offset = (size_t)(max_keys + 1) * sizeof(bptree_key_t);
        size_t req_align = (sizeof(bptree_value_t) > sizeof(bptree_node*) ? sizeof(bptree_value_t) : sizeof(bptree_node*));
        size_t pad = (req_align - (offset % req_align)) % req_align;
        bptree_node** children = (bptree_node**)(node->data + offset + pad);
        
        for (int i = 0; i <= node->num_keys; i++) {
            print_tree_structure(children[i], depth + 1, max_keys);
        }
    }
}

int main(int argc, char** argv) {
    int seed = time(NULL);
    int max_keys = 3;
    int operations = 10000;

    if (argc > 1) seed = atoi(argv[1]);
    if (argc > 2) max_keys = atoi(argv[2]);
    if (argc > 3) operations = atoi(argv[3]);

    printf("Seed: %d, Max Keys: %d, Operations: %d\n", seed, max_keys, operations);
    srand(seed);

    bptree* tree = bptree_create(max_keys, compare_ints, false);
    
    // Keep track of what we think is in the tree
    bool in_tree[MAX_ITEMS] = {0};
    int count = 0;

    for (int i = 0; i < operations; i++) {
        int op = rand() % 10;
        int key_val = rand() % MAX_ITEMS;
        bptree_key_t key = (bptree_key_t)key_val;
        bptree_value_t val = (bptree_value_t)(intptr_t)key_val;

        if (op < 3) { // 30% Insert
            if (!in_tree[key_val]) {
                // printf("Op %d: Insert %d\n", i, key_val);
                bptree_status status = bptree_put(tree, &key, val);
                if (status != BPTREE_OK) {
                    printf("Insert failed for key %d with status %d\n", key_val, status);
                    exit(1);
                }
                in_tree[key_val] = true;
                count++;
            }
        } else if (op < 6) { // 30% Delete
            if (in_tree[key_val]) {
                // printf("Op %d: Remove %d\n", i, key_val);
                bptree_status status = bptree_remove(tree, &key);
                if (status != BPTREE_OK) {
                    printf("Remove failed for key %d with status %d\n", key_val, status);
                    // print_tree_structure(tree->root, 0, tree->max_keys);
                    exit(1);
                }
                in_tree[key_val] = false;
                count--;
            }
        } else if (op < 8) { // 20% Get
            if (in_tree[key_val]) {
                bptree_value_t val;
                bptree_status status = bptree_get(tree, &key, &val);
                if (status != BPTREE_OK) {
                    printf("Get failed for key %d with status %d\n", key_val, status);
                    exit(1);
                }
                if ((intptr_t)val != key_val) {
                    printf("Get returned wrong value for key %d: %ld\n", key_val, (intptr_t)val);
                    exit(1);
                }
            } else {
                bptree_value_t val;
                bptree_status status = bptree_get(tree, &key, &val);
                if (status == BPTREE_OK) {
                    printf("Get found deleted/missing key %d!\n", key_val);
                    exit(1);
                }
            }
        } else { // 20% Range Query
            int start_val = rand() % MAX_ITEMS;
            int end_val = start_val + rand() % 100;
            if (end_val >= MAX_ITEMS) end_val = MAX_ITEMS - 1;
            
            bptree_key_t start_key = (bptree_key_t)start_val;
            bptree_key_t end_key = (bptree_key_t)end_val;
            
            bptree_value_t* results = NULL;
            int n_results = 0;
            bptree_status status = bptree_get_range(tree, &start_key, &end_key, &results, &n_results);
            if (status != BPTREE_OK) {
                printf("Range query failed for [%d, %d] with status %d\n", start_val, end_val, status);
                exit(1);
            }
            
            int expected_count = 0;
            for (int k = start_val; k <= end_val; k++) {
                if (in_tree[k]) expected_count++;
            }
            
            if (n_results != expected_count) {
                printf("Range query count mismatch for [%d, %d]. Got %d, Expected %d\n", start_val, end_val, n_results, expected_count);
                exit(1);
            }
            
            // Verify results are sorted
            for (int k = 0; k < n_results - 1; k++) {
                if ((intptr_t)results[k] >= (intptr_t)results[k+1]) {
                    printf("Range query results not sorted!\n");
                    exit(1);
                }
            }
            
            // Verify values match keys (since value == key in this test)
            for (int k = 0; k < n_results; k++) {
                 int val = (int)(intptr_t)results[k];
                 if (val < start_val || val > end_val || !in_tree[val]) {
                     printf("Range query returned invalid value: %d\n", val);
                     exit(1);
                 }
            }

            free(results);
        }
        if (!bptree_check_invariants(tree)) {
            printf("Invariant check failed after operation %d (Key: %d, Op: %s)\n", 
                   i, key_val, (op < 6) ? "Insert" : "Remove");
            // print_tree_structure(tree->root, 0, tree->max_keys);
            exit(1);
        }
        
        if (tree->count != count) {
             printf("Count mismatch! Tree: %d, Expected: %d\n", tree->count, count);
             exit(1);
        }
    }

    printf("Completed %d operations successfully. Final count: %d, Height: %d\n", operations, tree->count, tree->height);
    bptree_free(tree);
    return 0;
}
