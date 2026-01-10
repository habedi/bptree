#define BPTREE_IMPLEMENTATION
#define BPTREE_KEY_TYPE_STRING
#define BPTREE_KEY_SIZE 16
#include "bptree.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <assert.h>
#include <string.h>

#define MAX_ITEMS 2000

// Default compare is fine for strings (memcmp)

void make_key(bptree_key_t* key, int val) {
    memset(key, 0, BPTREE_KEY_SIZE);
    snprintf(key->data, BPTREE_KEY_SIZE, "%08d", val);
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

    bptree* tree = bptree_create(max_keys, NULL, false);
    
    bool in_tree[MAX_ITEMS] = {0};
    int count = 0;

    for (int i = 0; i < operations; i++) {
        int op = rand() % 10;
        int key_val = rand() % MAX_ITEMS;
        bptree_key_t key;
        make_key(&key, key_val);
        bptree_value_t val = (bptree_value_t)(intptr_t)key_val;

        if (op < 3) { // 30% Insert
            if (!in_tree[key_val]) {
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
                bptree_status status = bptree_remove(tree, &key);
                if (status != BPTREE_OK) {
                    printf("Remove failed for key %d with status %d\n", key_val, status);
                    exit(1);
                }
                in_tree[key_val] = false;
                count--;
            }
        } else if (op < 8) { // 20% Get
            if (in_tree[key_val]) {
                bptree_value_t val_out;
                bptree_status status = bptree_get(tree, &key, &val_out);
                if (status != BPTREE_OK) {
                    printf("Get failed for key %d with status %d\n", key_val, status);
                    exit(1);
                }
                if ((intptr_t)val_out != key_val) {
                    printf("Get returned wrong value for key %d: %ld\n", key_val, (intptr_t)val_out);
                    exit(1);
                }
            } else {
                 bptree_value_t val_out;
                 if (bptree_get(tree, &key, &val_out) == BPTREE_OK) {
                     printf("Get found missing key %d\n", key_val);
                     exit(1);
                 }
            }
        } else { // 20% Range Query
            int start_val = rand() % MAX_ITEMS;
            int end_val = start_val + rand() % 100;
            if (end_val >= MAX_ITEMS) end_val = MAX_ITEMS - 1;
            
            bptree_key_t start_key, end_key;
            make_key(&start_key, start_val);
            make_key(&end_key, end_val);
            
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
            free(results);
        }

        if (!bptree_check_invariants(tree)) {
            printf("Invariant check failed after operation %d\n", i);
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
