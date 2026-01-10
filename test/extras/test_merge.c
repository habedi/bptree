#define BPTREE_IMPLEMENTATION
#include "bptree.h"
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

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

int main() {
    int max_keys = 3;
    bptree* tree = bptree_create(max_keys, compare_ints, true);

    printf("Inserting 10, 20, 30, 40...\n");
    bptree_key_t k1=10, k2=20, k3=30, k4=40, k5=50, k6=60;
    bptree_put(tree, &k1, (bptree_value_t)10);
    bptree_put(tree, &k2, (bptree_value_t)20);
    bptree_put(tree, &k3, (bptree_value_t)30);
    bptree_put(tree, &k4, (bptree_value_t)40);
    
    print_tree_structure(tree->root, 0, max_keys);
    // Expected: Root[30] -> Child0[10, 20], Child1[30, 40]

    printf("Deleting 10 (Trigger Merge Right)...\n");
    bptree_remove(tree, &k1);
    print_tree_structure(tree->root, 0, max_keys);
    // Expected: Root[20, 30, 40] (Height reduced)
    
    if (tree->height != 1) {
        printf("FAIL: Height should be 1, got %d\n", tree->height);
    }
    if (tree->count != 3) {
        printf("FAIL: Count should be 3, got %d\n", tree->count);
    }

    bptree_free(tree);
    
    printf("\nTest 2: Merge Left\n");
    tree = bptree_create(max_keys, compare_ints, true);
    bptree_put(tree, &k1, (bptree_value_t)10);
    bptree_put(tree, &k2, (bptree_value_t)20);
    bptree_put(tree, &k3, (bptree_value_t)30);
    bptree_put(tree, &k4, (bptree_value_t)40);
    bptree_put(tree, &k5, (bptree_value_t)50);
    bptree_put(tree, &k6, (bptree_value_t)60);
    // Root[30, 50] -> [10, 20], [30, 40], [50, 60]
    print_tree_structure(tree->root, 0, max_keys);
    
    printf("Deleting 60 (Trigger Merge Left)...\n");
    bptree_remove(tree, &k6);
    // [50, 60] -> [50]. Underflow.
    // Left sibling [30, 40]. Can't borrow.
    // Merge with left.
    // [30, 40] + [50] -> [30, 40, 50].
    // Root loses 50.
    // Root[30] -> [10, 20], [30, 40, 50].
    print_tree_structure(tree->root, 0, max_keys);
    
    if (!bptree_check_invariants(tree)) {
        printf("FAIL: Invariants\n");
    }

    bptree_free(tree);
    return 0;
}
