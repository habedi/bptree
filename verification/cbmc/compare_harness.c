#include <assert.h>
#include <stdint.h>

#define BPTREE_IMPLEMENTATION
#include "bptree.h"

int64_t nondet_int64(void);

int main(void) {
    bptree_key_t a = nondet_int64();
    bptree_key_t b = nondet_int64();
    bptree_key_t c = nondet_int64();

    int ab = bptree_default_compare(&a, &b);
    int ba = bptree_default_compare(&b, &a);
    int bc = bptree_default_compare(&b, &c);
    int ac = bptree_default_compare(&a, &c);

    // 1. Trichotomy and bounded return values.
    assert(ab == -1 || ab == 0 || ab == 1);

    // 2. Reflexivity.
    assert(bptree_default_compare(&a, &a) == 0);

    // 3. Antisymmetry (also verifies absence of signed subtraction overflow at INT64_MIN).
    assert(ab == -ba);
    assert((ab == 0) == (a == b));
    assert((ab < 0) == (a < b));
    assert((ab > 0) == (a > b));

    // 4. Transitivity.
    if (ab < 0 && bc < 0) assert(ac < 0);
    if (ab == 0 && bc == 0) assert(ac == 0);

    return 0;
}
