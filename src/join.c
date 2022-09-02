#include "join.h"
#include "hash_table.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>


void swap_ptr(int** a, int** b) {
    int* t = *a;
    *a = *b;
    *b = t;
}

void swap_int(int* a, int* b) {
    int t = *a;
    *a = *b;
    *b = t;
}

// returns the number of the result
int nested_looped_join(int* pos1, int* val1, int* pos2, int* val2, int n1, int n2, int* r1, int* r2, int* s1, int* s2) {
    int* p1 = pos1;
    int* v1 = val1;
    int* p2 = pos2;
    int* v2 = val2;
    int m1 = n1;
    int m2 = n2;
    int* ptr1 = r1;
    int* ptr2 = r2;
    if (n1 > n2) {
        swap_ptr(&p1, &p2);
        swap_ptr(&v1, &v2);
        swap_ptr(&ptr1, &ptr2);
        swap_int(&m1, &m2);
    }

    int* mask1 = (int*)malloc(sizeof(int) * m1);
    int* mask2 = (int*)malloc(sizeof(int) * m2);
    memset(mask1, 0, m1 * sizeof(int));
    memset(mask2, 0, m2 * sizeof(int));

    int t1 = 0;
    int t2 = 0;
    for (int i = 0; i < m1; i++) {
        for (int j = 0; j < m2; j++) {
            if (v1[i] == v2[j]) {
                // if (mask1[i] == 0) {
                //     ptr1[t1++] = p1[i];
                //     mask1[i] = 1;
                // }
                ptr1[t1++] = p1[i];
                ptr2[t2++] = p2[j];
            }
        }
    }
    
    if (n1 > n2) {
        swap_int(&t1, &t2);
    }
    *s1 = t1;
    *s2 = t2;
    free(mask1);
    free(mask2);
    printf("nested loop %d %d\n", t1, t2);
    return 0;
}


int hash_join(int* pos1, int* val1, int* pos2, int* val2, int n1, int n2, int* r1, int* r2, int* s1, int* s2) {
    int* p1 = pos1;
    int* v1 = val1;
    int* p2 = pos2;
    int* v2 = val2;
    int m1 = n1;
    int m2 = n2;
    int* ptr1 = r1;
    int* ptr2 = r2;

    if (n1 > n2) {
        swap_ptr(&p1, &p2);
        swap_ptr(&v1, &v2);
        swap_ptr(&ptr1, &ptr2);
        swap_int(&m1, &m2);
    }
    
    HashTable* ht = create_hashtable();
    for (int i = 0; i < m1; i++) {
        put(ht, v1[i], p1[i]);
    }

    int t1 = 0;
    int t2 = 0;
    int* mask1 = (int*)malloc(sizeof(int) * m1);
    int* mask2 = (int*)malloc(sizeof(int) * m2);
    memset(mask1, 0, m1 * sizeof(int));
    memset(mask2, 0, m2 * sizeof(int));
    for (int j = 0; j < m2; j++) {
        int* positions = (int*)malloc(sizeof(int) * m1);
        int num_results = get(ht, v2[j], positions);
        for (int i = 0; i < num_results; i++) {
            // printf("%d %d %d %d %d\n", positions[i], p1[positions[i]], p2[i], v1[p1[positions[j]]], v2[j]);
            ptr1[t1++] = positions[i];
            ptr2[t2++] = p2[j];
        }
    }
    delete_hashtable(ht);
    free(mask1);
    free(mask2);
    if (n1 > n2) {
        swap_int(&t1, &t2);
    }
    *s1 = t1;
    *s2 = t2;
    printf("hash join %d %d\n", t1, t2);
    return 0;
}