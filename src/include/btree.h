#ifndef BTREE_H
#define BTREE_H
#define FANOUT 128
#include <stdlib.h>
#include <stdio.h>


typedef union {
    struct Node* node[FANOUT];
    int value[FANOUT];
} Span;


typedef struct Node {
    int num_sub;
    int keys[FANOUT]; // 0 index is useless
    Span span; // pointer to the next layer 
    int leaf; // whether or not leaf node
    struct Node* next;
} Node;


typedef Node* BTree;


typedef struct {
    BTree node;
    int bias; // bias
    int found; // 1 for found, 0 for not
} BTreeFindResult;


typedef struct {
    int* values;
    int num;
} RangeResult;


BTree build_btree(int* key, int* val, int n);
BTreeFindResult btree_find(BTree root, int key);
int btree_search_range(BTree root, long min, long max, int* result);
int binary_search_range(int* sorted, int* data, int* result, long min, long max, int n);
int scan_search_range(int* sorted, int* data, int* result, long min, long max, int n);
#endif