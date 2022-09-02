#ifndef LINK_H
#define LINK_H

#include <stdio.h>
#include <stdlib.h>

#define MAX_NODE 512
#define NODE_SIZE 256

typedef struct Pair {
    int key;
    int val;
} Pair;

typedef struct LinkNode {
    Pair* array;
    struct LinkNode* next;
} LinkNode;

typedef struct LinkedList {
    LinkNode* head;
    LinkNode* rear;
    int length;
    int last_index;
} LinkedList;


LinkedList create_link();
Pair make_pair(int key, int val);
void insert_link(LinkedList* l, int key, int val);
int search_link(LinkedList l, int key, int* val);
void clear_link(LinkedList* l);

#endif