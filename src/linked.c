#include "linked.h"


LinkedList create_link() {
    LinkedList l;
    l.head = NULL;
    l.rear = NULL;
    l.length = 0;
    l.last_index = -1;
    return l;
}


Pair make_pair(int key, int val) {
    Pair pair;
    pair.key = key;
    pair.val = val;
    return pair;
}


void insert_link(LinkedList* l, int key, int val) {
    if (l->head == NULL) {
        l->head = (LinkNode*)malloc(sizeof(LinkNode));
        l->head->array = (Pair*)malloc(sizeof(Pair) * NODE_SIZE);
        l->head->next = NULL;
        l->rear = l->head;
    }
    if (l->last_index == NODE_SIZE - 1) {
        LinkNode* new_rear = (LinkNode*)malloc(sizeof(LinkNode));
        new_rear->array = (Pair*)malloc(sizeof(Pair) * NODE_SIZE);
        new_rear->next = NULL;
        l->rear->next = new_rear;
        l->rear = new_rear;
        l->last_index = -1;
    }
    l->last_index++;
    l->rear->array[l->last_index] = make_pair(key, val);
}

// return num_res
int search_link(LinkedList l, int key, int* val) {
    int n = 0;
    if (l.head == NULL) {
        return 0;
    }
    for (LinkNode* p = l.head; p != NULL; p = p->next) {
        int end_index = (p == l.rear) ? (l.last_index + 1) : NODE_SIZE; 
        for (int i = 0; i < end_index; i++) {
            if (p->array[i].key == key) {
                val[n] = p->array[i].val;
                n++;
            }
        }
    }
    return n;
}


void clear_link(LinkedList* l) {
    LinkNode* p = l->head;
    if (p == NULL) {
        return;
    }
    while (p != NULL) {
        LinkNode* q = p->next;
        if (p->array) {
            free(p->array);
            p->next = NULL;
        }
        p = q;
    }
}