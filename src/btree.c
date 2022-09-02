#include "btree.h"
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include "io.h"


int span_num(int n) {
    int num_leaves = (int)(n / FANOUT);
    if (num_leaves * FANOUT < n) {
        num_leaves++;
    }
    return num_leaves;
}


BTree build_btree(int* key, int* val, int n) {
    int num_leaves = span_num(n);
    BTree* node_list = (BTree*)malloc(num_leaves * sizeof(BTree));
    for (int i = 0; i < num_leaves; i++) {
        BTree leaf = (BTree)malloc(sizeof(Node));
        leaf->leaf = 1;
        leaf->num_sub = i < num_leaves - 1 ? FANOUT : (n - FANOUT * i);
        for (int j = 0; j < leaf->num_sub; j++) {
            leaf->keys[j] = key[i * FANOUT + j];
            leaf->span.value[j] = val[i * FANOUT + j];
        }
        node_list[i] = leaf;
    }
    for (int i = 0; i < num_leaves - 1; i++) {
        node_list[i]->next = node_list[i + 1];
    }

    int num_nodes = num_leaves;
    while (num_nodes > 1) {
        int num_span = span_num(num_nodes);
        BTree* new_node_list = (BTree*)malloc(num_span * sizeof(BTree));
        for (int i = 0; i < num_span; i++) {
            BTree node = (BTree)malloc(sizeof(Node));
            node->leaf = 0;
            node->num_sub = i < num_span - 1 ? FANOUT : (num_nodes - FANOUT * i);
            for (int j = 0; j < node->num_sub; j++) {
                BTree child_node = node_list[i * FANOUT + j];
                if (j > 0) {
                    node->keys[j] = child_node->keys[child_node->leaf ? 0 : 1];
                }
                node->span.node[j] = node_list[i * FANOUT + j];
            }
            new_node_list[i] = node;
        }
        for (int i = 0; i < num_span - 1; i++) {
            new_node_list[i]->next = new_node_list[i + 1];
        }
        num_nodes = num_span;
        node_list = new_node_list;
    }
    return node_list[0];
}


BTreeFindResult btree_find(BTree root, int key) {
    BTreeFindResult r;
    BTree p = root;
    // BTree q = NULL;
    int found = 0;

    while (p && !found) {
        // recursively search
        if (p->leaf) {
            int i = 0;
            while (i < p->num_sub && !satisfy_low(p->keys[i], key)) {
                i++;
            }
            // printf("leaf %d %d\n", i, p->keys[i]);
            if (p->keys[i] == key) {
                r.found = 1;
            } else {
                r.found = 0;
            }
            Node* q = p->next;
            while (q && !satisfy_low(q->keys[q->num_sub - 1], key)) {
                p = q;
                q = p->next;
            }
            // if not found we also return the closest one(the least greater element compared to the key)
            r.bias = 0;
            r.node = p;
            return r;
        }

        // elsewise, internal node
        int i = 1;
        while (i < p->num_sub && !satisfy_low(p->keys[i], key)) {
            i++;
        }
        // printf("internal %d %d\n", i - 1, p->keys[i - 1]);
        p = p->span.node[i - 1];
    }
}


int btree_search_range(BTree root, long min, long max, int* result) {
    BTreeFindResult left = btree_find(root, min);
    // BTreeFindResult right = btree_find(root, max);
    Node* p = left.node;
    // Node* q = right.node;
    Node* c = p;

    int num = 0;
    int reach_right = 0;
    while (c) {
        int start = (c == left.node ? left.bias : 0);
        for (int i = start; i < c->num_sub; i++) {
            int key = c->keys[i];
            int val = c->span.value[i];
            if (satisfy_high(key, max)) {
                if (satisfy_low(key, min)) {
                    result[num++] = val;
                }
            } else {
                reach_right = 1;
                break;
            }
        }
        if (!reach_right) {
            c = c->next;
        } else {
            break;
        }
    }
    return num;
}


int binsearch(int* sorted, int* data, int key, int start, int end) {
    int len = end - start + 1;
    if (len == 1) {
        if (satisfy_low(data[sorted[start]], key)) {
            return start;
        } else {
            return -1;
        }
    }

    if (len == 2) {
        if (satisfy_low(data[sorted[start]], key)) {
            return start;
        }
        if (satisfy_low(data[sorted[end]], key)) {
            return end;
        }
        return -1;
    }

    // len >= 3
    int mid = (int)((start + end) / 2);
    if (satisfy_low(data[sorted[mid]], key)) {
        return binsearch(sorted, data, key, start, mid);
    } else {
        return binsearch(sorted, data, key, mid, end);
    }
}


int binary_search(int* sorted, int* data, int key, int n) {
    return binsearch(sorted, data, key, 0, n - 1);
}


int binary_search_range(int* sorted, int* data, int* result, long min, long max, int n) {
    int start = binary_search(sorted, data, min, n);
    // printf("start %d\n", start);
    int num_result = 0;
    if (start == -1) {
        return 0;
    }
    for (int i = start; i < n; i++) {
        if (satisfy_high(data[sorted[i]], max)) {
            result[num_result++] = sorted[i];
        }
    }
    return num_result;
}

int scan_search_range(int* sorted, int* data, int* result, long min, long max, int n) {
    int num_result = 0;
    for (int i = 0; i < n; i++) {
        if (satisfy_low(data[i], min) && satisfy_high(data[i], max)) {
            result[num_result++] = i;
        }
    }
    return num_result;
}