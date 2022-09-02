#include "hash_table.h"
#include <stdlib.h>
#include <stdio.h>
#include "linked.h"


HashTable* create_hashtable() {
    HashTable* ht = (HashTable*)malloc(sizeof(HashTable));
    for (int i = 0; i < BUCKET_SIZE; i++) {
        ht->list[i] = create_link();
    }
    return ht;
}

int put(HashTable* ht, int key, int val) {
    int hashed_key = key % BUCKET_SIZE;
    insert_link(&ht->list[hashed_key], key, val);
}

int get(HashTable* ht, int key, int* val) {
    int hashed_key = key % BUCKET_SIZE;
    return search_link(ht->list[hashed_key], key, val);
}

int delete_hashtable(HashTable* ht) {
    for (int i = 0; i < BUCKET_SIZE; i++) {
        clear_link(&ht->list[i]);
    }
    if (ht) {
        free(ht);
    }
}