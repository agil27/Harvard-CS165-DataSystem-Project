#ifndef CS165_HASH_TABLE // This is a header guard. It prevents the header from being included more than once.
#define CS165_HASH_TABLE  
#include "linked.h"

#define BUCKET_SIZE 5

typedef struct HashTable {
    LinkedList list[BUCKET_SIZE];
} HashTable;

HashTable* create_hashtable();
int put(HashTable* ht, int key, int val);
int get(HashTable* ht, int key, int* val);
int delete_hashtable(HashTable* ht);

#endif
