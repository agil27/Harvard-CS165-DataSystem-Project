#ifndef CS165_HASH_TABLE // This is a header guard. It prevents the header from being included more than once.
#define CS165_HASH_TABLE  

#define FATNODE_SIZE 8

typedef int keyType;
typedef int valType;

typedef struct hashpair {
    keyType key;
    valType val;
} hashpair;

typedef struct hashnode {
    // implements a fat node: each node has an array of element 8
    hashpair array[FATNODE_SIZE];
    struct hashnode* next;
} hashnode;


typedef struct hashtable {
// define the components of the hash table here (e.g. the array, bookkeeping for number of elements, etc)
    hashnode* head;
    hashnode* rear;
    int size;
} hashtable;

int allocate(hashtable** ht, int size);
int put(hashtable* ht, keyType key, valType value);
int get(hashtable* ht, keyType key, valType *values, int num_values, int* num_results);
int erase(hashtable* ht, keyType key);
int deallocate(hashtable* ht);

#endif
