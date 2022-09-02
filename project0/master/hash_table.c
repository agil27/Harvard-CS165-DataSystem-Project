#include "hash_table.h"
#include <stdlib.h>
#include <stdio.h>

# define HASH_MOD_BASE 37

// utility function to compute number of nodes needed
int getNumNodes(int a, int b) {
    if (a % b == 0) {
        return (int)(a / b);
    } else {
        return (int)(a / b) + 1;
    }
}

// Initialize the components of a hashtable.
// The size parameter is the expected number of elements to be inserted.
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the parameter passed to the method is not null, if malloc fails, etc).
int allocate(hashtable** ht, int size) {
    // allocate space
    // also deal with the failed malloc attempt
    (void)size;
    if (!((*ht) = malloc(HASH_MOD_BASE * sizeof(hashtable)))) {
        return -1;
    }

    for (int i = 0; i < HASH_MOD_BASE; i++) {
        // initialize each hashtable;
        (*ht)[i].size = 0;
        (*ht)[i].head = (*ht)[i].rear = 0;
    }
    
    return 0;
}

// This method inserts a key-value pair into the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if malloc is called and fails).
int put(hashtable* ht, keyType key, valType value) {
    if (!ht) {
        return -1;
    }
    // get hash bucket
    keyType hashed_key = key % HASH_MOD_BASE;
    hashtable* bucket= &(ht[hashed_key]);

    // decide where to insert and create new linked list node if need
    int insert_pos = (bucket->size++) % FATNODE_SIZE;

    // if the bucket is empty, create a new node
    if (!(bucket->head)) {
        if (!(bucket->head = malloc(sizeof(hashnode)))) {
            return -1;
        }
        bucket->rear = bucket->head;
    } else if (insert_pos == 0) {
        // if the fat node is filled, create a new node
        if (!((bucket->rear)->next = malloc(sizeof(hashnode)))) {
            return -1;
        }
        bucket->rear = (bucket->rear)->next;
    }

    (bucket->rear)->array[insert_pos].key = key;
    (bucket->rear)->array[insert_pos].val = value;
    return 0;
}

// This method retrieves entries with a matching key and stores the corresponding values in the
// values array. The size of the values array is given by the parameter
// num_values. If there are more matching entries than num_values, they are not
// stored in the values array to avoid a buffer overflow. The function returns
// the number of matching entries using the num_results pointer. If the value of num_results is greater than
// num_values, the caller can invoke this function again (with a larger buffer)
// to get values that it missed during the first call. 
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
int get(hashtable* ht, keyType key, valType *values, int num_values, int* num_results) {
    if (!ht) {
        return -1;
    }

    keyType hashed_key = key % HASH_MOD_BASE;
    hashtable* bucket= &ht[hashed_key];
    hashnode* p = bucket->head;
    int size = bucket->size;

    //if the bucket is empty;
    if (size == 0) {
        return 0;
    }

    // elsewise traverse every node and every array entry
    int num_nodes = getNumNodes(size, FATNODE_SIZE);
    int last_index = (size - 1) % FATNODE_SIZE;
    
    for (int i = 0; i < num_nodes; i++) {
        int max_index = (i == (num_nodes - 1) ? last_index : FATNODE_SIZE - 1);
        for (int j = 0; j <= max_index; j++) {
            if (++(*num_results) <= num_values) {
                values[(*num_results) - 1] = p->array[j].val;
            }
        }
        p = p->next;
    }
    return 0;
}

// This method erases a bucket
// It returns an error code, 0 for success and -1 otherwise.
int eraseBucket(hashtable* bucket) {
    hashnode* p = bucket->head;
    int size = bucket->size;

    // if the bucket is empty;
    if (size == 0) {
        return 0;
    }

    // elsewise traverse and delete
    int num_nodes = getNumNodes(size, FATNODE_SIZE);
    for (int i = 0; i < num_nodes; i++) {
        hashnode* prev = p;
        p = p->next;
        free(prev);
    }
    bucket->size = 0;
    return 0;
}

// This method erases all key-value pairs with a given key from the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
int erase(hashtable* ht, keyType key) {
    if (!ht) {
        return -1;
    }

    keyType hashed_key = key % HASH_MOD_BASE;
    hashtable* bucket= &ht[hashed_key];
    return eraseBucket(bucket);
}

// This method frees all memory occupied by the hash table.
// It returns an error code, 0 for success and -1 otherwise.
int deallocate(hashtable* ht) {
    // This line tells the compiler that we know we haven't used the variable
    // yet so don't issue a warning. You should remove this line once you use
    // the parameter.
    for (int i = 0; i < HASH_MOD_BASE; i++) {
        if (eraseBucket(&ht[i]) < 0) {
            return -1;
        }
    }
    free(ht);
    return 0;
}
