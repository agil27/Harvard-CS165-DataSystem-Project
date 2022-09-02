#ifndef THREADPOOL_H
#define THREADPOOL_H


#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <pthread.h>
#include <assert.h>
#include <stdio.h>


// thread worker struct
// can be viewed as a linked-list node
typedef struct worker
{
    void*(*process)(void*); // callee function
    void* arg; // arguments
    struct worker* next; // next node
} ThreadWorker;


// threadpool struct
typedef struct
{
    size_t shutdown;
    size_t max_thread_num;
    ThreadWorker* head; 
    pthread_mutex_t lock;
    pthread_cond_t ready;   
    pthread_t* thread_id;
} ThreadPool;


void create_pool(int max_thread_num, ThreadPool** pool);
int add_worker (void*(*process) (void* arg), void* arg, ThreadPool* pool);
int destroy_pool(ThreadPool* pool);
void* thread_routine(void* arg);


#endif