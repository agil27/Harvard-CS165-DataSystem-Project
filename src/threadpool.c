#include "threadpool.h"


void create_pool(int max_thread_num, ThreadPool** pool) {
    *pool = (ThreadPool*)malloc(sizeof(ThreadPool));
    (*pool)->shutdown = 0;
    (*pool)->max_thread_num = max_thread_num;
    (*pool)->thread_id = (pthread_t*)malloc(max_thread_num * sizeof(pthread_t));
    (*pool)->head = NULL;

    pthread_mutex_init(&((*pool)->lock), NULL);
    pthread_cond_init(&((*pool)->ready), NULL);

    for (int i = 0; i < max_thread_num; i++) { 
        pthread_create(&((*pool)->thread_id[i]), NULL, thread_routine, (void*)(*pool));
    }
}


int add_worker(void* (*process)(void*), void* arg, ThreadPool* pool) {
    ThreadWorker* worker = (ThreadWorker*)malloc(sizeof(ThreadWorker));
    worker->process = process;
    worker->arg = arg;
    worker->next = NULL;
    pthread_mutex_lock(&(pool->lock));

    ThreadWorker *current = pool->head;
    // add the new worker to the end of the linked-list
    if (current != NULL)
    {
        while (current->next != NULL)
            current = current->next;
        current->next = worker;
    } else {
        pool->head = worker;
    }
    pthread_cond_signal(&(pool->ready));
    pthread_mutex_unlock(&(pool->lock));
    return 0;
}


int destroy_pool(ThreadPool* pool)
{
    // circumvent repeated destroy
    if (pool->shutdown) {
        return -1;
    }
    pool->shutdown = 1;

    pthread_mutex_lock(&(pool->lock));
    pthread_cond_broadcast(&(pool->ready));
    pthread_mutex_unlock(&(pool->lock));

    for (int i = 0; i < pool->max_thread_num; i++) {
        pthread_join(pool->thread_id[i], NULL);
    }
        
    free(pool->thread_id);
    
    ThreadWorker* head = NULL;
    // release the linked-list
    while (pool->head != NULL)
    {
        head = pool->head;
        pool->head = pool->head->next;
        free(head);
    }
    
    pthread_mutex_destroy(&(pool->lock));
    pthread_cond_destroy(&(pool->ready));
    
    free (pool);
    return 0;
}


void* thread_routine(void* arg) {
    ThreadPool* pool = (ThreadPool*)arg;

    // printf ("starting thread 0x%x\n", pthread_self());
    while (1)
    {
        pthread_mutex_lock(&(pool->lock));
        while (pool->head == NULL && !pool->shutdown)
        {
            // printf("thread 0x%x is waiting\n", pthread_self());
            pthread_cond_wait(&(pool->ready), &(pool->lock));
        }
        
        if (pool->shutdown)
        {
            pthread_mutex_unlock(&(pool->lock));
            // printf("thread 0x%x will exit\n", pthread_self());
            pthread_exit(NULL);
        }
        
        // elsewise execute the recall function
        ThreadWorker *worker = pool->head;
        pool->head = worker->next;
        pthread_mutex_unlock(&(pool->lock));
        worker->process(worker->arg);
        free(worker);
    }
    // should be non-reachable
    return NULL;
}