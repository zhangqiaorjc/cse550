#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <queue>

using namespace std;

/* task struct */
typedef struct {
	void* (*function)(void *);
	void *argument;
} threadpool_task_t;

void* worker_function(void *argument);

class ThreadPool {
public:
	
	// task queue, lock and condition variables
	queue<threadpool_task_t> task_queue;
	int max_task_queue_size;
	pthread_mutex_t task_queue_lock;	// protect access to queue AND num_incomplete_tasks variable
	pthread_cond_t task_queue_full;		// CV to indicate full queue or kill threadpool signal
	pthread_cond_t task_queue_empty;
	pthread_cond_t all_tasks_completed;	// condition variable to implement a semaphore
	
	// pool of threads
	int num_threads;
	int num_incomplete_tasks;
	pthread_t *threads;
	bool destroy_threads;

	ThreadPool(int num_threads_var, int max_task_queue_size_var);

	void add_task(threadpool_task_t new_task);

	void wait_all_tasks_complete();

	void destroy_pool();

	~ThreadPool();
};

#endif /* THREADPOOL_H */