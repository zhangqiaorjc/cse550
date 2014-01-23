#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <queue>

using namespace std;

// task struct
// THREADPOOL client wraps the worker function as a task
typedef struct {
	void* (*function)(void *);
	void *argument;
} threadpool_task_t;

// threadMain function for a worker thread
// argument and return values are void* for maximum generality
void* worker_function(void *argument);


// THREADPOOL class
class ThreadPool {
public:
	
	// task queue, lock and condition variables
	queue<threadpool_task_t> task_queue;
	int max_task_queue_size;
	pthread_mutex_t task_queue_lock;	// protect access to queue AND num_incomplete_tasks variable
	pthread_cond_t task_queue_full;		// CV to indicate full queue or kill threadpool signal
	pthread_cond_t task_queue_empty;	// CV to indicate empty queue
	pthread_cond_t all_tasks_completed;	// condition variable to implement a semaphore
	
	// pool of threads
	int num_threads;			// total number of threads in the pool
	int num_incomplete_tasks;	// number of incomplete tasks
	pthread_t *threads;			// list of all threads in the pool
	bool destroy_threads;		// bool to indicate destroy_pool() has been called

	// create and initialize the ThreadPool
	// cleans up if error in thread creation
	ThreadPool(int num_threads_var, int max_task_queue_size_var);

	// client of ThreadPool calls add_task to use the ThreadPool
	void add_task(threadpool_task_t new_task);

	// client can call to wait for all tasks to complete
	void wait_all_tasks_complete();

	// destroys the pool
	// wake up all idle threads and make them return
	// clean up memory allocations
	void destroy_pool();

	// clean up remaining memory allocations left by destroy_pool
	~ThreadPool();
};

#endif /* THREADPOOL_H */