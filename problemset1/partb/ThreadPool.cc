#include "ThreadPool.h"

#include <iostream>
#include <assert.h> 


#define handle_error(msg) \
           do { perror(msg); exit(EXIT_FAILURE); } while (0)


ThreadPool::ThreadPool(int num_threads_var, int max_task_queue_size_var) {
	// initialize ThreadPool internal states	
	num_threads = num_threads_var;
	max_task_queue_size = max_task_queue_size_var;
	num_incomplete_tasks = 0;
	destroy_threads = false;

	// initialize locks and condition variables
	pthread_mutex_init(&task_queue_lock, NULL);
	pthread_cond_init(&task_queue_full, NULL);
	pthread_cond_init(&task_queue_empty, NULL);
	pthread_cond_init(&all_tasks_completed, NULL);

	// create all threads
	threads = new pthread_t[num_threads];
	for (int i = 0; i < num_threads; ++i) {
		if (pthread_create(&threads[i], NULL,
						 &worker_function, (void*)this) < 0) {
			// thread create error
			destroy_pool();	// clean up
			handle_error("pthread_create");	//exit with error
		}
	}

}

void ThreadPool::add_task(threadpool_task_t new_task) {
	// add task to task_queue
	pthread_mutex_lock(&task_queue_lock);

	while (task_queue.size() == max_task_queue_size) {
		// wait if task_queue is full
		pthread_cond_wait(&task_queue_empty, &task_queue_lock);
	}

	task_queue.push(new_task);
	num_incomplete_tasks++;
	pthread_cond_signal(&task_queue_full);	// signal to waiting worker threads

	pthread_mutex_unlock(&task_queue_lock);
}

void ThreadPool::wait_all_tasks_complete() {

	// wait for all tasks to be completed
	pthread_mutex_lock(&task_queue_lock);
	while (num_incomplete_tasks > 0) {
		pthread_cond_wait(&all_tasks_completed, &task_queue_lock);
	}
	pthread_mutex_unlock(&task_queue_lock);
	
}

void ThreadPool::destroy_pool() {
	pthread_mutex_lock(&task_queue_lock);

	// wake up all waiting worker threads and terminate them
	destroy_threads = true;
	pthread_cond_broadcast(&task_queue_full);	// wake up waiting worker threads to exit

	// clear queue
	while (!task_queue.empty()) task_queue.pop();

	pthread_mutex_unlock(&task_queue_lock);

	pthread_mutex_destroy(&task_queue_lock);	// delete lock

	delete[] threads;	// deallocate threads array
}

void* worker_function(void *argument) {
	ThreadPool *pool = (ThreadPool *) argument;

	while (1) {
		// retrieve a task from pool's task queue
		pthread_mutex_lock(&pool->task_queue_lock);
		
		while ((pool->task_queue.size() == 0)
				 && (pool->destroy_threads == false)) {
			// wait if task_queue is empty
			pthread_cond_wait(&pool->task_queue_full, &pool->task_queue_lock);
		}
		
		// if worker thread is being cancelled
		if (pool->destroy_threads) {
			pthread_mutex_unlock(&pool->task_queue_lock);
			return NULL;
		}

		if (pool->task_queue.size() > 0) {
			// remove a task from queue and start running the function
			threadpool_task_t task = pool->task_queue.front();
			pool->task_queue.pop();

			pthread_cond_signal(&pool->task_queue_empty);
			pthread_mutex_unlock(&pool->task_queue_lock);
			
			// runs the task
			(*(task.function))(&task.argument);
		
			// decrement num_incomplete_tasks
			pthread_mutex_lock(&pool->task_queue_lock);
			pool->num_incomplete_tasks--;
			pthread_cond_signal(&pool->all_tasks_completed);
			pthread_mutex_unlock(&pool->task_queue_lock);
		} else {
			pthread_mutex_unlock(&pool->task_queue_lock);	
		}

	}
}

// ThreadPool::~ThreadPool() {
// 	delete[] threads;
// }


// void say_hello(void *threadID) {
// 	printf("hello from thread\n");
// }

// int main() {
// 	ThreadPool tp(20, 5);
// 	threadpool_task_t task;
// 	task.function = &say_hello;
// 	int a = 1;
// 	task.argument = &a;
// 	for (int i = 0; i < 10000; ++i) {
// 		tp.add_task(task);
// 	}
// 	//tp.wait_all_tasks_complete();
// 	// sleep(2);
// 	tp.destroy_pool();
// }

