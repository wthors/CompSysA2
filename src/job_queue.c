#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "job_queue.h"

// Initialize the job queue with the given capacity. Returns 0 on success, -1 on failure.

int job_queue_init(struct job_queue *job_queue, int capacity) {
  if (capacity <= 0) {
    return -1; //capacity must be positive
  }
  job_queue->buffer = malloc(sizeof(void *) * capacity);
  if (job_queue->buffer == NULL) {
    return -1; //memory allocation failed
  }
  job_queue->capacity = capacity;
  job_queue->count = 0;
  job_queue->head = 0;
  job_queue->tail = 0;
  job_queue->destroyed = 0;
  // Initialize mutex and condition variables
  if (pthread_mutex_init(&job_queue->mutex, NULL) != 0) {
    free(job_queue->buffer);
    return -1;
  }
  if (pthread_cond_init(&job_queue->not_empty, NULL) != 0) {
    pthread_mutex_destroy(&job_queue->mutex);
    free(job_queue->buffer);
    return -1;
  }
  if (pthread_cond_init(&job_queue->not_full, NULL) != 0) {
    pthread_cond_destroy(&job_queue->not_empty);
    pthread_mutex_destroy(&job_queue->mutex);
    free(job_queue->buffer);
    return -1;
  }
  return 0;
}

// Destroy the job queue, freeing resources. Blocks until all jobs are processed.

int job_queue_destroy(struct job_queue *job_queue) {
  // Lock and mark the queue as destroyed
  assert(pthread_mutex_lock(&job_queue->mutex) == 0);
  job_queue->destroyed = 1;
  // block until the queue is empty
  while (job_queue->count > 0) {
    assert(pthread_cond_wait(&job_queue->not_full, &job_queue->mutex) == 0);
  }
  //Queue is now empty, wake all waiting threads
  assert(pthread_cond_broadcast(&job_queue->not_empty) == 0);
  assert(pthread_cond_broadcast(&job_queue->not_full) == 0);
  assert(pthread_mutex_unlock(&job_queue->mutex) == 0);
  // free buffer memory
  free(job_queue->buffer);
  return 0;
}

// Push a new job onto the queue. Returns 0 on success, -1 if the queue is destroyed.

int job_queue_push(struct job_queue *job_queue, void *data) {
  assert(pthread_mutex_lock(&job_queue->mutex) == 0);
    // Error if queue has been destroyed
    if (job_queue->destroyed) {
        pthread_mutex_unlock(&job_queue->mutex);
        return -1;
    }
    // Queue is full, wait for space to become available
    while (job_queue->count == job_queue->capacity && !job_queue->destroyed) {
        assert(pthread_cond_wait(&job_queue->not_full, &job_queue->mutex) == 0);
        if (job_queue->destroyed) {  // check again after waking up
            pthread_mutex_unlock(&job_queue->mutex);
            return -1;
        }
    }
    if (job_queue->destroyed) {
        // If destroyed while waiting, do not push
        pthread_mutex_unlock(&job_queue->mutex);
        return -1;
    }
    // Insert the new job at the tail of the circular buffer
    job_queue->buffer[job_queue->tail] = data;
    job_queue->tail = (job_queue->tail + 1) % job_queue->capacity;
    job_queue->count++;
    // Signal that the queue is not empty (wake one waiting consumer)
    assert(pthread_cond_signal(&job_queue->not_empty) == 0);
    assert(pthread_mutex_unlock(&job_queue->mutex) == 0);
    return 0;
}

// Pop a job from the queue. Returns 0 on success, -1 if the queue is destroyed and empty.

int job_queue_pop(struct job_queue *job_queue, void **data) {
   assert(pthread_mutex_lock(&job_queue->mutex) == 0);
    // Queue is empty, wait for a job (unless destroyed)
    while (job_queue->count == 0 && !job_queue->destroyed) {
        assert(pthread_cond_wait(&job_queue->not_empty, &job_queue->mutex) == 0);
    }
    // If destroyed *and* no jobs remain, return -1 to signal termination
    if (job_queue->destroyed && job_queue->count == 0) {
        pthread_mutex_unlock(&job_queue->mutex);
        return -1;  // queue has been shut down
    }
    // Remove the job from the head of the queue
    assert(job_queue->count > 0);
    *data = job_queue->buffer[job_queue->head];
    job_queue->head = (job_queue->head + 1) % job_queue->capacity;
    job_queue->count--;
    // Signal that the queue is not full (wake one waiting producer)
    assert(pthread_cond_signal(&job_queue->not_full) == 0);
    assert(pthread_mutex_unlock(&job_queue->mutex) == 0);
    return 0;
}
