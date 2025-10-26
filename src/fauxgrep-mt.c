// Setting _DEFAULT_SOURCE is necessary to activate visibility of
// certain header file contents on GNU/Linux systems.
#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fts.h>

// err.h contains various nonstandard BSD extensions, but they are
// very handy.
#include <err.h>

#include <pthread.h>

#include "job_queue.h"

// ---------- Global shared state ----------

pthread_mutex_t stdout_mutex = PTHREAD_MUTEX_INITIALIZER;

static char const *g_needle = NULL;

int fauxgrep_file(char const *needle, char const *path) {
  FILE *f = fopen(path, "r");
  if (f == NULL) {
    assert(pthread_mutex_lock(&stdout_mutex) == 0);
    warn("failed to open %s", path);
    assert(pthread_mutex_unlock(&stdout_mutex) == 0);
    return -1;
  }

  char *line = NULL;
  size_t linelen = 0;
  int lineno = 0;
  while (getline(&line, &linelen, f) != -1) {
    if (strstr(line, needle) != NULL) {
      assert(pthread_mutex_lock(&stdout_mutex) == 0);
      printf("%s:%d:%s", path, lineno, line);
      assert(pthread_mutex_unlock(&stdout_mutex) == 0);
    }
    lineno++;
  }

  free(line);
  fclose(f);
  return 0;
}

// ---------- Worker thread ----------

void *worker(void *arg) {
    struct job_queue *jq = (struct job_queue *)arg;
    void *data;
    while (job_queue_pop(jq, &data) == 0) {
        char *filepath = data;
        fauxgrep_file(g_needle, filepath);
        free(filepath);
    }
    return NULL;
}

// ---------- Main ----------

int main(int argc, char * const *argv) {
  if (argc < 2) {
        err(1, "usage: [-n INT] STRING paths...");
    }

    int num_threads = 1;
    char const *needle;
    char * const *paths;
    // Parse optional "-n k" flag for number of threads
    if (argc > 3 && strcmp(argv[1], "-n") == 0) {
        num_threads = atoi(argv[2]);
        if (num_threads < 1) {
            err(1, "invalid thread count: %s", argv[2]);
        }
        needle = argv[3];
        paths = &argv[4];
    } else {
        // No -n flag: default to 1 thread
        needle = argv[1];
        paths = &argv[2];
    }

    g_needle = needle;                      // make the search string accessible to all threads
    struct job_queue jq;
    if (job_queue_init(&jq, 64) != 0) {     // initialize job queue with capacity 64
        err(1, "job_queue_init failed");
    }

    // Create the worker threads
    pthread_t *threads = calloc(num_threads, sizeof(pthread_t));
    if (threads == NULL) {
        err(1, "failed to allocate thread array");
    }
    for (int i = 0; i < num_threads; i++) {
        if (pthread_create(&threads[i], NULL, worker, &jq) != 0) {
            err(1, "pthread_create() failed");
        }
    }

    // Traverse the given file/directory paths and enqueue each file found
    int fts_flags = FTS_LOGICAL | FTS_NOCHDIR;
    FTS *ftsp = fts_open(paths, fts_flags, NULL);
    if (ftsp == NULL) {
        // If the directory traversal cannot be started, clean up and exit
        job_queue_destroy(&jq);
        err(1, "fts_open() failed");
    }
    FTSENT *entry;
    while ((entry = fts_read(ftsp)) != NULL) {
        if (entry->fts_info == FTS_F) {  // regular file
            // Duplicate the file path and push it onto the job queue
            char *path_copy = strdup(entry->fts_path);
            if (path_copy == NULL) {
                err(1, "out of memory duplicating path");
            }
            if (job_queue_push(&jq, path_copy) != 0) {
                // If the queue is destroyed or an error occurs, stop processing
                free(path_copy);
                fts_close(ftsp);
                job_queue_destroy(&jq);
                err(1, "job_queue_push failed");
            }
        }
        // (Ignore other cases: directories are handled by fts, symbolic links, etc., are skipped)
    }
    fts_close(ftsp);

    // No more files to enqueue. Destroy the queue to signal workers no more jobs will be added.
    job_queue_destroy(&jq);
    // At this point, the queue is empty and marked destroyed; waiting threads will break out.

    // Join all worker threads to ensure they have finished processing.
    for (int i = 0; i < num_threads; i++) {
        if (pthread_join(threads[i], NULL) != 0) {
            err(1, "pthread_join() failed");
        }
    }
    free(threads);
    return 0;
}
