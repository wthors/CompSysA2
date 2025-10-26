// fhistogram-mt.c
// Multithreaded file bit-histogram with live UI like the original.

#define _DEFAULT_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fts.h>
#include <pthread.h>
#include <err.h>

#include "job_queue.h"
#include "histogram.h" 

// ---------- Global shared state ----------

static int    g_hist[8] = {0};          // global bit counts (bit 0..7)
static size_t g_total_bytes = 0;        // total bytes processed across all files
static size_t g_last_ui_bytes = 0;      // last byte count printed to the UI

// Protect shared histogram and counters
static pthread_mutex_t g_hist_mutex   = PTHREAD_MUTEX_INITIALIZER;
// Serialize all terminal output (warnings + histogram UI)
pthread_mutex_t         stdout_mutex  = PTHREAD_MUTEX_INITIALIZER;

// UI cadence: print after roughly this many new bytes
static const size_t PRINT_STEP = 100000;

// Convenience: safe UI print of the current snapshot
static void ui_print_locked(void) {
    pthread_mutex_lock(&stdout_mutex);
    print_histogram(g_hist);    // prints 8 bars + footer
    fflush(stdout);
    move_lines(9);              // go back up to overwrite next time
    pthread_mutex_unlock(&stdout_mutex);
}

// ---------- Worker thread ----------

static void *worker_fn(void *arg) {
    struct job_queue *jq = (struct job_queue *)arg;

    void *data;
    while (job_queue_pop(jq, &data) == 0) {
        char *filepath = (char *)data;

        FILE *f = fopen(filepath, "rb");
        if (!f) {
            pthread_mutex_lock(&stdout_mutex);
            warn("failed to open %s", filepath);
            pthread_mutex_unlock(&stdout_mutex);
            free(filepath);
            continue;
        }

        // Local accumulators to reduce contention
        int    local_hist[8] = {0};
        size_t local_bytes_since_merge = 0;

        unsigned char buf[8192];
        size_t n;
        while ((n = fread(buf, 1, sizeof(buf), f)) > 0) {
            // Count bits for this block
            for (size_t i = 0; i < n; ++i) {
                unsigned char b = buf[i];
                // For each bit position, add 1 if set
                for (int bit = 0; bit < 8; ++bit) {
                    local_hist[bit] += (b >> bit) & 1u;
                }
            }
            local_bytes_since_merge += n;

            // Merge occasionally so UI stays responsive
            if (local_bytes_since_merge >= 32768) {
                pthread_mutex_lock(&g_hist_mutex);
                for (int bit = 0; bit < 8; ++bit) {
                    g_hist[bit] += local_hist[bit];
                    local_hist[bit] = 0;
                }
                g_total_bytes += local_bytes_since_merge;
                local_bytes_since_merge = 0;

                if (g_total_bytes - g_last_ui_bytes >= PRINT_STEP) {
                    g_last_ui_bytes = g_total_bytes;
                    // Print a consistent snapshot
                    ui_print_locked();
                }
                pthread_mutex_unlock(&g_hist_mutex);
            }
        }
        fclose(f);

        // Final merge for leftovers
        pthread_mutex_lock(&g_hist_mutex);
        for (int bit = 0; bit < 8; ++bit) {
            g_hist[bit] += local_hist[bit];
        }
        g_total_bytes += local_bytes_since_merge;

        if (g_total_bytes - g_last_ui_bytes >= PRINT_STEP) {
            g_last_ui_bytes = g_total_bytes;
            ui_print_locked();
        }
        pthread_mutex_unlock(&g_hist_mutex);

        free(filepath);
    }

    return NULL;
}

// ---------- Main ----------

int main(int argc, char * const *argv) {
    if (argc < 2) {
        err(1, "usage: [-n N] paths...");
    }

    int num_threads = 1;
    char * const *paths = &argv[1];

    if (argc > 3 && strcmp(argv[1], "-n") == 0) {
        num_threads = atoi(argv[2]);
        if (num_threads < 1) {
            err(1, "invalid thread count: %s", argv[2]);
        }
        paths = &argv[3];
    }

    // Init job queue
    struct job_queue jq;
    if (job_queue_init(&jq, 64) != 0) {
        err(1, "job_queue_init failed");
    }

    // Start workers
    pthread_t *threads = calloc(num_threads, sizeof(pthread_t));
    if (!threads) {
        err(1, "failed to allocate thread array");
    }
    for (int i = 0; i < num_threads; ++i) {
        if (pthread_create(&threads[i], NULL, worker_fn, &jq) != 0) {
            err(1, "pthread_create failed");
        }
    }

    // Walk the file tree and enqueue regular files
    int fts_flags = FTS_LOGICAL | FTS_NOCHDIR;
    FTS *ftsp = fts_open(paths, fts_flags, NULL);
    if (!ftsp) {
        job_queue_destroy(&jq);
        err(1, "fts_open failed");
    }

    FTSENT *ent;
    while ((ent = fts_read(ftsp)) != NULL) {
        if (ent->fts_info == FTS_F) {
            char *path_copy = strdup(ent->fts_path);
            if (!path_copy) {
                fts_close(ftsp);
                job_queue_destroy(&jq);
                err(1, "out of memory duplicating path");
            }
            if (job_queue_push(&jq, path_copy) != 0) {
                free(path_copy);
                fts_close(ftsp);
                job_queue_destroy(&jq);
                err(1, "job_queue_push failed");
            }
        }
    }
    fts_close(ftsp);

    // No more jobs; signal workers to finish when queue drains
    job_queue_destroy(&jq);

    // Wait for all workers
    for (int i = 0; i < num_threads; ++i) {
        if (pthread_join(threads[i], NULL) != 0) {
            err(1, "pthread_join failed");
        }
    }
    free(threads);

    // Final print leaves the result visible on screen
    pthread_mutex_lock(&stdout_mutex);
    print_histogram(g_hist);
    fflush(stdout);
    // Do not call move_lines(9) here
    pthread_mutex_unlock(&stdout_mutex);

    return 0;
}
