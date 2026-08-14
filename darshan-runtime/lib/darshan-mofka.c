/*
 * (C) 2026 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#ifdef HAVE_CONFIG_H
# include <darshan-runtime-config.h>
#endif

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>

#include "darshan-mofka.h"
#include "darshan.h"

#ifdef HAVE_MOFKA

#include <diaspora/diaspora_c.h>

/* -------------------------------------------------------------------------- */
/* Configuration                                                              */
/* -------------------------------------------------------------------------- */

#define MOFKA_MAX_PUSH_TIMES 100000

/* -------------------------------------------------------------------------- */
/* Global Mofka/Diaspora objects                                              */
/* -------------------------------------------------------------------------- */

static diaspora_driver_t   *g_driver   = NULL;
static diaspora_topic_t    *g_topic    = NULL;
static diaspora_producer_t *g_producer = NULL;

/* Timing accumulators */
static atomic_ullong g_init_ns;
static atomic_ullong g_push_ns;
static atomic_ullong g_push_n;

/*
 * Individual push durations.
 *
 * Each send atomically reserves a unique index before storing its duration.
 * Printing happens only at finalize.
 */
static unsigned long long g_push_times_ns[MOFKA_MAX_PUSH_TIMES];

/* -------------------------------------------------------------------------- */
/* Initialize                                                                 */
/* -------------------------------------------------------------------------- */

void darshan_mofka_connector_initialize(
    struct darshan_core_runtime *init_core)
{
    const char *group_file;
    const char *topic_name;
    const char *batch_env;
    const char *max_batches_env;

    size_t batch_size = 0;
    size_t max_batches = 0;

    char opts[4096];
    char producer_name[64];

    double init_t0;
    double init_elapsed;

    /*
     * errno transparency (see darshan_mofka_connector_send): Darshan initializes
     * lazily from inside the first intercepted I/O wrapper, so this runs in the
     * application's syscall context. diaspora_driver_create/topic_open/
     * producer_create issue syscalls that clobber errno; snapshot and restore.
     */
    int saved_errno = errno;

    (void)init_core;

    /*
     * Measure complete connector initialization.
     */
    init_t0 = darshan_core_wtime();

    /*
     * Required Mofka group file.
     */
    group_file = getenv("DARSHAN_MOFKA_GROUP_FILE");

    if (group_file == NULL || *group_file == '\0')
    {
        darshan_core_fprintf(
            stderr,
            "darshan-mofka: DARSHAN_MOFKA_GROUP_FILE not set\n");

        errno = saved_errno;
        return;
    }

    topic_name = getenv("DARSHAN_MOFKA_TOPIC");

    if (topic_name == NULL || *topic_name == '\0')
        topic_name = "darshan";

    batch_env = getenv("DARSHAN_MOFKA_BATCH");

    if (batch_env != NULL && *batch_env != '\0')
        batch_size = (size_t)strtoull(batch_env, NULL, 10);

    max_batches_env = getenv("DARSHAN_MOFKA_MAX_BATCHES");

    if (max_batches_env != NULL && *max_batches_env != '\0')
        max_batches = (size_t)strtoull(max_batches_env, NULL, 10);

    snprintf(
        opts,
        sizeof(opts),
        "{\"group_file\":\"%s\"}",
        group_file);

    g_driver = diaspora_driver_create(
        "mofka",
        opts);

    if (g_driver == NULL)
    {
        darshan_core_fprintf(
            stderr,
            "darshan-mofka: driver_create failed (%s)\n",
            diaspora_c_last_error());

        errno = saved_errno;
        return;
    }

    g_topic = diaspora_topic_open(
        g_driver,
        topic_name);

    if (g_topic == NULL)
    {
        darshan_core_fprintf(
            stderr,
            "darshan-mofka: topic_open failed (%s)\n",
            diaspora_c_last_error());

        diaspora_driver_destroy(g_driver);
        g_driver = NULL;

        errno = saved_errno;
        return;
    }

    /*
     * Keep one sender thread to avoid unnecessary thread oversubscription.
     */
    setenv("DIASPORA_C_SENDER_THREADS", "1", 0);

    snprintf(
        producer_name,
        sizeof(producer_name),
        "darshan-%ld",
        (long)getpid());

    g_producer = diaspora_producer_create(
        g_topic,
        producer_name,
        batch_size,
        max_batches,
        DIASPORA_C_ORDERING_LOOSE);

    if (g_producer == NULL)
    {
        darshan_core_fprintf(
            stderr,
            "darshan-mofka: producer_create failed (%s)\n",
            diaspora_c_last_error());

        diaspora_topic_destroy(g_topic);
        g_topic = NULL;

        diaspora_driver_destroy(g_driver);
        g_driver = NULL;

        errno = saved_errno;
        return;
    }

    /*
     * Stop initialization timer only after producer creation succeeds.
     */
    init_elapsed =
        darshan_core_wtime() - init_t0;

    atomic_store(
        &g_init_ns,
        (unsigned long long)(init_elapsed * 1e9));

    errno = saved_errno;
}

/* -------------------------------------------------------------------------- */
/* Send                                                                       */
/* -------------------------------------------------------------------------- */

void darshan_mofka_connector_send(
    uint64_t record_id,
    int64_t rank,
    int64_t record_count,
    char *rwo,
    int64_t offset,
    int64_t length,
    int64_t max_byte,
    int64_t rw_switch,
    int64_t flushes,
    double start_time,
    double end_time,
    double total_time,
    char *mod_name,
    char *data_type,
    const void *rec,
    uint64_t rec_size)
{
    char buf[2048];

    struct timespec ts;
    struct timespec te;

    double started_epoch;
    double ended_epoch;

    double push_t0;
    double push_elapsed;

    unsigned long long elapsed_ns;
    unsigned long long idx;

    int n;

    /*
     * errno transparency. This function runs SYNCHRONOUSLY inside Darshan's
     * intercepted POSIX/STDIO/MPIIO/HDF5 wrappers (open/read/write/close/...),
     * i.e. between the real syscall and the instrumented application's own
     * errno check. diaspora_producer_push() and darshan_core_wtime() issue
     * their own syscalls (Mofka/margo/mercury/libfabric progress, clock_gettime)
     * that overwrite errno. Without saving and restoring it, a successful
     * app-level open() can return with errno==0 clobbered to some transient
     * value -- or, worse, a genuinely-set errno wiped -- corrupting the app's
     * error handling. This concretely broke CPython startup (<frozen getpath>:
     * "OSError: [Errno 0] Error"). Snapshot on entry, restore on EVERY exit.
     */
    int saved_errno = errno;

    (void)record_count;
    (void)offset;
    (void)max_byte;
    (void)rw_switch;
    (void)flushes;
    (void)total_time;
    (void)data_type;
    (void)rec;
    (void)rec_size;

    if (g_producer == NULL)
    {
        errno = saved_errno;
        return;
    }

    /*
     * Convert Darshan relative wtime values to absolute timestamps.
     */
    ts =
        darshan_core_abs_timespec_from_wtime(start_time);

    te =
        darshan_core_abs_timespec_from_wtime(end_time);

    started_epoch =
        (double)ts.tv_sec +
        (double)ts.tv_nsec / 1e9;

    ended_epoch =
        (double)te.tv_sec +
        (double)te.tv_nsec / 1e9;


    n = snprintf(
        buf,
        sizeof(buf),
        "{"
        "\"type\":\"task\","
        "\"schema\":\"darshan_runtime\","
        "\"schema_version\":2,"
        "\"module\":\"%s\","
        "\"op\":\"%s\","
        "\"record_id\":\"%016llx\","
        "\"rank\":%lld,"
        "\"len\":%lld,"
        "\"started_at\":%.6f,"
        "\"ended_at\":%.6f"
        "}",
        mod_name ? mod_name : "?",
        rwo ? rwo : "?",
        (unsigned long long)record_id,
        (long long)rank,
        (long long)length,
        started_epoch,
        ended_epoch);

    if (n < 0 || (size_t)n >= sizeof(buf))
    {
        errno = saved_errno;
        return;
    }

    /*
     * Measure ONLY diaspora_producer_push().
     */
    push_t0 =
        darshan_core_wtime();

    diaspora_producer_push(
        g_producer,
        buf,
        NULL,
        0);

    push_elapsed =
        darshan_core_wtime() - push_t0;

    elapsed_ns =
        (unsigned long long)(push_elapsed * 1e9);

    /*
     * Reserve a unique index and increment total push count.
     */
    idx =
        atomic_fetch_add(
            &g_push_n,
            1);

    /*
     * Store individual timing if space is available.
     *
     * This happens after the second timer call, so it is not included
     * in the measured diaspora_producer_push() duration.
     */
    if (idx < MOFKA_MAX_PUSH_TIMES)
    {
        g_push_times_ns[idx] =
            elapsed_ns;
    }

    /*
     * Maintain aggregate push time.
     */
    atomic_fetch_add(
        &g_push_ns,
        elapsed_ns);

    /*
     * Restore the application's errno (see entry note): the push path above
     * issued syscalls that clobbered it.
     */
    errno = saved_errno;
}

/* -------------------------------------------------------------------------- */
/* Finalize                                                                   */
/* -------------------------------------------------------------------------- */

void darshan_mofka_connector_finalize(void)
{
    unsigned long long init_ns;
    unsigned long long push_ns;
    unsigned long long pushes;
    unsigned long long stored_pushes;
    unsigned long long i;

    double init_us;
    double push_total_us;
    double push_avg_us;

    /*
     * errno transparency (see darshan_mofka_connector_send): the flush/destroy
     * calls below issue syscalls that clobber errno. Darshan shutdown can run
     * from an application atexit context, so preserve it.
     */
    int saved_errno = errno;

    /*
     * Flush outstanding producer events before destroying Mofka objects.
     */
    if (g_producer != NULL)
    {
        diaspora_producer_flush_timeout(
            g_producer,
            5000);
    }

    /*
     * Load aggregate timing measurements.
     */
    init_ns =
        atomic_load(&g_init_ns);

    push_ns =
        atomic_load(&g_push_ns);

    pushes =
        atomic_load(&g_push_n);

    init_us =
        (double)init_ns / 1e3;

    push_total_us =
        (double)push_ns / 1e3;

    push_avg_us =
        pushes
            ? push_total_us / (double)pushes
            : 0.0;

    /*
     * Print aggregate timing.
     */
    darshan_core_fprintf(
        stderr,
        "darshan-mofka TIMING "
        "init_us=%.3f "
        "pushes=%llu "
        "push_total_us=%.3f "
        "push_avg_us=%.3f\n",
        init_us,
        pushes,
        push_total_us,
        push_avg_us);

    /*
     * Print individual push timings.
     *
     * These were collected in memory during execution, so this printing
     * does not affect the measured duration of each push.
     */
    stored_pushes =
        pushes < MOFKA_MAX_PUSH_TIMES
            ? pushes
            : MOFKA_MAX_PUSH_TIMES;

    for (i = 0; i < stored_pushes; i++)
    {
        darshan_core_fprintf(
            stderr,
            "darshan-mofka PUSH "
            "index=%llu "
            "push_us=%.3f\n",
            i,
            (double)g_push_times_ns[i] / 1e3);
    }

    /*
     * Warn if more pushes occurred than we had storage for.
     */
    if (pushes > MOFKA_MAX_PUSH_TIMES)
    {
        darshan_core_fprintf(
            stderr,
            "darshan-mofka: WARNING "
            "pushes=%llu but only first %d individual timings stored\n",
            pushes,
            MOFKA_MAX_PUSH_TIMES);
    }

    /*
     * Destroy Mofka objects.
     */
    if (g_producer != NULL)
    {
        diaspora_producer_destroy(g_producer);
        g_producer = NULL;
    }

    if (g_topic != NULL)
    {
        diaspora_topic_destroy(g_topic);
        g_topic = NULL;
    }

    if (g_driver != NULL)
    {
        diaspora_driver_destroy(g_driver);
        g_driver = NULL;
    }

    errno = saved_errno;
}

#else  /* !HAVE_MOFKA */

/* -------------------------------------------------------------------------- */
/* No-op stubs when Mofka support is disabled                                 */
/* -------------------------------------------------------------------------- */

void darshan_mofka_connector_initialize(
    struct darshan_core_runtime *init_core)
{
    (void)init_core;
}

void darshan_mofka_connector_send(
    uint64_t record_id,
    int64_t rank,
    int64_t record_count,
    char *rwo,
    int64_t offset,
    int64_t length,
    int64_t max_byte,
    int64_t rw_switch,
    int64_t flushes,
    double start_time,
    double end_time,
    double total_time,
    char *mod_name,
    char *data_type,
    const void *rec,
    uint64_t rec_size)
{
    (void)record_id;
    (void)rank;
    (void)record_count;
    (void)rwo;
    (void)offset;
    (void)length;
    (void)max_byte;
    (void)rw_switch;
    (void)flushes;
    (void)start_time;
    (void)end_time;
    (void)total_time;
    (void)mod_name;
    (void)data_type;
    (void)rec;
    (void)rec_size;
}

void darshan_mofka_connector_finalize(void)
{
}

#endif