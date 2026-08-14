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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>

#include "darshan-mofka.h"
#include "darshan.h"

#ifdef HAVE_MOFKA

#include <diaspora/diaspora_c.h>

/* -------------------------------------------------------------------------- */
/* Global Mofka/Diaspora objects                                              */
/* -------------------------------------------------------------------------- */

static diaspora_driver_t   *g_driver   = NULL;
static diaspora_topic_t    *g_topic    = NULL;
static diaspora_producer_t *g_producer = NULL;

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

    (void)init_core;

    group_file = getenv("DARSHAN_MOFKA_GROUP_FILE");

    if (group_file == NULL || *group_file == '\0')
    {
        darshan_core_fprintf(
            stderr,
            "darshan-mofka: DARSHAN_MOFKA_GROUP_FILE not set\n");

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

        return;
    }
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

    int n;

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
        return;

    /*
     * Convert Darshan relative timestamps to absolute epoch timestamps.
     */
    ts = darshan_core_abs_timespec_from_wtime(start_time);
    te = darshan_core_abs_timespec_from_wtime(end_time);

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
        return;


    diaspora_producer_push(
        g_producer,
        buf,
        NULL,
        0);
}

/* -------------------------------------------------------------------------- */
/* Finalize                                                                   */
/* -------------------------------------------------------------------------- */

void darshan_mofka_connector_finalize(void)
{
    if (g_producer != NULL)
    {

        diaspora_producer_flush_timeout(
            g_producer,
            5000);

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