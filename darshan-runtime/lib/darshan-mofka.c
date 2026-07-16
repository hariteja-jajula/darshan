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

#include <stdatomic.h>
#include <diaspora/diaspora_c.h>

struct darshanMofkaConnector mC = {
    .mofka_lib = 1,
};

static diaspora_driver_t*   g_driver;
static diaspora_topic_t*    g_topic;
static diaspora_producer_t* g_producer;

static atomic_ullong g_seq;
static __thread int  g_in_send;

static char    g_hostname[256];
static long    g_pid;
static int64_t g_uid   = -1;
static int64_t g_jobid = -1;
static double  g_t0_epoch;

#define MOFKA_JSON_BUF 8192
#define MOFKA_BATCH_ADAPTIVE ((size_t)0)
#define MOFKA_BATCH_SIZE     MOFKA_BATCH_ADAPTIVE

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void mofka_took(const char* fn, uint64_t t0)
{
    if (getenv("DARSHAN_MOFKA_TIMING"))
        darshan_core_fprintf(stderr, "darshan-mofka[timing] %s %.3f us\n", fn, (now_ns() - t0) / 1e3);
}

static void json_escape_into(char* dst, size_t dstsz, const char* src)
{
    size_t o = 0;
    if (dstsz == 0) return;
    if (src == NULL) src = "unknown";
    for (; *src && o + 2 < dstsz; src++) {
        unsigned char c = (unsigned char)*src;
        if (c == '"' || c == '\\') { dst[o++] = '\\'; dst[o++] = (char)c; }
        else if (c < 0x20)         { dst[o++] = '?'; }
        else                       { dst[o++] = (char)c; }
    }
    dst[o] = '\0';
}

static void hex_into(char* dst, size_t dstsz, const void* src, uint64_t n)
{
    static const char H[] = "0123456789abcdef";
    const unsigned char* p = (const unsigned char*)src;
    size_t o = 0; uint64_t i;
    if (dstsz == 0) return;
    for (i = 0; i < n && o + 2 < dstsz; i++) {
        dst[o++] = H[p[i] >> 4]; dst[o++] = H[p[i] & 0xf];
    }
    dst[o] = '\0';
}

void darshan_mofka_connector_initialize(struct darshan_core_runtime* init_core)
{
    const char* group_file;
    const char* topic_name;
    char opts[1200];
    char gf_esc[1024];
    char pname[64];
    uint64_t t0 = now_ns();

    g_pid = (long)(init_core ? init_core->pid : getpid());

    if (gethostname(g_hostname, sizeof(g_hostname)) != 0)
        snprintf(g_hostname, sizeof(g_hostname), "unknown");
    g_hostname[sizeof(g_hostname) - 1] = '\0';

    g_t0_epoch = darshan_core_wtime_absolute();

    if (init_core && init_core->log_job_p) {
        g_uid   = (int64_t)init_core->log_job_p->uid;
        g_jobid = (int64_t)init_core->log_job_p->jobid;
    }

    group_file = getenv("DARSHAN_MOFKA_GROUP_FILE");
    topic_name = getenv("DARSHAN_MOFKA_TOPIC");
    if (topic_name == NULL || *topic_name == '\0') topic_name = "darshan";

    size_t batch_size = MOFKA_BATCH_SIZE, max_batches = 0;
    { const char* e;
      if ((e = getenv("DARSHAN_MOFKA_BATCH"))       && *e) batch_size  = (size_t)strtoull(e, NULL, 10);
      if ((e = getenv("DARSHAN_MOFKA_MAX_BATCHES"))  && *e) max_batches = (size_t)strtoull(e, NULL, 10); }

    if (group_file == NULL || *group_file == '\0') {
        darshan_core_fprintf(stderr, "darshan-mofka: DARSHAN_MOFKA_GROUP_FILE not set; "
                "records will not be streamed.\n");
        return;
    }

    json_escape_into(gf_esc, sizeof(gf_esc), group_file);
    snprintf(opts, sizeof(opts), "{\"group_file\":\"%s\"}", gf_esc);

    g_driver = diaspora_driver_create("mofka", opts);
    if (g_driver == NULL) {
        darshan_core_fprintf(stderr, "darshan-mofka: driver_create failed (%s)\n",
                diaspora_c_last_error());
        return;
    }

    g_topic = diaspora_topic_open(g_driver, topic_name);
    if (g_topic == NULL) {
        darshan_core_fprintf(stderr, "darshan-mofka: topic_open('%s') failed (%s)\n",
                topic_name, diaspora_c_last_error());
        diaspora_driver_destroy(g_driver); g_driver = NULL;
        return;
    }

    snprintf(pname, sizeof(pname), "darshan-%ld", g_pid);
    g_producer = diaspora_producer_create(g_topic, pname, batch_size, max_batches,
                                          DIASPORA_C_ORDERING_LOOSE);
    if (g_producer == NULL) {
        darshan_core_fprintf(stderr, "darshan-mofka: producer_create failed (%s)\n",
                diaspora_c_last_error());
        diaspora_topic_destroy(g_topic);   g_topic = NULL;
        diaspora_driver_destroy(g_driver); g_driver = NULL;
        return;
    }

    if (getenv("DARSHAN_MOFKA_VERBOSE"))
        darshan_core_fprintf(stderr, "darshan-mofka: producer connected to topic '%s' "
                "(batch_size=%zu max_num_batches=%zu)\n",
                topic_name, batch_size, max_batches);

    mofka_took("initialize", t0);
}

void darshan_mofka_connector_send(uint64_t record_id, int64_t rank,
                                  int64_t record_count, char* rwo,
                                  int64_t offset, int64_t length,
                                  int64_t max_byte, int64_t rw_switch,
                                  int64_t flushes,
                                  double start_time, double end_time,
                                  double total_time,
                                  char* mod_name, char* data_type,
                                  const void* rec, uint64_t rec_size)
{
    char buf[MOFKA_JSON_BUF];
    char file_esc[1024];
    char host_esc[300];
    const char* file_path;
    unsigned long long seq;
    uint64_t t0;
    int n;
    double started_epoch, ended_epoch;
    char rec_hex[4096];

    if (g_in_send) return;
    g_in_send = 1;
    t0 = now_ns();

    if (g_producer == NULL) goto out;

    file_path = (const char*)darshan_core_lookup_record_name(record_id);
    json_escape_into(file_esc, sizeof(file_esc), file_path);
    json_escape_into(host_esc, sizeof(host_esc), g_hostname);
    seq = (unsigned long long)atomic_fetch_add(&g_seq, 1);

    { struct timespec s = darshan_core_abs_timespec_from_wtime(start_time);
      struct timespec e = darshan_core_abs_timespec_from_wtime(end_time);
      started_epoch = (double)s.tv_sec + (double)s.tv_nsec / 1e9;
      ended_epoch   = (double)e.tv_sec + (double)e.tv_nsec / 1e9; }

    rec_hex[0] = '\0';
    if (rec != NULL && rec_size > 0)
        hex_into(rec_hex, sizeof(rec_hex), rec, rec_size);

    n = snprintf(buf, sizeof(buf),
        "{\"type\":\"task\","
        "\"activity_id\":\"darshan_%s\","
        "\"task_id\":\"darshan-%016llx-%ld-%llu\","
        "\"schema\":\"darshan_runtime\",\"schema_version\":2,"
        "\"module\":\"%s\",\"event_type\":\"%s\",\"op\":\"%s\","
        "\"record_id\":\"%016llx\",\"file\":\"%s\","
        "\"hostname\":\"%s\",\"pid\":%ld,\"uid\":%lld,\"job_id\":%lld,"
        "\"rank\":%lld,\"seq\":%llu,\"t0_epoch\":%.6f,"
        "\"cnt\":%lld,\"off\":%lld,\"len\":%lld,\"max_byte\":%lld,"
        "\"switches\":%lld,\"flushes\":%lld,"
        "\"started_at\":%.6f,\"ended_at\":%.6f,\"dur\":%.6f,\"total\":%.6f,"
        "\"rec_size\":%llu,\"rec_hex\":\"%s\"}",
        mod_name ? mod_name : "?",
        (unsigned long long)record_id, g_pid, seq,
        mod_name ? mod_name : "?",
        data_type ? data_type : "?",
        rwo ? rwo : "?",
        (unsigned long long)record_id, file_esc,
        host_esc, g_pid, (long long)g_uid, (long long)g_jobid,
        (long long)rank, seq, g_t0_epoch,
        (long long)record_count, (long long)offset, (long long)length,
        (long long)max_byte, (long long)rw_switch, (long long)flushes,
        started_epoch, ended_epoch, end_time - start_time, total_time,
        (unsigned long long)rec_size, rec_hex);

    if (n < 0 || (size_t)n >= sizeof(buf)) goto out;

    if (diaspora_producer_push(g_producer, buf, NULL, 0) != DIASPORA_C_OK)
        darshan_core_fprintf(stderr, "darshan-mofka: push failed (%s)\n",
                diaspora_c_last_error());

out:
    mofka_took("send", t0);
    g_in_send = 0;
}

void darshan_mofka_connector_finalize(void)
{
    int rc;
    uint64_t t0;

    if (g_producer == NULL) goto clear;

    t0 = now_ns();
    { const char* fe = getenv("DARSHAN_MOFKA_FLUSH_MS");
      unsigned flush_ms = (fe && *fe) ? (unsigned)strtoul(fe, NULL, 10) : 5000;
      rc = diaspora_producer_flush_timeout(g_producer, flush_ms); }
    if (rc == DIASPORA_C_TIMEOUT)
        darshan_core_fprintf(stderr, "darshan-mofka: flush timed out; leaking handles.\n");
    else if (rc == DIASPORA_C_ERR)
        darshan_core_fprintf(stderr, "darshan-mofka: flush error (%s)\n",
                diaspora_c_last_error());
    mofka_took("finalize", t0);

clear:
    g_producer = NULL;
    g_topic = NULL;
    g_driver = NULL;
}

#else

struct darshanMofkaConnector mC = {
    .mofka_lib = 0,
};

void darshan_mofka_connector_initialize(struct darshan_core_runtime* init_core)
{
    (void)init_core;
}

void darshan_mofka_connector_send(uint64_t record_id, int64_t rank,
                                  int64_t record_count, char* rwo,
                                  int64_t offset, int64_t length,
                                  int64_t max_byte, int64_t rw_switch,
                                  int64_t flushes,
                                  double start_time, double end_time,
                                  double total_time,
                                  char* mod_name, char* data_type,
                                  const void* rec, uint64_t rec_size)
{
    (void)record_id; (void)rank;       (void)record_count; (void)rwo;
    (void)offset;    (void)length;     (void)max_byte;     (void)rw_switch;
    (void)flushes;   (void)start_time; (void)end_time;     (void)total_time;
    (void)mod_name;  (void)data_type;  (void)rec;          (void)rec_size;
}

void darshan_mofka_connector_finalize(void)
{
}

#endif /* HAVE_MOFKA */
