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

static diaspora_driver_t*   g_driver;
static diaspora_topic_t*    g_topic;
static diaspora_producer_t* g_producer;

static atomic_ullong g_seq;
static __thread int  g_in_send;

/* Aggregate producer-push timing (the ONE number Phase 1 reports at finalize):
 * summed wall-clock ns spent inside diaspora_producer_push() and the push count.
 * Only accumulated when DARSHAN_MOFKA_TIMING is set, so the production hot path
 * pays nothing. */
static atomic_ullong g_push_ns;
static atomic_ullong g_push_n;

/* Field bundle for one I/O event, filled on the app thread and passed straight to
 * mofka_serialize_and_push() -> diaspora_producer_push() (no ring, no drain thread). */
struct mofka_slot {
    uint64_t record_id;
    int64_t  rank, record_count, offset, length, max_byte, rw_switch, flushes;
    double   start_time, end_time, total_time;
    unsigned long long seq;
    const char *rwo;
    const char *mod_name;
    const char *data_type;
    char     file_esc[1024];
    void   *snap_buf;
    size_t  snap_size;
    int     snap_mod;      /* module id of the snapshot (for counter-count lookup) */
};

static void mofka_record_dims(int mod_id, int *nctr, int *nfctr)
{
    switch (mod_id) {
        case DARSHAN_POSIX_MOD: *nctr = POSIX_NUM_INDICES; *nfctr = POSIX_F_NUM_INDICES; break;
        case DARSHAN_STDIO_MOD: *nctr = STDIO_NUM_INDICES; *nfctr = STDIO_F_NUM_INDICES; break;
        default:                *nctr = 0;                 *nfctr = 0;                   break;
    }
}

static int mofka_mod_id(const char *mod_name)
{
    if (mod_name == NULL) return -1;
    if (strcmp(mod_name, "POSIX") == 0) return DARSHAN_POSIX_MOD;
    if (strcmp(mod_name, "STDIO") == 0) return DARSHAN_STDIO_MOD;
    return -1;
}

static char    g_hostname[256];
static long    g_pid;
static int64_t g_uid   = -1;
static int64_t g_jobid = -1;
static double  g_t0_epoch;
static int     g_timing;
static int64_t g_launcher_rank = -1;

static const char *g_exemnt = NULL;
static char        g_host_esc[300];
static atomic_int  g_meta_sent;

#define MOFKA_JSON_BUF 8192

#define MOFKA_ENV_HEAD   "{\"type\":\"task\","
#define MOFKA_ENV_SCHEMA "\"schema\":\"darshan_runtime\",\"schema_version\":2,"
#define MOFKA_ENV_IDENT  "\"hostname\":\"%s\",\"pid\":%ld,\"uid\":%lld,\"job_id\":%lld,"

/* ------------------------------------------------------------------ *
 *  Helpers                                                           *
 * ------------------------------------------------------------------ */

/* Emit a per-call timing line when DARSHAN_MOFKA_TIMING is set. */
static void mofka_took(const char* fn, double t0)
{
    if (g_timing)
        darshan_core_fprintf(stderr, "darshan-mofka[timing] %s %.3f us\n",
            fn, (darshan_core_wtime() - t0) * 1e6);
}

/* Copy src into dst, escaping JSON-unsafe bytes; always NUL-terminates. */
static void json_escape_into(char* dst, size_t dstsz, const char* src)
{
    size_t o = 0;
    if (dstsz == 0) return;
    if (src == NULL) src = "unknown";
    for (; *src && o + 2 < dstsz; src++) {
        unsigned char c = (unsigned char)*src;
        if (c == '"' || c == '\\') { dst[o++] = '\\'; dst[o++] = (char)c; }
        else if (c == '\n')        { dst[o++] = '\\'; dst[o++] = 'n'; }
        else if (c == '\t')        { dst[o++] = '\\'; dst[o++] = 't'; }
        else if (c == '\r')        { dst[o++] = '\\'; dst[o++] = 'r'; }
        else if (c < 0x20)         { dst[o++] = '?'; }
        else                       { dst[o++] = (char)c; }
    }
    dst[o] = '\0';
}

/* Push the one-shot job metadata event (exe + mounts, no module record). */
static void emit_metadata(void)
{
    char buf[9216];
    char exemnt_esc[8192];
    int n;

    if (g_producer == NULL) return;

    json_escape_into(exemnt_esc, sizeof(exemnt_esc), g_exemnt);

    n = snprintf(buf, sizeof(buf),
        MOFKA_ENV_HEAD
        "\"activity_id\":\"darshan_meta\","
        "\"task_id\":\"darshan-meta-%ld-%lld\","
        MOFKA_ENV_SCHEMA
        "\"event_type\":\"metadata\","
        MOFKA_ENV_IDENT
        "\"t0_epoch\":%.6f,\"exemnt\":\"%s\"}",
        g_pid, (long long)g_jobid,
        g_host_esc, g_pid, (long long)g_uid, (long long)g_jobid,
        g_t0_epoch, exemnt_esc);

    if (n < 0 || (size_t)n >= sizeof(buf)) return;

    if (diaspora_producer_push(g_producer, buf, NULL, 0) != DIASPORA_C_OK)
        darshan_core_fprintf(stderr, "darshan-mofka: metadata push failed (%s)\n",
                diaspora_c_last_error());
}

/* Emit metadata exactly once across all threads (atomic CAS guard). */
static void mofka_emit_metadata_once(void)
{
    int expected = 0;
    if (atomic_compare_exchange_strong(&g_meta_sent, &expected, 1))
        emit_metadata();
}

/* ------------------------------------------------------------------ *
 *  Direct push (the entire hot path -- no ring, no drain thread)      *
 * ------------------------------------------------------------------ */

/* Build the JSON envelope from a snapshot and push it (all the CPU cost). */
static void mofka_serialize_and_push(const struct mofka_slot* s)
{
    char buf[MOFKA_JSON_BUF];
    double started_epoch, ended_epoch;
    int n;

    { struct timespec ts = darshan_core_abs_timespec_from_wtime(s->start_time);
      struct timespec te = darshan_core_abs_timespec_from_wtime(s->end_time);
      started_epoch = (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
      ended_epoch   = (double)te.tv_sec + (double)te.tv_nsec / 1e9; }


    n = snprintf(buf, sizeof(buf),
        MOFKA_ENV_HEAD
        MOFKA_ENV_SCHEMA
        "\"module\":\"%s\",\"op\":\"%s\","
        "\"record_id\":\"%016llx\",\"file\":\"%s\",\"pid\":%ld,"
        "\"rank\":%lld,\"seq\":%llu,"
        "\"len\":%lld,\"started_at\":%.6f,\"ended_at\":%.6f",
        s->mod_name ? s->mod_name : "?",
        s->rwo ? s->rwo : "?",
        (unsigned long long)s->record_id, s->file_esc, g_pid,
        (long long)(g_launcher_rank >= 0 ? g_launcher_rank : s->rank), s->seq,
        (long long)s->length, started_epoch, ended_epoch);

    if (n < 0 || (size_t)n >= sizeof(buf)) return;

    if (s->snap_buf) {
        int nctr = 0, nfctr = 0, i;
        mofka_record_dims(s->snap_mod, &nctr, &nfctr);
        if (nctr > 0 &&
            s->snap_size >= sizeof(struct darshan_base_record)
                            + (size_t)nctr * sizeof(int64_t)
                            + (size_t)nfctr * sizeof(double)) {
            const char *base = (const char*)s->snap_buf
                             + sizeof(struct darshan_base_record);
            const int64_t *ctr  = (const int64_t*)base;
            const double  *fctr = (const double*)(base + (size_t)nctr * sizeof(int64_t));
            int m = snprintf(buf + n, sizeof(buf) - (size_t)n, ",\"counters\":[");
            if (m < 0 || (size_t)(n + m) >= sizeof(buf)) return;
            n += m;
            for (i = 0; i < nctr; i++) {
                m = snprintf(buf + n, sizeof(buf) - (size_t)n, "%s%lld",
                             i ? "," : "", (long long)ctr[i]);
                if (m < 0 || (size_t)(n + m) >= sizeof(buf)) return;
                n += m;
            }
            m = snprintf(buf + n, sizeof(buf) - (size_t)n, "],\"fcounters\":[");
            if (m < 0 || (size_t)(n + m) >= sizeof(buf)) return;
            n += m;
            for (i = 0; i < nfctr; i++) {
                m = snprintf(buf + n, sizeof(buf) - (size_t)n, "%s%.6f",
                             i ? "," : "", fctr[i]);
                if (m < 0 || (size_t)(n + m) >= sizeof(buf)) return;
                n += m;
            }
            m = snprintf(buf + n, sizeof(buf) - (size_t)n, "]");
            if (m < 0 || (size_t)(n + m) >= sizeof(buf)) return;
            n += m;
        }
    }

    /* close the JSON object */
    if ((size_t)n + 1 >= sizeof(buf)) return;
    buf[n++] = '}';
    buf[n]   = '\0';

    /* Direct push -- the entire cost of the connector's hot path is on this one
     * line. diaspora_producer_push() is made ABT-safe from this raw app thread by
     * forcing DIASPORA_C_SENDER_THREADS>=1 at init, so the enqueue runs as a
     * self-contained ULT on a dedicated sender ES (it copies buf internally).
     * When timing is enabled we bracket ONLY the push and fold it into the
     * aggregate accumulators reported once at finalize; otherwise the production
     * hot path pays nothing for measurement. */
    if (g_timing) {
        double pt0 = darshan_core_wtime();
        int rc = diaspora_producer_push(g_producer, buf, NULL, 0);
        atomic_fetch_add(&g_push_ns,
            (unsigned long long)((darshan_core_wtime() - pt0) * 1e9));
        atomic_fetch_add(&g_push_n, 1);
        if (rc != DIASPORA_C_OK)
            darshan_core_fprintf(stderr, "darshan-mofka: push failed (%s)\n",
                    diaspora_c_last_error());
    } else if (diaspora_producer_push(g_producer, buf, NULL, 0) != DIASPORA_C_OK) {
        darshan_core_fprintf(stderr, "darshan-mofka: push failed (%s)\n",
                diaspora_c_last_error());
    }
}

/* ------------------------------------------------------------------ *
 *  Lifecycle: initialize -> send -> flush_records -> finalize        *
 * ------------------------------------------------------------------ */

/* Connect the producer. The hot path pushes directly from the app thread; a
 * dedicated Diaspora sender ES (forced on below) makes that push ABT-safe. */
void darshan_mofka_connector_initialize(struct darshan_core_runtime* init_core)
{
    const char* group_file;
    const char* topic_name;
    char opts[4096];
    char gf_esc[1024];
    char pname[64];
    double t0 = darshan_core_wtime();

    { const char* v = getenv("DARSHAN_MOFKA_TIMING");
      g_timing = (v && v[0] && strcmp(v, "0")); }
    g_pid = (long)(init_core ? init_core->pid : getpid());

    { const char* r = getenv("OMPI_COMM_WORLD_RANK");
      if (!r) r = getenv("PMIX_RANK");
      if (!r) r = getenv("PMI_RANK");
      if (r && *r) g_launcher_rank = (int64_t)strtoll(r, NULL, 10); }

    {
        const char* en = getenv("DARSHAN_MOFKA_ENABLE");
        if (en && strcmp(en, "0") == 0) {
            mofka_took("initialize", t0);
            return;
        }
    }

    if (gethostname(g_hostname, sizeof(g_hostname)) != 0)
        snprintf(g_hostname, sizeof(g_hostname), "unknown");
    g_hostname[sizeof(g_hostname) - 1] = '\0';

    json_escape_into(g_host_esc, sizeof(g_host_esc), g_hostname);

    g_t0_epoch = darshan_core_wtime_absolute();
    if (init_core && init_core->log_job_p) {
        g_uid      = (int64_t)init_core->log_job_p->uid;
        g_jobid    = (int64_t)init_core->log_job_p->jobid;
        g_t0_epoch = (double)init_core->log_job_p->start_time_sec
                   + (double)init_core->log_job_p->start_time_nsec / 1e9;
    }
    if (init_core)
        g_exemnt = init_core->log_exemnt_p;

    group_file = getenv("DARSHAN_MOFKA_GROUP_FILE");
    topic_name = getenv("DARSHAN_MOFKA_TOPIC");
    if (topic_name == NULL || *topic_name == '\0') topic_name = "darshan";

    size_t batch_size = 0 , max_batches = 0;
    { const char* e;
      if ((e = getenv("DARSHAN_MOFKA_BATCH"))       && *e) batch_size  = (size_t)strtoull(e, NULL, 10);
      if ((e = getenv("DARSHAN_MOFKA_MAX_BATCHES"))  && *e) max_batches = (size_t)strtoull(e, NULL, 10); }

    if (group_file == NULL || *group_file == '\0') {
        darshan_core_fprintf(stderr, "darshan-mofka: DARSHAN_MOFKA_GROUP_FILE not set; "
                "records will not be streamed.\n");
        return;
    }

    long prog_to = 100, rpc_thr = 0, prog_thread = 1;
    { const char* e;
      if ((e = getenv("DARSHAN_MOFKA_PROGRESS_TIMEOUT_MS")) && *e) prog_to     = strtol(e, NULL, 10);
      if ((e = getenv("DARSHAN_MOFKA_RPC_THREADS"))         && *e) rpc_thr     = strtol(e, NULL, 10);
      if ((e = getenv("DARSHAN_MOFKA_PROGRESS_THREAD"))     && *e) prog_thread = strtol(e, NULL, 10); }
    if (prog_to < 0) prog_to = 0;
    if (rpc_thr < 0) rpc_thr = 0;

    json_escape_into(gf_esc, sizeof(gf_esc), group_file);
    { const char* margo_json = getenv("DARSHAN_MOFKA_MARGO_JSON");
      if (margo_json && *margo_json) {
        /* Verbatim margo object -- caller owns the JSON. */
        snprintf(opts, sizeof(opts),
            "{\"group_file\":\"%s\",\"margo\":%s}", gf_esc, margo_json);
      } else {
        snprintf(opts, sizeof(opts),
            "{\"group_file\":\"%s\","
            "\"margo\":{\"use_progress_thread\":%s,"
            "\"progress_timeout_ub_msec\":%ld,"
            "\"rpc_thread_count\":%ld}}",
            gf_esc, prog_thread ? "true" : "false", prog_to, rpc_thr);
      }
    }
    if (g_timing)
        darshan_core_fprintf(stderr, "darshan-mofka[cfg] margo opts: %s\n", opts);

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

    /* Force a dedicated Diaspora sender xstream (>=1) BEFORE creating the producer.
     * diaspora_producer_create reads DIASPORA_C_SENDER_THREADS: with 0 (the default)
     * the sender runs on Mofka's progress pool and push() takes an Argobots mutex
     * directly on THIS raw pthread -- unsafe, it can wedge the margo scheduler. With
     * >=1 it builds an Argobots ThreadPool, sets abt_safe_push, and routes push
     * through pool.pushWork() (a self-contained ULT that copies our buffer), which is
     * safe to call from the app thread. We only set it if the user has not, so an
     * explicit override still wins. */
    setenv("DIASPORA_C_SENDER_THREADS", "1", 0);

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
                "(batch_size=%zu max_num_batches=%zu, direct push)\n",
                topic_name, batch_size, max_batches);

    mofka_took("initialize", t0);
}

/* Hot path: assign seq, snapshot the op, build the envelope, push it directly.
 * No ring, no drain thread -- the push goes straight to the Diaspora producer
 * (whose own dedicated sender ES does the transport). */
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
    struct mofka_slot ss;
    const char* file_path;
    double t0;

    if (g_producer == NULL || g_in_send) return;
    g_in_send = 1;
    t0 = darshan_core_wtime();

    ss.snap_buf = NULL; ss.snap_size = 0; ss.snap_mod = -1;
    ss.seq = (unsigned long long)atomic_fetch_add(&g_seq, 1);
    file_path = (const char*)darshan_core_lookup_record_name(record_id);

    if (rec != NULL && rec_size > 0 && rwo != NULL && strcmp(rwo, "close") == 0) {
        int mid = mofka_mod_id(mod_name);
        if (mid >= 0) {
            void* cp = malloc(rec_size);
            if (cp != NULL) {
                memcpy(cp, rec, rec_size);
                ss.snap_buf = cp; ss.snap_size = rec_size; ss.snap_mod = mid;
            }
        }
    }

    mofka_emit_metadata_once();
    ss.record_id=record_id; ss.rank=rank; ss.record_count=record_count;
    ss.offset=offset; ss.length=length; ss.max_byte=max_byte;
    ss.rw_switch=rw_switch; ss.flushes=flushes;
    ss.start_time=start_time; ss.end_time=end_time; ss.total_time=total_time;
    ss.rwo=rwo; ss.mod_name=mod_name; ss.data_type=data_type;
    json_escape_into(ss.file_esc, sizeof(ss.file_esc), file_path);
    mofka_serialize_and_push(&ss);
    free(ss.snap_buf);   /* we own the snapshot; push copied what it needed */

    mofka_took("send", t0);
    g_in_send = 0;
}

/* Opt-in (DARSHAN_MOFKA_FINAL_SWEEP=1): re-stream every module's final record at
 * shutdown. OFF by default -- enabling it hangs python-ml (see the guard below). */
void darshan_mofka_connector_flush_records(struct darshan_core_runtime* core)
{
    int m;

    if (g_producer == NULL || core == NULL) return;

    { const char* sw = getenv("DARSHAN_MOFKA_FINAL_SWEEP");
      if (sw == NULL || sw[0] == '\0' || sw[0] == '0') return; }

    for (m = 0; m < DARSHAN_KNOWN_MODULE_COUNT; m++)
    {
        struct darshan_core_module* mod = core->mod_array[m];
        char  *p, *end;
        size_t stride;

        if (mod == NULL) continue;
        stride = mod->rec_size;
        if (stride == 0) continue;

        p   = (char*) mod->rec_buf_start;
        end = (char*) mod->rec_buf_p;
        for (; p && p + stride <= end; p += stride)
        {
            struct darshan_base_record* b = (struct darshan_base_record*) p;
            darshan_mofka_connector_send(
                b->id, b->rank, 0, "FINAL",
                0, 0, 0, 0, 0,
                0.0, 0.0, 0.0,
                (char*) darshan_module_names[m], "final_record",
                p, (uint64_t) stride);
        }
    }
}

/* Flush pending batches, report the aggregate push cost, tear down the producer. */
void darshan_mofka_connector_finalize(void)
{
    int rc;
    double t0;

    if (g_producer == NULL) goto clear;

    t0 = darshan_core_wtime();

    {
        const char* fe = getenv("DARSHAN_MOFKA_FLUSH_MS");
        unsigned flush_ms = (fe && *fe) ? (unsigned)strtoul(fe, NULL, 10) : 5000;
        rc = diaspora_producer_flush_timeout(g_producer, flush_ms);
        if (rc == DIASPORA_C_TIMEOUT)
            darshan_core_fprintf(stderr, "darshan-mofka: flush timed out; some events may be dropped.\n");
        else if (rc == DIASPORA_C_ERR)
            darshan_core_fprintf(stderr, "darshan-mofka: flush error (%s)\n",
                    diaspora_c_last_error());
    }
    mofka_took("finalize", t0);

    /* The ONE Phase-1 number: total wall-clock spent in diaspora_producer_push()
     * across this rank, the push count, and the mean per push. Only populated when
     * DARSHAN_MOFKA_TIMING is set (the hot path skips the timer otherwise). */
    if (g_timing) {
        unsigned long long tot_ns = atomic_load(&g_push_ns);
        unsigned long long n      = atomic_load(&g_push_n);
        double tot_us = (double)tot_ns / 1e3;
        double avg_us = n ? tot_us / (double)n : 0.0;
        darshan_core_fprintf(stderr,
            "darshan-mofka[timing] PUSH_TOTAL pushes=%llu total_push_us=%.1f avg_push_us=%.3f\n",
            n, tot_us, avg_us);
    }

clear:

    if (g_producer) { diaspora_producer_destroy(g_producer); g_producer = NULL; }
    if (g_topic)    { diaspora_topic_destroy(g_topic);       g_topic = NULL; }
    if (g_driver)   { diaspora_driver_destroy(g_driver);     g_driver = NULL; }
}

#else

/* !HAVE_MOFKA: no-op stubs so the module hooks still link (mirrors darshan-ldms.c). */

void darshan_mofka_connector_initialize(struct darshan_core_runtime *init_core)
{
    (void)init_core;
    return;
}

void darshan_mofka_connector_send(uint64_t record_id, int64_t rank,
                                  int64_t record_count, char *rwo,
                                  int64_t offset, int64_t length,
                                  int64_t max_byte, int64_t rw_switch,
                                  int64_t flushes,
                                  double start_time, double end_time,
                                  double total_time,
                                  char *mod_name, char *data_type,
                                  const void *rec, uint64_t rec_size)
{
    (void)record_id; (void)rank; (void)record_count; (void)rwo;
    (void)offset; (void)length; (void)max_byte; (void)rw_switch;
    (void)flushes; (void)start_time; (void)end_time; (void)total_time;
    (void)mod_name; (void)data_type; (void)rec; (void)rec_size;
    return;
}

void darshan_mofka_connector_flush_records(struct darshan_core_runtime *core)
{
    (void)core;
    return;
}

void darshan_mofka_connector_finalize(void)
{
    return;
}

#endif
