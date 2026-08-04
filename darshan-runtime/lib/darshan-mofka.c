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
#include <pthread.h>
#include <diaspora/diaspora_c.h>

static diaspora_driver_t*   g_driver;
static diaspora_topic_t*    g_topic;
static diaspora_producer_t* g_producer;

static atomic_ullong g_seq;
static __thread int  g_in_send;

#define MOFKA_REC_MAX   1024
#define MOFKA_MAX_DRAIN 16

/* One I/O event, snapshotted on the app thread and drained off it. */
struct mofka_slot {
    uint64_t record_id;
    int64_t  rank, record_count, offset, length, max_byte, rw_switch, flushes;
    double   start_time, end_time, total_time;
    unsigned long long seq;
    const char *rwo;
    const char *mod_name;
    const char *data_type;
    char     file_esc[1024];
    uint32_t rec_size;
    unsigned char rec[MOFKA_REC_MAX];
};

static int             g_async;
static int             g_block;
static struct mofka_slot *g_ring;
static size_t          g_qdepth;
static size_t          g_head, g_tail;
static pthread_mutex_t g_qmtx     = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_notempty = PTHREAD_COND_INITIALIZER;
static pthread_cond_t  g_notfull  = PTHREAD_COND_INITIALIZER;
static pthread_t       g_drain[MOFKA_MAX_DRAIN];
static int             g_ndrain;
static volatile int    g_stop;
static int             g_leak_ring;
static atomic_ullong   g_dropped;

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

/* Hex-encode n bytes of src into dst; always NUL-terminates. */
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
 *  Ring buffer + drain thread (the hot path lives here)              *
 * ------------------------------------------------------------------ */

/* fork() doesn't clone the drain thread: lock the ring across the fork, and in
 * the child disable async + null the producer so it streams nothing until its
 * own initialize runs. (Mofka/margo is not fork-safe.) */
static void mofka_atfork_prepare(void) { pthread_mutex_lock(&g_qmtx); }
static void mofka_atfork_parent(void)  { pthread_mutex_unlock(&g_qmtx); }
static void mofka_atfork_child(void)
{
    pthread_mutex_init(&g_qmtx, NULL);
    pthread_cond_init(&g_notempty, NULL);
    pthread_cond_init(&g_notfull, NULL);
    g_ring = NULL; g_async = 0; g_ndrain = 0;
    g_head = g_tail = 0; g_producer = NULL; g_topic = NULL; g_driver = NULL;
}

/* Build the JSON envelope from a snapshot and push it (all the CPU cost). */
static void mofka_serialize_and_push(const struct mofka_slot* s)
{
    char buf[MOFKA_JSON_BUF];
    char rec_hex[4096];
    double started_epoch, ended_epoch;
    int n;

    { struct timespec ts = darshan_core_abs_timespec_from_wtime(s->start_time);
      struct timespec te = darshan_core_abs_timespec_from_wtime(s->end_time);
      started_epoch = (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
      ended_epoch   = (double)te.tv_sec + (double)te.tv_nsec / 1e9; }

    rec_hex[0] = '\0';
    if (s->rec_size > 0)
        hex_into(rec_hex, sizeof(rec_hex), s->rec, s->rec_size);

    /* SLIM ENVELOPE: only the fields darshan-mofka-reconstruct.c actually reads from a
     * module event -- module, record_id, rank, pid, rec_size, seq, file, op, len,
     * started_at, ended_at, rec_hex. The full record is inside rec_hex, so the parsed
     * counters (cnt/off/max_byte/switches/flushes/dur/total) were pure duplication; and
     * uid/job_id/hostname/t0_epoch are supplied once by the metadata event (reconstruct
     * uses have_* guards). Dropping them ~halves each message -> higher drain throughput.
     * (op/len/started_at/ended_at are kept: reconstruct's heatmap needs them.) */
    n = snprintf(buf, sizeof(buf),
        MOFKA_ENV_HEAD
        MOFKA_ENV_SCHEMA
        "\"module\":\"%s\",\"op\":\"%s\","
        "\"record_id\":\"%016llx\",\"file\":\"%s\",\"pid\":%ld,"
        "\"rank\":%lld,\"seq\":%llu,"
        "\"len\":%lld,\"started_at\":%.6f,\"ended_at\":%.6f,"
        "\"rec_size\":%llu,\"rec_hex\":\"%s\"}",
        s->mod_name ? s->mod_name : "?",
        s->rwo ? s->rwo : "?",
        (unsigned long long)s->record_id, s->file_esc, g_pid,
        (long long)(g_launcher_rank >= 0 ? g_launcher_rank : s->rank), s->seq,
        (long long)s->length, started_epoch, ended_epoch,
        (unsigned long long)s->rec_size, rec_hex);

    if (n < 0 || (size_t)n >= sizeof(buf)) return;

    /* the real push; in async mode this runs on the drain thread, not in send() */
    double pt0 = darshan_core_wtime();
    if (diaspora_producer_push(g_producer, buf, NULL, 0) != DIASPORA_C_OK)
        darshan_core_fprintf(stderr, "darshan-mofka: push failed (%s)\n",
                diaspora_c_last_error());
    mofka_took("push", pt0);
}

/* Drain thread: pop snapshots off the ring and serialize+push them. */
static void* mofka_drain_main(void* arg)
{
    struct mofka_slot local;
    (void)arg;

    g_in_send = 1;
    mofka_emit_metadata_once();
    for (;;) {
        pthread_mutex_lock(&g_qmtx);
        while (g_head == g_tail && !g_stop)
            pthread_cond_wait(&g_notempty, &g_qmtx);
        if (g_head == g_tail && g_stop) { pthread_mutex_unlock(&g_qmtx); break; }
        local = g_ring[g_tail];
        g_tail = (g_tail + 1) % g_qdepth;
        pthread_cond_signal(&g_notfull);
        pthread_mutex_unlock(&g_qmtx);
        mofka_serialize_and_push(&local);
    }
    return NULL;
}

/* ------------------------------------------------------------------ *
 *  Lifecycle: initialize -> send -> flush_records -> finalize        *
 * ------------------------------------------------------------------ */

/* Connect the producer and, unless ASYNC=0, start the ring + drain thread(s). */
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

    /* --- client-engine margo config -------------------------------------
     * A pure producer never serves incoming RPCs, so we drop the rpc threads and
     * run a dedicated progress thread. The fixed-field form below is built from
     * three env-overridable knobs; for full control (custom argobots pools/xstreams,
     * cpubind, etc.) set DARSHAN_MOFKA_MARGO_JSON and its value is spliced in AS-IS
     * as the entire "margo" object.
     *   DARSHAN_MOFKA_PROGRESS_TIMEOUT_MS  progress_timeout_ub_msec (default 100)
     *   DARSHAN_MOFKA_RPC_THREADS          rpc_thread_count         (default 0)
     *   DARSHAN_MOFKA_PROGRESS_THREAD      use_progress_thread      (default 1)
     *   DARSHAN_MOFKA_MARGO_JSON           verbatim "margo" object (overrides above)
     */
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

    g_async = 1;
    { const char* a = getenv("DARSHAN_MOFKA_ASYNC");
      if (a && a[0] == '0') g_async = 0; }
    if (g_async) {
        const char* e;
        g_qdepth = 65536;
        if ((e = getenv("DARSHAN_MOFKA_QUEUE_DEPTH")) && *e) {
            size_t q = (size_t)strtoull(e, NULL, 10);
            if (q >= 2) g_qdepth = q;
        }
        g_block = 0;
        if ((e = getenv("DARSHAN_MOFKA_DROP_POLICY")) && strcmp(e, "block") == 0) g_block = 1;
        g_ndrain = 1;
        if ((e = getenv("DARSHAN_MOFKA_DRAIN_THREADS")) && *e) {
            int nd = (int)strtol(e, NULL, 10);
            if (nd >= 1 && nd <= MOFKA_MAX_DRAIN) g_ndrain = nd;
        }
        g_stop = 0; g_head = g_tail = 0;
        g_ring = calloc(g_qdepth, sizeof(*g_ring));
        if (g_ring == NULL) {
            darshan_core_fprintf(stderr, "darshan-mofka: ring calloc(%zu) failed; "
                    "falling back to synchronous push.\n", g_qdepth);
            g_async = 0;
        } else {
            int started = 0, i;
            for (i = 0; i < g_ndrain; i++)
                if (pthread_create(&g_drain[i], NULL, mofka_drain_main, NULL) == 0) started++;
            g_ndrain = started;
            if (started == 0) {
                darshan_core_fprintf(stderr, "darshan-mofka: no drain thread started; "
                        "falling back to synchronous push.\n");
                free(g_ring); g_ring = NULL; g_async = 0;
            } else {
                pthread_atfork(mofka_atfork_prepare, mofka_atfork_parent, mofka_atfork_child);
                if (getenv("DARSHAN_MOFKA_VERBOSE"))
                    darshan_core_fprintf(stderr, "darshan-mofka: async ON "
                            "(qdepth=%zu drain_threads=%d drop_policy=%s)\n",
                            g_qdepth, g_ndrain, g_block ? "block" : "drop");
            }
        }
    }

    mofka_took("initialize", t0);
}

/* Hot path: assign seq, snapshot the op into the ring (sync mode pushes inline). */
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
    struct mofka_slot* s;
    unsigned long long seq;
    const char* file_path;
    size_t next;
    double t0;

    if (g_producer == NULL || g_in_send) return;
    if (rec_size > MOFKA_REC_MAX) return;
    g_in_send = 1;
    t0 = darshan_core_wtime();

    /* seq assigned here, on the app thread, in issue order -- the reconstructor's
     * max-seq dedup key depends on it reflecting issue, not drain, order. */
    seq = (unsigned long long)atomic_fetch_add(&g_seq, 1);
    file_path = (const char*)darshan_core_lookup_record_name(record_id);

    if (!g_async) {

        struct mofka_slot ss;
        mofka_emit_metadata_once();
        ss.record_id=record_id; ss.rank=rank; ss.record_count=record_count;
        ss.offset=offset; ss.length=length; ss.max_byte=max_byte;
        ss.rw_switch=rw_switch; ss.flushes=flushes;
        ss.start_time=start_time; ss.end_time=end_time; ss.total_time=total_time;
        ss.seq=seq; ss.rwo=rwo; ss.mod_name=mod_name; ss.data_type=data_type;
        json_escape_into(ss.file_esc, sizeof(ss.file_esc), file_path);
        ss.rec_size=(uint32_t)rec_size;
        if (rec && rec_size) memcpy(ss.rec, rec, rec_size);
        mofka_serialize_and_push(&ss);
        goto out;
    }

    pthread_mutex_lock(&g_qmtx);
    next = (g_head + 1) % g_qdepth;
    if (next == g_tail) {
        if (g_block) {
            while (next == g_tail && !g_stop)
                pthread_cond_wait(&g_notfull, &g_qmtx);
        }
        if (next == g_tail) {
            pthread_mutex_unlock(&g_qmtx);
            atomic_fetch_add(&g_dropped, 1);
            goto out;
        }
    }
    s = &g_ring[g_head];
    s->record_id=record_id; s->rank=rank; s->record_count=record_count;
    s->offset=offset; s->length=length; s->max_byte=max_byte;
    s->rw_switch=rw_switch; s->flushes=flushes;
    s->start_time=start_time; s->end_time=end_time; s->total_time=total_time;
    s->seq=seq; s->rwo=rwo; s->mod_name=mod_name; s->data_type=data_type;
    json_escape_into(s->file_esc, sizeof(s->file_esc), file_path);
    s->rec_size=(uint32_t)rec_size;
    if (rec && rec_size) memcpy(s->rec, rec, rec_size);
    g_head = next;
    pthread_cond_signal(&g_notempty);
    pthread_mutex_unlock(&g_qmtx);

out:
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

/* Stop + join the drain thread(s), flush pending batches, tear down the producer.
 * Bounded join: on timeout, leak the producer/ring to avoid a shutdown hang. */
void darshan_mofka_connector_finalize(void)
{
    int rc;
    double t0;

    if (g_producer == NULL) goto clear;

    t0 = darshan_core_wtime();

    if (g_async && g_ndrain > 0) {
        int i, all_joined = 1;
        struct timespec ts;
        pthread_mutex_lock(&g_qmtx);
        g_stop = 1;
        pthread_cond_broadcast(&g_notempty);
        pthread_cond_broadcast(&g_notfull);
        pthread_mutex_unlock(&g_qmtx);
        clock_gettime(CLOCK_REALTIME, &ts);
        { const char* je = getenv("DARSHAN_MOFKA_JOIN_MS");
          unsigned join_ms = (je && *je) ? (unsigned)strtoul(je, NULL, 10) : 10000;
          ts.tv_sec  += join_ms / 1000;
          ts.tv_nsec += (long)(join_ms % 1000) * 1000000L;
          if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; } }
        for (i = 0; i < g_ndrain; i++)
            if (pthread_timedjoin_np(g_drain[i], NULL, &ts) != 0) all_joined = 0;
        g_async = 0; g_ndrain = 0;
        if (!all_joined) {
            darshan_core_fprintf(stderr, "darshan-mofka: drain join timed out; "
                    "leaking producer to avoid a shutdown hang.\n");
            g_producer = NULL;
            g_leak_ring = 1;
        }
    }

    { unsigned long long d = atomic_load(&g_dropped);
      if (d) darshan_core_fprintf(stderr, "darshan-mofka: dropped %llu events (ring full); "
              "raise DARSHAN_MOFKA_QUEUE_DEPTH or use DARSHAN_MOFKA_DROP_POLICY=block\n", d); }

    if (g_producer) {
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

clear:

    if (g_producer) { diaspora_producer_destroy(g_producer); g_producer = NULL; }
    if (g_topic)    { diaspora_topic_destroy(g_topic);       g_topic = NULL; }
    if (g_driver)   { diaspora_driver_destroy(g_driver);     g_driver = NULL; }
    if (g_ring && !g_leak_ring) { free(g_ring); g_ring = NULL; }
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
