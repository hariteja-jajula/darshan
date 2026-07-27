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

/* ---- async off-the-hot-path streaming --------------------------------------
 * The Mofka push itself is fire-and-forget (diaspora_c.h:95: no broker round-trip
 * on the happy path), but the ~30-42us/op cost is CPU serialization -- hex_into of
 * the whole record + the big snprintf + the JSON re-parse inside push -- all paid
 * inline on the application thread. To get the app thread to ~sub-us we move ALL of
 * that to one (or more) background drain threads: the hot path only assigns seq,
 * snapshots the record bytes, and enqueues into a bounded ring; the drain thread(s)
 * serialize + push. "UDP semantics": when the ring is full we DROP (and count) by
 * default so the app never blocks; DARSHAN_MOFKA_DROP_POLICY=block makes it lossless
 * with backpressure instead. At finalize we stop the drain thread(s), let the ring
 * drain, then diaspora_producer_flush_timeout() -- i.e. "the last push is a flush".
 *
 * seq is assigned on the APP thread at enqueue (op-issue order) -- the reconstructor
 * keeps the max-seq snapshot per (module,record,rank,pid) (should_replace in
 * darshan-mofka-reconstruct.c), so seq MUST reflect issue order, not drain order.
 * The record struct is COPIED at enqueue because the live file_rec keeps mutating.
 * The largest streamed fixed record is H5D (912 B); MOFKA_REC_MAX bounds the copy. */
#define MOFKA_REC_MAX   1024
#define MOFKA_MAX_DRAIN 16

struct mofka_slot {
    uint64_t record_id;
    int64_t  rank, record_count, offset, length, max_byte, rw_switch, flushes;
    double   start_time, end_time, total_time;
    unsigned long long seq;
    const char *rwo;        /* string literals at every call site -> pointer is stable */
    const char *mod_name;
    const char *data_type;
    char     file_esc[1024];/* escaped on the app thread (record-name hash isn't safe */
                            /* to walk from the drain thread while app threads register)*/
    uint32_t rec_size;
    unsigned char rec[MOFKA_REC_MAX];
};

static int             g_async;         /* DARSHAN_MOFKA_ASYNC (default 1)              */
static int             g_block;         /* DARSHAN_MOFKA_DROP_POLICY=block -> 1         */
static struct mofka_slot *g_ring;
static size_t          g_qdepth;        /* ring capacity (slots)                       */
static size_t          g_head, g_tail;  /* head=producer, tail=consumer; both under mtx*/
static pthread_mutex_t g_qmtx     = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_notempty = PTHREAD_COND_INITIALIZER;
static pthread_cond_t  g_notfull  = PTHREAD_COND_INITIALIZER;
static pthread_t       g_drain[MOFKA_MAX_DRAIN];
static int             g_ndrain;
static volatile int    g_stop;
static int             g_leak_ring;     /* set on join-timeout: a live drain thread may read g_ring */
static atomic_ullong   g_dropped;

static char    g_hostname[256];
static long    g_pid;
static int64_t g_uid   = -1;
static int64_t g_jobid = -1;
static double  g_t0_epoch;
static int     g_timing;   /* cached DARSHAN_MOFKA_TIMING: set once in initialize */
static int64_t g_launcher_rank = -1;  /* real per-process rank from the MPI launcher (see initialize).
                                       * The non-MPI Darshan lib reports rank 0 for every process, so
                                       * without this every mpirun rank streams as rank 0 and the
                                       * reconstruction collapses distinct ranks into one record. */

static const char *g_exemnt = NULL;   /* core's exe+mounts buffer (init_core->log_exemnt_p) */
static char        g_host_esc[300];   /* g_hostname JSON-escaped once at init (job constant) */
static atomic_int  g_meta_sent;       /* one-shot metadata guard (atomic: emitted once across threads) */

#define MOFKA_JSON_BUF 8192
/* Envelope fragments shared verbatim by emit_metadata() and darshan_mofka_send() -- macros
 * (compile-time concatenation, no hot-path helper) so type/schema_version/identity live in
 * one place. The differing parts (activity_id, task_id, module/op, rank/seq) stay inline. */
#define MOFKA_ENV_HEAD   "{\"type\":\"task\","
#define MOFKA_ENV_SCHEMA "\"schema\":\"darshan_runtime\",\"schema_version\":2,"
#define MOFKA_ENV_IDENT  "\"hostname\":\"%s\",\"pid\":%ld,\"uid\":%lld,\"job_id\":%lld,"

static void mofka_took(const char* fn, double t0)
{
    if (g_timing)
        darshan_core_fprintf(stderr, "darshan-mofka[timing] %s %.3f us\n",
            fn, (darshan_core_wtime() - t0) * 1e6);
}

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

/* One-shot: stream exe + mounts as a standalone metadata event (no module /
 * record_id, so the reconstructor's update_job_info picks it up and read_events
 * otherwise skips it). Bulky mounts would overflow the shared record buffer, so
 * this gets its own message. */
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

    if (n < 0 || (size_t)n >= sizeof(buf)) return;  /* too big: skip metadata */

    if (diaspora_producer_push(g_producer, buf, NULL, 0) != DIASPORA_C_OK)
        darshan_core_fprintf(stderr, "darshan-mofka: metadata push failed (%s)\n",
                diaspora_c_last_error());
}

/* fork() hazards for the drain thread: a pthread is NOT replicated into the child,
 * so a forked child would inherit the ring + a possibly-locked mutex with no thread
 * to drain it. Lock across the fork (prepare), unlock in the parent, and in the child
 * disable async entirely + null the producer -- Darshan re-inits per process anyway
 * (one process == one launcher rank), so the child simply streams nothing until its
 * own initialize runs. Mofka/margo is not fork-safe regardless. */
static void* mofka_drain_main(void* arg);   /* fwd: used in initialize, defined below */

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

void darshan_mofka_connector_initialize(struct darshan_core_runtime* init_core)
{
    const char* group_file;
    const char* topic_name;
    char opts[1200];
    char gf_esc[1024];
    char pname[64];
    double t0 = darshan_core_wtime();

    g_timing = (getenv("DARSHAN_MOFKA_TIMING") != NULL);
    g_pid = (long)(init_core ? init_core->pid : getpid());

    /* Real per-process rank for multi-process runs. The non-MPI Darshan lib assigns rank 0 to every
     * process, so under mpirun all ranks would stream as rank 0 and the reconstruction (keyed on
     * module/record_id/rank) would collapse distinct ranks into one record. The MPI launcher exports
     * the true rank in the environment -- read it once here and stamp it on every event (see send). */
    { const char* r = getenv("OMPI_COMM_WORLD_RANK");   /* Open MPI */
      if (!r) r = getenv("PMIX_RANK");                  /* PMIx / PRRTE */
      if (!r) r = getenv("PMI_RANK");                   /* MPICH / Intel MPI */
      if (r && *r) g_launcher_rank = (int64_t)strtoll(r, NULL, 10); }

    /* Honor the connector's own enable flag: DARSHAN_MOFKA_ENABLE=0 leaves
     * Darshan recording but sets up no producer, so nothing streams -- the
     * runtime-only baseline for the overhead study. (Absent keeps the historical
     * "stream if a group file is set" behavior; only an explicit 0 disables.) */
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

    json_escape_into(g_host_esc, sizeof(g_host_esc), g_hostname);  /* once, not per send (C3) */

    /* Prefer the job's real start time -- it matches the native log's start_time and avoids
     * the RDTSCP counter darshan_core_wtime_absolute() can hand back as an epoch (C4). */
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

    size_t batch_size = 0 /* adaptive */, max_batches = 0;
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

    /* Async off-the-hot-path streaming (default ON). Spin up the ring + drain
     * thread(s) so per-op app cost is just snapshot+enqueue. DARSHAN_MOFKA_ASYNC=0
     * keeps the old synchronous inline push (for A/B and as a fallback). */
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

/* Build the JSON envelope from a snapshotted slot and push it. This does ALL the
 * expensive work (hex_into + snprintf + the push's internal JSON re-parse). In async
 * mode it runs ONLY on the drain thread(s), off the application critical path; in the
 * synchronous fallback (DARSHAN_MOFKA_ASYNC=0) the app thread calls it directly. */
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

    n = snprintf(buf, sizeof(buf),
        MOFKA_ENV_HEAD
        "\"activity_id\":\"darshan_%s\","
        "\"task_id\":\"darshan-%016llx-%ld-%llu\","
        MOFKA_ENV_SCHEMA
        "\"module\":\"%s\",\"event_type\":\"%s\",\"op\":\"%s\","
        "\"record_id\":\"%016llx\",\"file\":\"%s\","
        MOFKA_ENV_IDENT
        "\"rank\":%lld,\"seq\":%llu,\"t0_epoch\":%.6f,"
        "\"cnt\":%lld,\"off\":%lld,\"len\":%lld,\"max_byte\":%lld,"
        "\"switches\":%lld,\"flushes\":%lld,"
        "\"started_at\":%.6f,\"ended_at\":%.6f,\"dur\":%.6f,\"total\":%.6f,"
        "\"rec_size\":%llu,\"rec_hex\":\"%s\"}",
        s->mod_name ? s->mod_name : "?",
        (unsigned long long)s->record_id, g_pid, s->seq,
        s->mod_name ? s->mod_name : "?",
        s->data_type ? s->data_type : "?",
        s->rwo ? s->rwo : "?",
        (unsigned long long)s->record_id, s->file_esc,
        g_host_esc, g_pid, (long long)g_uid, (long long)g_jobid,
        (long long)(g_launcher_rank >= 0 ? g_launcher_rank : s->rank), s->seq, g_t0_epoch,
        (long long)s->record_count, (long long)s->offset, (long long)s->length,
        (long long)s->max_byte, (long long)s->rw_switch, (long long)s->flushes,
        started_epoch, ended_epoch, s->end_time - s->start_time, s->total_time,
        (unsigned long long)s->rec_size, rec_hex);

    if (n < 0 || (size_t)n >= sizeof(buf)) return;

    if (diaspora_producer_push(g_producer, buf, NULL, 0) != DIASPORA_C_OK)
        darshan_core_fprintf(stderr, "darshan-mofka: push failed (%s)\n",
                diaspora_c_last_error());
}

/* Drain thread: pull snapshotted slots off the ring and serialize+push them. One or
 * more of these run for the process lifetime (DARSHAN_MOFKA_DRAIN_THREADS). All the
 * per-op CPU cost lives here, so the app threads only pay the cheap enqueue. */
/* Emit the one-shot metadata event exactly once across all threads. atomic CAS so
 * concurrent drain threads (or app threads in sync mode) can't double-emit. */
static void mofka_emit_metadata_once(void)
{
    int expected = 0;
    if (atomic_compare_exchange_strong(&g_meta_sent, &expected, 1))
        emit_metadata();
}

static void* mofka_drain_main(void* arg)
{
    struct mofka_slot local;
    (void)arg;
    /* Mark this thread as "in send" for its whole life: if diaspora_producer_push ever
     * performed Darshan-instrumented POSIX/STDIO I/O it would otherwise re-enter
     * connector_send and enqueue a feedback loop. (The push is margo/libfabric network
     * I/O, which Darshan doesn't wrap, so this is a latent guard, not an active path.) */
    g_in_send = 1;
    mofka_emit_metadata_once();   /* first drain thread emits metadata, off the app thread */
    for (;;) {
        pthread_mutex_lock(&g_qmtx);
        while (g_head == g_tail && !g_stop)
            pthread_cond_wait(&g_notempty, &g_qmtx);
        if (g_head == g_tail && g_stop) { pthread_mutex_unlock(&g_qmtx); break; }
        local = g_ring[g_tail];                     /* copy out under lock */
        g_tail = (g_tail + 1) % g_qdepth;
        pthread_cond_signal(&g_notfull);            /* wake a blocked producer (block mode) */
        pthread_mutex_unlock(&g_qmtx);
        mofka_serialize_and_push(&local);
    }
    return NULL;
}

/* Hot path: snapshot the op into a ring slot and return. In sync-fallback mode
 * (g_async==0) it serializes+pushes inline exactly as the original connector did. */
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

    /* Gate on the disabled/reentrant state BEFORE any work: when the connector is
     * off (g_producer == NULL, e.g. DARSHAN_MOFKA_ENABLE=0) the hooked op pays
     * nothing, so the runtime-only baseline is truly zero-overhead and the overhead
     * A/B measures only real streaming cost. (Matches the upstream LDMS early-out.) */
    if (g_producer == NULL || g_in_send) return;
    if (rec_size > MOFKA_REC_MAX) return;   /* variable/heatmap module: skip (as before) */
    g_in_send = 1;
    t0 = darshan_core_wtime();

    /* seq is assigned HERE, on the app thread, in op-issue order -- the reconstructor's
     * last-writer-wins key. Escaping the record name also happens here (the core name
     * hash is walked while app threads may register; keep it off the drain thread). */
    seq = (unsigned long long)atomic_fetch_add(&g_seq, 1);
    file_path = (const char*)darshan_core_lookup_record_name(record_id);

    if (!g_async) {
        /* Synchronous fallback: build a stack slot and push inline. */
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

    /* Async: enqueue a snapshot; the drain thread does serialize+push. */
    pthread_mutex_lock(&g_qmtx);
    next = (g_head + 1) % g_qdepth;
    if (next == g_tail) {                    /* ring full */
        if (g_block) {
            while (next == g_tail && !g_stop)
                pthread_cond_wait(&g_notfull, &g_qmtx);   /* lossless: backpressure */
        }
        if (next == g_tail) {                /* still full (drop mode, or stopping) */
            pthread_mutex_unlock(&g_qmtx);
            atomic_fetch_add(&g_dropped, 1); /* UDP: push-and-forget, drop */
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

/* Opt-in (DARSHAN_MOFKA_FINAL_SWEEP=1): re-stream every in-memory module record's FINAL struct at
 * shutdown, so records whose ops were never streamed live still land. Import-heavy workloads (e.g.
 * python-ml) open interpreter-startup files during the ~200ms producer-init window; those files'
 * per-op sends are no-ops (producer not up yet) even though Darshan records them in memory, so they
 * are absent from the live stream but present in the native log. This sweep recovers them.
 * Re-sending records that WERE streamed live is harmless -- reconstruct keeps the max-seq snapshot
 * per (module,record,rank). Variable-size/heatmap/unknown modules are dropped on the reconstruct
 * side by its record-size check, and op="FINAL" contributes no heatmap bins, so this only makes
 * COUNTERS complete (the per-op heatmap for init-window files stays approximate -- known caveat).
 * Must run BEFORE mod_cleanup_func() frees the record buffers (see darshan-core.c cleanup:).
 *
 * KNOWN ISSUE -- DISABLED BY DEFAULT. When enabled, the FIRST push here hangs indefinitely on
 * python-ml: the extra records are NEW async sends issued from the atexit/shutdown context, and
 * mofka's producer sender loop runs on the margo *progress* pool (MofkaDriver::defaultThreadPool ->
 * get_progress_pool), so a send RPC initiated once the process is winding down never progresses.
 * Live sends work because they run while the process is active. Confirmed 2026-07-25: clean HEAD
 * (no sweep) completes python-ml with the known counter gap; every run with the sweep on hangs at
 * finalize (no `finalize` timing line, killed at walltime). A proper fix needs a mofka-side change
 * (run the producer sender on a dedicated non-progress pool), out of scope here. C is unaffected
 * (byte-exact) and never enables this. Left in place, off, for a future mofka fix. */
void darshan_mofka_connector_flush_records(struct darshan_core_runtime* core)
{
    int m;

    if (g_producer == NULL || core == NULL) return;
    /* Off unless explicitly enabled: unset, "", and "0" all mean OFF (a plain NULL check would
     * treat "0" as on). See the KNOWN ISSUE note above -- enabling this hangs python-ml. */
    { const char* sw = getenv("DARSHAN_MOFKA_FINAL_SWEEP");
      if (sw == NULL || sw[0] == '\0' || sw[0] == '0') return; }

    for (m = 0; m < DARSHAN_KNOWN_MODULE_COUNT; m++)
    {
        struct darshan_core_module* mod = core->mod_array[m];
        char  *p, *end;
        size_t stride;

        if (mod == NULL) continue;
        stride = mod->rec_size;              /* fixed per-record stride captured at register */
        if (stride == 0) continue;

        p   = (char*) mod->rec_buf_start;
        end = (char*) mod->rec_buf_p;        /* next free byte: records live in [start, p) */
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

void darshan_mofka_connector_finalize(void)
{
    int rc;
    double t0;

    if (g_producer == NULL) goto clear;

    t0 = darshan_core_wtime();

    /* Stop the drain thread(s) and let the ring finish. All enqueued ops are pushed
     * by the drain thread (never by finalize itself -- issuing NEW sends from the
     * shutdown context is exactly what hangs the disabled sweep, see the note above),
     * so this only JOINS. Bounded join: on timeout, leak the producer and skip the
     * flush/destroy (diaspora_c.h:120-123 -- destructors may re-contact a dead broker
     * and hang exit; the OS reclaims everything anyway). */
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
            g_producer = NULL;   /* skip flush+destroy below */
            g_leak_ring = 1;     /* a stuck drain thread may still read g_ring: don't free it */
        }
    }

    { unsigned long long d = atomic_load(&g_dropped);
      if (d) darshan_core_fprintf(stderr, "darshan-mofka: dropped %llu events (ring full); "
              "raise DARSHAN_MOFKA_QUEUE_DEPTH or use DARSHAN_MOFKA_DROP_POLICY=block\n", d); }

    /* "The last push is a flush": drain Mofka's own pending batches once. */
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
    /* Tear down on every path (including flush timeout) rather than leaking --
     * order matches the diaspora_c.h example: producer, then topic, then driver. */
    if (g_producer) { diaspora_producer_destroy(g_producer); g_producer = NULL; }
    if (g_topic)    { diaspora_topic_destroy(g_topic);       g_topic = NULL; }
    if (g_driver)   { diaspora_driver_destroy(g_driver);     g_driver = NULL; }
    if (g_ring && !g_leak_ring) { free(g_ring); g_ring = NULL; }
}

#else

/* No stub definitions when Mofka is unavailable: darshan-core.c guards the
 * initialize/finalize calls with #ifdef HAVE_MOFKA and DARSHAN_MOFKA_SEND()
 * expands to a no-op (see darshan-mofka.h), so none of the connector entry
 * points are referenced in a !HAVE_MOFKA build. */

#endif /* HAVE_MOFKA */
