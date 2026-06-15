/*
 * (C) 2026 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 *
 * darshan-mofka.c -- Mofka/Diaspora streaming connector for darshan-runtime.
 *
 * RETARGET EDITION (branch feature/mofka-module-diasporac; P3): everything
 * that previously lived in darshan-mofka-impl.cpp (C++) is now pure C
 * calling the diaspora-c bindings (hariteja-jajula/diaspora-stream-api
 * feature/c-bindings sha c25dd71, live-verified 2026-06-15 13:12 on
 * bdw-0065 -- see CHECK_IN_P2.md ADDENDUM). darshan-runtime contains no
 * C++ translation unit. Two whole hazard classes from the C++ era are
 * gone by construction: static-destruction ordering (no global C++
 * objects) and exception leakage (the bindings guarantee none crosses
 * their boundary).
 *
 * Stacks on top of the C5 -> S2-disc -> S4 chain: the public C ABI
 * (this file's three extern functions in darshan-mofka.h) is byte-
 * identical to the vendored branch, so the ~20 call sites across
 * darshan-{core,posix,stdio,mpiio,hdf5}.c need ZERO changes -- that
 * invariance is the design point of the retarget.
 *
 * Defensive layers (all preserved, but mechanism differs):
 *  - fail-safe init: broker unreachable => warn once, stream disabled,
 *    application untouched
 *  - circuit breaker: push error => streaming disabled for the process
 *    (B layer in the RFC's three-layer story)
 *  - STALL DETECTOR (S4 / B2 layer): instead of tracking the oldest
 *    unacked Future inline (the impl.cpp shape), call into the bindings'
 *    diaspora_producer_oldest_pending_age(); policy stays in darshan,
 *    mechanism lives in C++ behind the C boundary. This is the cleaner
 *    factoring -- diaspora_c.h:97-106 documents the bindings' contract.
 *  - fork safety (S2-disc + alpha equivalent): atfork CHILD handler
 *    disarms inherited handles. Pairs with the in_atfork_child sentinel
 *    in darshan-core.c (committed in eb711591) -- core skips both mofka
 *    init and finalize on the fork-child path, so we only need atfork
 *    here as belt-and-braces.
 *  - re-entrancy guard: our own stderr writes can be instrumented I/O
 *    once STDIO sites are active; thread-local guard prevents recursion
 *    (preserved from C5b's AD-21 pattern at impl.cpp:441-443).
 *  - shutdown: bounded flush; on timeout LEAK handles (destroying would
 *    re-contact a dead broker and hang exit; OS reclaims at exit anyway).
 *    Patch D, preserved verbatim in spirit.
 *
 * VERIFY-D reconciliations applied from the senior's retarget draft
 * (~/setup/notes/supplemental_material/darshan-mofka.c):
 *  D1: include set mirrors current darshan-mofka.c (darshan.h + the .h);
 *      the !HAVE_MOFKA stub block also lives in this file (unified pattern)
 *      not in the header.
 *  D2: connector struct type is `darshanMofkaConnector` (not `darshan_mofka`);
 *      mC field names from the frozen header are used verbatim.
 *  D3: json_escape semantics match impl.cpp's escape policy: '"' and '\\'
 *      escaped, control bytes -> '?'.
 *  D4: init prototype matches darshan-mofka.h exactly.
 *  D5: identity field paths from init_core->log_job_p->{uid,jobid} per
 *      C5a's identity block (Patch A).
 *  D6: send prototype matches darshan-mofka.h's 14-arg signature exactly;
 *      includes the file_path lookup the C side does for the bindings.
 *  D7: darshan_core_lookup_record_name signature matches darshan.h.
 *  D8: JSON format string IS the F1-fixed version from impl.cpp's send_impl
 *      (commit 8b24d294): task_id template's third positional arg is `seq`
 *      (per-producer monotonic), NOT record_count -- the bug that caused
 *      flowcept mongo upsert to collapse 50 sends to 24 docs is preserved
 *      as fixed across the retarget.
 *  D-extra: removed dead pthread_once line that referenced a NULL fn ptr;
 *      keep the simpler static-int-once pattern instead.
 *  D-extra: dbg() breadcrumb strings match impl.cpp exactly for the
 *      strings that test harnesses grep ("send: FIRE op=", "send: PUSH ok",
 *      "finalize: DONE", "finalize: SKIP (idempotent:..."), so the
 *      P0.2 ledger + P0.4 STDIO burst verdicts read identical to the
 *      vendored branch -- this is the parity invariant the master plan's
 *      P3.5 acceptance criterion depends on.
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
#include <stdarg.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>

#include "darshan-mofka.h"
#include "darshan.h"

#ifdef HAVE_MOFKA

#include <stdatomic.h>             /* fine in pure C; no C++ TU left */
#include <diaspora/diaspora_c.h>   /* the new boundary; provided by check_diaspora_c.m4 */

/* ---------------------------------------------------------------------- */
/* connector state                                                         */
/* VERIFY-D2: struct type + EXACT field names from the frozen header.      */
/* The opaque void* slots (mC.driver/topic/producer) are mirrored to the   */
/* typed g_* statics for internal use; consumers only see mC.              */
/* ---------------------------------------------------------------------- */
struct darshanMofkaConnector mC = {
    .mofka_lib = 1,
    .driver = NULL,
    .topic = NULL,
    .producer = NULL,
};

/* private state (this file only) */
static diaspora_driver_t*   g_driver;
static diaspora_topic_t*    g_topic;
static diaspora_producer_t* g_producer;

static atomic_bool          g_push_broken;     /* circuit breaker          */
static atomic_bool          g_push_stalled;    /* S4 stall detector flag   */
static atomic_ullong        g_seq;             /* per-producer event seq   */
static __thread int         g_in_send;         /* re-entrancy guard (TLS;
                                                * AD-21 pattern preserved) */

static char    g_hostname[256];
static long    g_pid;
static int64_t g_uid     = -1;
static int64_t g_jobid   = -1;
static double  g_t0_epoch;                     /* CLOCK_REALTIME at init   */
static int     g_stall_sec   = 5;              /* 0 disables the detector  */
static int     g_dbg;
static int     g_verbose;

#define MOFKA_JSON_BUF 2048                    /* matches C5 schema v2     */

/* ---------------------------------------------------------------------- */
/* small helpers                                                           */
/* ---------------------------------------------------------------------- */

/* DEBUG breadcrumb (gated by DARSHAN_MOFKA_DEBUG). Cached on first call
 * (same idiom as impl.cpp's dbg_enabled at lines 206-214). Format matches
 * impl.cpp's `dbg()` output: a single "darshan-mofka[DEBUG]" prefix with
 * pid + body. */
static int dbg_enabled(void)
{
    static int cached = -1;
    if (cached < 0) {
        const char* v = getenv("DARSHAN_MOFKA_DEBUG");
        cached = (v != NULL && *v != '\0' && *v != '0') ? 1 : 0;
    }
    return cached;
}

static void dbg(const char* fmt, ...)
{
    if (!dbg_enabled()) return;
    va_list ap;
    va_start(ap, fmt);
    /* match impl.cpp's prefix shape (impl.cpp:217-228): single line,
     * stderr, flushed; pid included so logs from concurrent helpers
     * stay attributable. */
    fprintf(stderr, "darshan-mofka[DEBUG] pid=%ld ", g_pid ? g_pid : (long)getpid());
    vfprintf(stderr, fmt, ap);
    fputc('\n', stderr);
    fflush(stderr);
    va_end(ap);
}

/* warn exactly once per message site (flood protection, from C3) */
#define WARN_ONCE(...) do {                                          \
        static int _warned;                                          \
        if (!_warned) { _warned = 1;                                 \
            fprintf(stderr, __VA_ARGS__); fflush(stderr); }          \
    } while (0)

/* VERIFY-D3: JSON string escape. '"' and '\' escaped, control bytes
 * replaced with '?'. Byte-for-byte semantics must match impl.cpp's
 * json_escape helper so the records on the wire are identical across
 * the vendored and retargeted branches (parity test invariant). */
static void json_escape_into(char* dst, size_t dstsz, const char* src)
{
    size_t o = 0;
    if (dstsz == 0) return;
    if (src == NULL) src = "unknown";
    for (; *src && o + 2 < dstsz; src++) {
        unsigned char c = (unsigned char)*src;
        if (c == '"' || c == '\\') {
            dst[o++] = '\\';
            dst[o++] = (char)c;
        } else if (c < 0x20) {
            dst[o++] = '?';
        } else {
            dst[o++] = (char)c;
        }
    }
    dst[o] = '\0';
}

static int64_t env_i64(const char* name, int64_t dflt, int64_t lo, int64_t hi)
{
    const char* v = getenv(name);
    char* end = NULL;
    long long x;
    if (v == NULL || *v == '\0') return dflt;
    x = strtoll(v, &end, 10);
    if (end == v || x < lo || x > hi) return dflt;
    return (int64_t)x;
}

/* per-module enable check -- mirrors impl.cpp:336-339 module name list */
static int module_enabled_for(const char* mod)
{
    if (mod == NULL) return 0;
    if (strcmp(mod, "POSIX") == 0)   return mC.posix_enable_mofka;
    if (strcmp(mod, "STDIO") == 0)   return mC.stdio_enable_mofka;
    if (strcmp(mod, "MPIIO") == 0 ||
        strcmp(mod, "MPI-IO") == 0)  return mC.mpiio_enable_mofka;
    if (strcmp(mod, "H5F") == 0 ||
        strcmp(mod, "H5D") == 0 ||
        strcmp(mod, "HDF5") == 0)    return mC.hdf5_enable_mofka;
    return 0;
}

/* fork safety. Child inherits broken transport state (progress thread
 * does not survive fork). Disarm: trip the breaker AND clear the handles
 * WITHOUT destroying them (deliberate leak in the child -- destroying
 * would touch the now-dead engine). Parent is untouched. Pairs with the
 * darshan-core in_atfork_child sentinel (S2-disc, eb711591), which now
 * skips the mofka init/finalize block entirely on the fork-child path;
 * this handler is the belt-and-braces fallback. */
static void mofka_atfork_child(void)
{
    atomic_store(&g_push_broken, 1);
    g_producer = NULL;
    g_topic = NULL;
    g_driver = NULL;
    mC.producer = NULL;
    mC.topic = NULL;
    mC.driver = NULL;
    mC.mofka_lib = 0;
}

/* ---------------------------------------------------------------------- */
/* initialize -- called from darshan_core_initialize (HAVE_MOFKA hook)     */
/* VERIFY-D4: prototype exactly matches the frozen darshan-mofka.h:86.    */
/* ---------------------------------------------------------------------- */
void darshan_mofka_connector_initialize(struct darshan_core_runtime* init_core)
{
    const char* group_file;
    const char* topic_name;
    const char* driver_name;
    char opts[1200];
    char gf_esc[1024];
    struct timespec ts;
    static int atfork_done = 0;

    if (getenv("DARSHAN_MOFKA_ENABLE") == NULL) return;   /* opt-in */

    g_dbg     = (getenv("DARSHAN_MOFKA_DEBUG")   != NULL);
    g_verbose = (getenv("DARSHAN_MOFKA_VERBOSE") != NULL);
    g_pid     = (long)getpid();

    dbg("init: ENTER (ppid=%ld)", (long)getppid());

    /* identity block (schema v2, Patch A) -- VERIFY-D5: fields from
     * init_core->log_job_p match impl.cpp:75-86 (the C-side init that
     * extracted scalars before passing them to impl.cpp).  Hostname
     * and t0_epoch likewise. */
    if (gethostname(g_hostname, sizeof(g_hostname)) != 0) {
        snprintf(g_hostname, sizeof(g_hostname), "unknown");
    }
    g_hostname[sizeof(g_hostname) - 1] = '\0';

    if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
        g_t0_epoch = (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
    }

    if (init_core && init_core->log_job_p) {
        g_uid   = (int64_t)init_core->log_job_p->uid;
        g_jobid = (int64_t)init_core->log_job_p->jobid;
    }

    g_stall_sec = (int)env_i64("DARSHAN_MOFKA_STALL_TIMEOUT_SEC", 5, 0, 3600);

    group_file  = getenv("DARSHAN_MOFKA_GROUP_FILE");
    topic_name  = getenv("DARSHAN_MOFKA_TOPIC");
    driver_name = getenv("DARSHAN_MOFKA_DRIVER");
    if (driver_name == NULL || *driver_name == '\0') {
        driver_name = "mofka";   /* default driver name */
    }

    if (group_file == NULL || topic_name == NULL) {
        WARN_ONCE("darshan-mofka: DARSHAN_MOFKA_GROUP_FILE/TOPIC not set; "
                  "records will not be streamed.\n");
        dbg("init: ABORT (DARSHAN_MOFKA_GROUP_FILE/TOPIC unset)");
        return;
    }

    dbg("init: module enables posix=%d stdio=%d mpiio=%d hdf5=%d",
        getenv("DARSHAN_MOFKA_ENABLE_POSIX") ? 1 : 0,
        getenv("DARSHAN_MOFKA_ENABLE_STDIO") ? 1 : 0,
        getenv("DARSHAN_MOFKA_ENABLE_MPIIO") ? 1 : 0,
        getenv("DARSHAN_MOFKA_ENABLE_HDF5")  ? 1 : 0);
    dbg("init: group_file=%s topic=%s driver=%s",
        group_file, topic_name, driver_name);

    /* Register atfork CHILD handler once per process (D-extra: replaces
     * dead pthread_once line in the draft -- pthread_once requires
     * void(void), not NULL). atfork is best-effort: if the registration
     * fails we log and continue; the in_atfork_child sentinel in
     * darshan-core.c (S2-disc) is the load-bearing protection now. */
    if (!atfork_done) {
        atfork_done = 1;
        if (pthread_atfork(NULL, NULL, mofka_atfork_child) != 0) {
            dbg("init: WARNING pthread_atfork failed (fork-safety not installed)");
        } else {
            dbg("init: pthread_atfork child handler registered");
        }
    }

    json_escape_into(gf_esc, sizeof(gf_esc), group_file);
    snprintf(opts, sizeof(opts), "{\"group_file\":\"%s\"}", gf_esc);

    dbg("init: creating diaspora driver");
    g_driver = diaspora_driver_create(driver_name, opts);
    if (g_driver == NULL) {
        WARN_ONCE("darshan-mofka: failed to initialize driver (%s); "
                  "records will not be streamed.\n", diaspora_c_last_error());
        return;
    }

    dbg("init: driver created; opening topic '%s'", topic_name);
    g_topic = diaspora_topic_open(g_driver, topic_name);
    if (g_topic == NULL) {
        WARN_ONCE("darshan-mofka: failed to open topic '%s' (%s); "
                  "records will not be streamed.\n",
                  topic_name, diaspora_c_last_error());
        diaspora_driver_destroy(g_driver);
        g_driver = NULL;
        return;
    }

    {
        char pname[64];
        size_t max_batches =
            (size_t)env_i64("DARSHAN_MOFKA_MAX_BATCHES", 0, 0, 1024);
        snprintf(pname, sizeof(pname), "darshan-%ld", g_pid);
        dbg("init: topic opened; constructing producer '%s'", pname);
        g_producer = diaspora_producer_create(g_topic, pname,
                                              0 /* adaptive */, max_batches,
                                              DIASPORA_C_ORDERING_LOOSE);
    }
    if (g_producer == NULL) {
        WARN_ONCE("darshan-mofka: failed to initialize producer (%s); "
                  "records will not be streamed.\n", diaspora_c_last_error());
        diaspora_topic_destroy(g_topic);   g_topic = NULL;
        diaspora_driver_destroy(g_driver); g_driver = NULL;
        return;
    }

    /* expose through the connector struct so call-site guards keep
     * working with the same field names */
    mC.driver   = (void*)g_driver;
    mC.topic    = (void*)g_topic;
    mC.producer = (void*)g_producer;
    mC.mofka_lib = 1;
    mC.posix_enable_mofka = (getenv("DARSHAN_MOFKA_ENABLE_POSIX") != NULL) ? 1 : 0;
    mC.stdio_enable_mofka = (getenv("DARSHAN_MOFKA_ENABLE_STDIO") != NULL) ? 1 : 0;
    mC.mpiio_enable_mofka = (getenv("DARSHAN_MOFKA_ENABLE_MPIIO") != NULL) ? 1 : 0;
    mC.hdf5_enable_mofka  = (getenv("DARSHAN_MOFKA_ENABLE_HDF5")  != NULL) ? 1 : 0;

    if (g_verbose) {
        fprintf(stderr,
                "darshan-mofka: producer connected to topic '%s' via %s driver\n",
                topic_name, driver_name);
    }
    dbg("init: handles published to mC; SUCCESS");
}

/* ---------------------------------------------------------------------- */
/* send -- one record per instrumented I/O event                           */
/* VERIFY-D6: prototype EXACTLY matches darshan-mofka.h:100-107 (14 args). */
/* ---------------------------------------------------------------------- */
void darshan_mofka_connector_send(uint64_t record_id, int64_t rank,
                                  int64_t record_count, char* rwo,
                                  int64_t offset, int64_t length,
                                  int64_t max_byte, int64_t rw_switch,
                                  int64_t flushes,
                                  double start_time, double end_time,
                                  double total_time,
                                  char* mod_name, char* data_type)
{
    char buf[MOFKA_JSON_BUF];
    char file_esc[1024];
    char host_esc[300];
    const char* file_path;
    unsigned long long seq;
    int n;
    int rc;

    /* re-entrancy guard (AD-21 from impl.cpp:441-443): our own stderr
     * writes are instrumented I/O once STDIO sites are active. */
    if (g_in_send) return;
    g_in_send = 1;

    /* fast-path no-op when producer not connected */
    if (g_producer == NULL || mC.producer == NULL) {
        dbg("send: SKIP (producer not connected) op=%s mod=%s rec=%016llx",
            rwo ? rwo : "?", mod_name ? mod_name : "?",
            (unsigned long long)record_id);
        goto out;
    }

    /* circuit breaker (B layer) */
    if (atomic_load(&g_push_broken)) {
        dbg("send: SKIP (circuit broken) op=%s rec=%016llx",
            rwo ? rwo : "?", (unsigned long long)record_id);
        goto out;
    }

    /* stall absorption (B2 already-tripped path) */
    if (atomic_load(&g_push_stalled)) {
        dbg("send: SKIP (stalled) op=%s rec=%016llx",
            rwo ? rwo : "?", (unsigned long long)record_id);
        goto out;
    }

    /* defensive per-module enable check */
    if (!module_enabled_for(mod_name)) {
        dbg("send: SKIP (module disabled) op=%s mod=%s rec=%016llx",
            rwo ? rwo : "?", mod_name ? mod_name : "?",
            (unsigned long long)record_id);
        goto out;
    }

    /* S4 stall detector (B2 trip site). The ENTIRE mechanism lives in the
     * bindings -- diaspora_producer_oldest_pending_age() is a single
     * double (diaspora_c.h:97-106). Policy (threshold + trip action +
     * one-shot stderr line) stays in darshan. This is the design
     * improvement over impl.cpp's inline Future tracking: the C++
     * machinery is now where C++ lives (in the bindings) and pure-C
     * callers like darshan get a clean numeric signal. */
    if (g_stall_sec > 0) {
        double age = diaspora_producer_oldest_pending_age(g_producer);
        if (age > (double)g_stall_sec) {
            atomic_store(&g_push_stalled, 1);
            /* Fixed-string-stable trip line: test harnesses
             * (s4_stall_demo.sh) grep for "STALL detected" exactly. */
            fprintf(stderr,
                    "darshan-mofka: STALL detected: oldest push pending "
                    "%.3fs > %ds threshold; subsequent pushes will be "
                    "dropped silently.\n",
                    age, g_stall_sec);
            fflush(stderr);
            dbg("send: SKIP (stall trip first hit, age=%.3fs) op=%s rec=%016llx",
                age, rwo ? rwo : "?", (unsigned long long)record_id);
            goto out;
        }
    }

    /* macro FIRE breadcrumb (ledger column; identical wording to
     * impl.cpp:475-476 so the P0.2 five-column ledger keeps working) */
    dbg("send: FIRE op=%s mod=%s rec=%016llx",
        rwo ? rwo : "?", mod_name ? mod_name : "?",
        (unsigned long long)record_id);

    /* VERIFY-D7: file path lookup on the C side (the C++ era did this
     * in darshan-mofka.c before forwarding to impl.cpp; we do it inline
     * here). darshan_core_lookup_record_name takes a darshan_record_id;
     * returns a borrowed pointer into darshan's name hash valid for
     * this call's duration. NULL means "unknown record id"; the JSON
     * escape below handles NULL by substituting "unknown". */
    file_path = (const char*)darshan_core_lookup_record_name(record_id);
    json_escape_into(file_esc, sizeof(file_esc), file_path);
    json_escape_into(host_esc, sizeof(host_esc), g_hostname);

    seq = (unsigned long long)atomic_fetch_add(&g_seq, 1);

    /* VERIFY-D8: schema v2 JSON format string. THIS IS BYTE-EXACT FROM
     * impl.cpp's _send_impl (commit 8b24d294 has the F1 fix: task_id's
     * third positional arg is `seq` not `record_count` -- the bug that
     * collapsed 50 sends into 24 flowcept-mongo docs). Parity test
     * (P0.2 ledger) depends on this format being identical to the
     * vendored branch. Do NOT casually reflow this format string. */
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
        "\"started_at\":%.6f,\"ended_at\":%.6f,\"dur\":%.6f,\"total\":%.6f}",
        mod_name ? mod_name : "?",
        (unsigned long long)record_id, g_pid, seq,    /* task_id: rec, pid, SEQ (F1) */
        mod_name ? mod_name : "?",
        data_type ? data_type : "?",
        rwo ? rwo : "?",
        (unsigned long long)record_id, file_esc,
        host_esc, g_pid, (long long)g_uid, (long long)g_jobid,
        (long long)rank, seq, g_t0_epoch,
        (long long)record_count, (long long)offset, (long long)length,
        (long long)max_byte, (long long)rw_switch, (long long)flushes,
        start_time, end_time, end_time - start_time, total_time);

    if (n < 0 || (size_t)n >= sizeof(buf)) {
        dbg("send: DROP (json truncated, n=%d)", n);
        goto out;
    }

    rc = diaspora_producer_push(g_producer, buf, NULL, 0);
    if (rc == DIASPORA_C_OK) {
        dbg("send: PUSH ok (json_bytes=%d)", n);   /* parity-stable string */
    } else {
        /* push() returned an error code (the C++ throw equivalent).
         * Trip the circuit breaker (B layer), warn once, drop. */
        atomic_store(&g_push_broken, 1);
        WARN_ONCE("darshan-mofka: push failed (%s); circuit broken, "
                  "subsequent records will be dropped silently.\n",
                  diaspora_c_last_error());
        dbg("send: PUSH EXCEPTION (%s); tripping circuit breaker",
            diaspora_c_last_error());
    }

out:
    g_in_send = 0;
}

/* ---------------------------------------------------------------------- */
/* finalize -- called from darshan_core_shutdown (HAVE_MOFKA hook)         */
/* Patch D: bounded flush, leak on timeout. Idempotent.                    */
/* ---------------------------------------------------------------------- */
void darshan_mofka_connector_finalize(void)
{
    int64_t timeout_sec;
    int rc;

    dbg("finalize: ENTER (g_producer=%p mC.producer=%p)",
        (void*)g_producer, mC.producer);

    /* idempotent / never-initialized path */
    if (g_producer == NULL) {
        dbg("finalize: SKIP (idempotent: never fully initialized or already finalized)");
        goto clear;
    }

    /* breaker tripped: broker already known-dead; flushing would burn
     * the full timeout for nothing. Leak deliberately. */
    if (atomic_load(&g_push_broken)) {
        WARN_ONCE("darshan-mofka: circuit open; skipping flush; buffered "
                  "records dropped.\n");
        dbg("finalize: SKIP (breaker tripped) -> leak");
        goto leak;
    }

    timeout_sec = env_i64("DARSHAN_MOFKA_FLUSH_TIMEOUT_SEC", 5, 0, 3600);
    if (timeout_sec == 0) {
        WARN_ONCE("darshan-mofka: flush skipped (timeout=0); in-flight "
                  "records may be lost.\n");
        dbg("finalize: SKIP flush (timeout=0 explicitly requested); leaking handles");
        goto leak;
    }

    dbg("finalize: timeout_ms=%d", (int)(timeout_sec * 1000));
    rc = diaspora_producer_flush_timeout(g_producer, (int)(timeout_sec * 1000));
    if (rc == DIASPORA_C_OK) {
        dbg("finalize: flush CLEAN");
        /* clean teardown via the bindings' destructors. No C++ static-
         * destruction footgun here -- explicit C destroys are trivially
         * ordered (one of the structural wins of the retarget). */
        diaspora_producer_destroy(g_producer);
        diaspora_topic_destroy(g_topic);
        diaspora_driver_destroy(g_driver);
        goto clear;
    }

    if (rc == DIASPORA_C_TIMEOUT) {
        fprintf(stderr,
                "darshan-mofka: finalize: flush TIMEOUT after %llds; "
                "leaking handles to avoid blocking exit.\n",
                (long long)timeout_sec);
        dbg("finalize: flush TIMEOUT -> leak");
    } else {
        fprintf(stderr,
                "darshan-mofka: finalize: flush EXCEPTION (%s); "
                "leaking handles to avoid blocking exit.\n",
                diaspora_c_last_error());
        dbg("finalize: flush EXCEPTION -> leak");
    }

leak:   /* deliberate: skip destroys; OS reaps at exit (header guidance) */
clear:
    g_producer = NULL;
    g_topic = NULL;
    g_driver = NULL;
    mC.producer = NULL;
    mC.topic = NULL;
    mC.driver = NULL;
    dbg("finalize: DONE");
}

#else   /* !HAVE_MOFKA ------------------------------------------------------
         * Stubs so per-module call sites still link. Same shape as the
         * vendored branch's stubs (current darshan-mofka.c:118-152). */

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
                                  char* mod_name, char* data_type)
{
    (void)record_id; (void)rank;       (void)record_count; (void)rwo;
    (void)offset;    (void)length;     (void)max_byte;     (void)rw_switch;
    (void)flushes;   (void)start_time; (void)end_time;     (void)total_time;
    (void)mod_name;  (void)data_type;
}

void darshan_mofka_connector_finalize(void)
{
}

#endif /* HAVE_MOFKA */
