/*
 * (C) 2026 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

/* NOTE: This translation unit is a vendored C<->C++ bridge between
 * Darshan's C module API (darshan-mofka.h/.c) and the C++-only
 * diaspora-stream-api / mofka client. Below the extern "C" layer it
 * is deliberately darshan-agnostic. It is proposed for upstream
 * extraction as official diaspora-stream-api C bindings; once those
 * are released, this file is deleted and darshan-mofka.c retargets
 * its forwarders with no ABI change. See the RFC. */

/*
 * C++ implementation backing darshan-mofka.c.
 *
 * The mofka client library is C++ only; this file isolates all C++
 * usage from the rest of darshan-runtime. Public entry points are
 * declared extern "C" so darshan-mofka.c can forward to them.
 *
 * Component status (see ldms_to_mofka_map.md for the full ladder):
 *   C1: build scaffolding only; all bodies were stubs        [shipped]
 *   C2: real producer setup at darshan_core_initialize       [shipped]
 *   C3: serialize and publish POSIX open records             [THIS COMMIT]
 *   C4: extend to POSIX read/write/close call sites
 *   C5: shutdown flush + STDIO/MPIIO/HDF5 call sites
 *
 * Design notes (cross-reference: ~/setup/notes/ldms_to_mofka_map.md
 * §3 and ~/setup/notes/mofka_api_verification.md):
 *
 *   * Send path is fire-and-forget: producer.push() returns a Future
 *     that we deliberately drop. Per producer.rst line 169, this is
 *     explicitly sanctioned by the Diaspora Stream API ("perfectly OK
 *     to drop the future if you do not care to wait for its
 *     completion"). The producer was constructed with
 *     BatchSize::Adaptive(), so the runtime batches small records
 *     automatically and grows the batch under backpressure.
 *
 *   * MaxNumBatches caveat: when too many batches are pending on the
 *     client side, push() blocks. We rely on the default MaxNumBatches
 *     for now; if bursty I/O loops show pushback in profiling, expose
 *     it as DARSHAN_MOFKA_MAX_BATCHES.
 *
 *   * Metadata-only at C3: we only populate the Mofka metadata slot
 *     (a JSON string), not the data slot. The metadata format
 *     deliberately matches darshan-ldms.c:253 so existing LDMS-aware
 *     consumers (including the post-hoc darshan_to_mofka.py path that
 *     flowcept already consumes) read our records without changes.
 *
 *   * Future-proofing for the data slot: when we later want to carry
 *     binary payloads (e.g. DXT trace segments, RDMA-bulk-transferred
 *     out of band), the diaspora::Producer::push(metadata, data)
 *     overload accepts a DataView. The DataView is non-owning per
 *     producer.rst line 126-141, so the caller must keep the buffer
 *     alive until the Future completes. For C3 we have no data
 *     payload so this is moot, but the public C signature should
 *     eventually grow (void* data, size_t data_size) parameters with
 *     the same lifetime contract documented at the call site.
 */

#ifdef HAVE_CONFIG_H
# include <darshan-runtime-config.h>
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>  /* pthread_atfork for fork() safety */
#include <time.h>     /* DEBUG: clock_gettime for breadcrumb timestamps */
#include <stdarg.h>   /* DEBUG: variadic dbg() helper */

#include <string>
#include <string_view>
#include <memory>
#include <mutex>      /* std::once_flag for one-shot atfork registration;
                       * std::mutex for S4 stall-detector adopt/clear */
#include <atomic>     /* std::atomic<bool> for push circuit breaker + stall */
#include <optional>   /* S4 stall-detector: std::optional<Future<...>> oldest */
#include <chrono>     /* S4 stall-detector: steady_clock for wallclock age */
#include <exception>

#include <diaspora/Driver.hpp>
#include <diaspora/TopicHandle.hpp>
#include <diaspora/Producer.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/Ordering.hpp>
#include <diaspora/BatchParams.hpp>
#include <diaspora/Future.hpp>   /* S4 stall-detector: Future<optional<EventID>> */
#include <diaspora/EventID.hpp>  /* S4: EventID type for the Future template arg */

extern "C" {
#include "darshan-mofka.h"
}

/*
 * NOTE: we deliberately do NOT include darshan.h from this C++
 * translation unit. darshan.h uses C11 <stdatomic.h> features
 * (atomic_flag, atomic_flag_test_and_set, atomic_flag_clear) inside
 * the __DARSHAN_CORE_LOCK / __DARSHAN_CORE_UNLOCK macros; those map
 * to C11-only types that the C++ frontend cannot parse.
 *
 * The mofka implementation does not need any darshan.h types beyond
 * the opaque forward declaration of `struct darshan_core_runtime`
 * (provided by darshan-mofka.h). If a future feature needs to read
 * fields out of darshan_core_runtime (e.g. mC.driver setup using
 * init_core->log_job_p->jobid like LDMS does), that work belongs in
 * the C side (darshan-mofka.c) which can safely include darshan.h.
 * The C side should extract the needed fields into a plain C struct
 * and pass it to the C++ implementation.
 */

namespace {

/*
 * Hidden C++ state held alongside the opaque void* handles in the C
 * struct `mC`. We keep the real shared_ptrs in file-scope statics so
 * their lifetime spans the whole process; mC.{driver,topic,producer}
 * are populated with their .get() addresses so the C side can
 * non-NULL-check them.
 *
 * NOTE: this is intentionally not thread-safe at construction time;
 * darshan_core_initialize() runs once during library load before any
 * threads are spawned by the application. Per-record sends from
 * multiple threads later use the producer object directly, which is
 * thread-safe per Mofka contract.
 */
struct MofkaState {
    diaspora::Driver       driver;
    diaspora::TopicHandle  topic;
    diaspora::Producer     producer;
};

static std::unique_ptr<MofkaState> g_state;

/*
 * Producer-side circuit breaker. The first push() failure flips this
 * permanently; all subsequent sends fast-path return without touching
 * the producer. Prevents a dead/slow broker from blocking the
 * application's I/O hot path when MaxNumBatches backpressure makes
 * push() itself block (not just throw). The application's I/O must
 * never fail because of telemetry.
 *
 * No re-arm path: once broken, broken for the lifetime of the process.
 * Re-arm is a documented followup (DARSHAN_MOFKA_REARM_SEC).
 */
static std::atomic<bool> push_broken{false};

/*
 * S4 stall detector ("B2" in the RFC three-layer story: B insurance /
 * B2 mid-run stall+memory bound / D exit backstop).
 *
 * Mechanism: track the OLDEST unacknowledged push as a Future<...>;
 * if its wallclock age exceeds DARSHAN_MOFKA_STALL_TIMEOUT_SEC (default
 * 5s; 0 disables), trip `push_stalled` permanently and short-circuit
 * subsequent pushes (same effect as the circuit breaker, but a separate
 * bit so the RFC's B-vs-B2 distinction stays greppable in the trip
 * stderr line).
 *
 * Adopt-only-when-empty (one Future tracked at a time -- always the
 * oldest unacked). Lazy clear inside the age check: if the tracked
 * Future has completed, reset and don't trip. operator bool() is
 * load-bearing -- completed() throws on default-constructed Futures
 * (Future.hpp:80-84); engaged push()-returned Futures never throw and
 * the call is a non-blocking atomic read (Promise.hpp:46-50 + 65-67;
 * verified by S4_MX_response.md). Mechanism + ranked design shapes:
 * see ~/setup/notes/agents/S4_HX_response.md + S4_MX_response.md.
 *
 * Earns RFC three-layer separation: when the stall trip fires, the
 * stderr line uses the literal string "STALL detected" -- the test
 * harness greps for this exact string to distinguish a stall trip
 * from a circuit-breaker trip ("push failed").
 */
static std::mutex                                              stall_mtx;
static std::optional<diaspora::Future<std::optional<diaspora::EventID>>> stall_oldest;
static std::chrono::steady_clock::time_point                  stall_oldest_t0{};
static std::atomic<bool>                                       push_stalled{false};

/* Resolve DARSHAN_MOFKA_STALL_TIMEOUT_SEC once, cache thereafter.
 * Same idiom as dbg_enabled() (:206-214). 0 disables; default 5s. */
static int stall_timeout_sec()
{
    static int cached = -2;  /* -2 = not yet read; -1 = disabled */
    if (cached == -2) {
        const char* v = std::getenv("DARSHAN_MOFKA_STALL_TIMEOUT_SEC");
        if (v == nullptr || *v == '\0') {
            cached = 5;
        } else {
            int n = std::atoi(v);
            if (n <= 0)        cached = -1;   /* disabled */
            else if (n > 3600) cached = 3600; /* clamp */
            else               cached = n;
        }
    }
    return cached;
}

/*
 * Schema v0 identity block (Patch A). Captured once at init; carried
 * in every record. Senior correction #3: without these, multi-node /
 * multi-tenant / cross-stream joins have no key. `file` is resolved
 * per-record on the C side; `pid` comes from getpid() at send time
 * (so post-fork children with rebuilt state carry the correct value
 * if they ever re-init).
 */
struct MofkaIdentity {
    int64_t uid;
    int64_t jobid;
    char    hostname[256];
    double  t0_epoch;       /* CLOCK_REALTIME seconds at init */
};
static MofkaIdentity g_identity = {0, 0, "?", 0.0};

/*
 * Per-producer monotonic sequence counter. Increments on every push
 * attempt (regardless of outcome). Lets a downstream consumer answer
 * "did I miss anything?" via gap detection on (pid, seq) pairs.
 * Senior correction: "one atomic increment, one query, forever."
 */
static std::atomic<uint64_t> g_seq{0};

/* read an env var, return default if unset or empty */
const char* env_or(const char* name, const char* default_value)
{
    const char* v = std::getenv(name);
    if (v == nullptr || *v == '\0') return default_value;
    return v;
}

/*
 * pthread_atfork child handler. After fork(), the child inherits the
 * unique_ptr<MofkaState> but the underlying thallium progress thread
 * did not survive the fork. Running ~Driver in the child would touch
 * a non-existent engine. Null the public handles so any subsequent
 * send() short-circuits, and release (NOT reset) g_state so the
 * destructor chain does not run. The child can re-init cleanly via
 * its own darshan_core_initialize() if needed.
 *
 * Async-signal-safety: this runs in the child after fork() returns.
 * No malloc, no exceptions, no I/O. unique_ptr::release() is a plain
 * pointer-store + nullification, safe in this context.
 */
extern "C" void darshan_mofka_atfork_child(void)
{
    mC.producer = nullptr;
    mC.topic    = nullptr;
    mC.driver   = nullptr;
    if (g_state) (void)g_state.release();
}

/* ===== DEBUG SCAFFOLDING (gated by DARSHAN_MOFKA_DEBUG) =====
 * Single helper used everywhere we want a breadcrumb. The env var is
 * read once on first call and cached so per-record overhead in the
 * hot path is one branch + one comparison when disabled. All output
 * goes to stderr, line-buffered with a flush so crashes don't lose
 * the last breadcrumb. Remove this whole block + all dbg() call
 * sites before upstream RFC. */
static int dbg_enabled()
{
    static int cached = -1;
    if (cached < 0) {
        const char* v = std::getenv("DARSHAN_MOFKA_DEBUG");
        cached = (v != nullptr && *v != '\0' && *v != '0') ? 1 : 0;
    }
    return cached;
}

__attribute__((format(printf, 1, 2)))
static void dbg(const char* fmt, ...)
{
    if (!dbg_enabled()) return;
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    struct tm tmv;
    localtime_r(&ts.tv_sec, &tmv);
    char tbuf[32];
    std::snprintf(tbuf, sizeof(tbuf), "%02d:%02d:%02d.%03ld",
                  tmv.tm_hour, tmv.tm_min, tmv.tm_sec, ts.tv_nsec / 1000000);
    std::fprintf(stderr, "darshan-mofka[DEBUG] [%s] pid=%ld ",
                 tbuf, (long)getpid());
    va_list ap;
    va_start(ap, fmt);
    std::vfprintf(stderr, fmt, ap);
    va_end(ap);
    std::fputc('\n', stderr);
    std::fflush(stderr);
}
/* ===== END DEBUG SCAFFOLDING ===== */

} // namespace

extern "C" void darshan_mofka_connector_initialize_impl(
    int64_t uid, int64_t jobid,
    const char *hostname,
    double t0_epoch)
{
    dbg("init: ENTER (ppid=%ld uid=%lld jobid=%lld host=%s t0=%.6f)",
        (long)getppid(), (long long)uid, (long long)jobid,
        hostname ? hostname : "?", t0_epoch);

    /* Cache identity for use in every send. These statics persist for
     * the lifetime of the process; init may be called more than once
     * across LD_PRELOAD-spawned subprocesses, but each subprocess gets
     * its own copy (file-scope statics are per-process). */
    g_identity.uid = uid;
    g_identity.jobid = jobid;
    if (hostname != nullptr) {
        std::strncpy(g_identity.hostname, hostname, sizeof(g_identity.hostname) - 1);
        g_identity.hostname[sizeof(g_identity.hostname) - 1] = '\0';
    } else {
        std::strcpy(g_identity.hostname, "?");
    }
    g_identity.t0_epoch = t0_epoch;

    /* Register fork() child handler once per process. Defensive against
     * Python multiprocessing / joblib / any other fork()-without-exec
     * caller that would inherit a half-dead producer state. */
    static std::once_flag atfork_registered;
    std::call_once(atfork_registered, [] {
        if (pthread_atfork(nullptr, nullptr, darshan_mofka_atfork_child) != 0) {
            dbg("init: WARNING pthread_atfork failed (fork-safety not installed)");
        } else {
            dbg("init: pthread_atfork child handler registered");
        }
    });

    /* env-driven per-module enables; mirrors LDMS pattern */
    if (std::getenv("DARSHAN_MOFKA_ENABLE_ALL")) {
        mC.posix_enable_mofka = 1;
        mC.mpiio_enable_mofka = 1;
        mC.stdio_enable_mofka = 1;
        mC.hdf5_enable_mofka  = 1;
    } else {
        mC.posix_enable_mofka = std::getenv("DARSHAN_MOFKA_ENABLE_POSIX") ? 1 : 0;
        mC.mpiio_enable_mofka = std::getenv("DARSHAN_MOFKA_ENABLE_MPIIO") ? 1 : 0;
        mC.stdio_enable_mofka = std::getenv("DARSHAN_MOFKA_ENABLE_STDIO") ? 1 : 0;
        mC.hdf5_enable_mofka  = std::getenv("DARSHAN_MOFKA_ENABLE_HDF5")  ? 1 : 0;
    }
    dbg("init: module enables posix=%d stdio=%d mpiio=%d hdf5=%d",
        mC.posix_enable_mofka, mC.stdio_enable_mofka,
        mC.mpiio_enable_mofka, mC.hdf5_enable_mofka);

    /* mofka connection parameters */
    const char* group_file = std::getenv("DARSHAN_MOFKA_GROUP_FILE");
    if (group_file == nullptr || *group_file == '\0') {
        dbg("init: ABORT (DARSHAN_MOFKA_GROUP_FILE unset)");
        std::fprintf(stderr,
            "darshan-mofka: DARSHAN_MOFKA_GROUP_FILE is not set; "
            "Mofka producer not started.\n");
        return;
    }
    const char* topic_name    = env_or("DARSHAN_MOFKA_TOPIC", "darshan");
    dbg("init: group_file=%s topic=%s", group_file, topic_name);

    /* per-process unique producer name (pid suffix avoids collisions
     * when N concurrent darshan-traced processes connect to the same
     * topic) */
    char producer_name[64];
    std::snprintf(producer_name, sizeof(producer_name),
                  "darshan-%ld", (long)getpid());

    try {
        /* Driver: { "group_file": "..." } JSON metadata */
        std::string metadata_json = std::string{"{\"group_file\":\""} + group_file + "\"}";
        diaspora::Metadata driver_options{metadata_json};

        dbg("init: creating MofkaState + Driver");
        g_state = std::make_unique<MofkaState>();
        g_state->driver   = diaspora::Driver::New("mofka", driver_options);
        dbg("init: Driver created; opening topic '%s'", topic_name);
        g_state->topic    = g_state->driver.openTopic(topic_name);
        dbg("init: topic opened; constructing producer '%s'", producer_name);
        /* Wrap producer_name in an explicit std::string_view: the
         * TopicHandle::producer() template uses decltype(auto)
         * deduction over its variadic options and cannot reconcile a
         * raw char array (char(&)[64]) against the expected default
         * string_view type. */
        /* Ordering::Loose: records carry timestamps and a per-producer
         * monotonic seq counter (Patch A) for ordering downstream;
         * Strict ordering would cost thread-pool parallelism with no
         * consumer needing that property. */
        g_state->producer = g_state->topic.producer(
                                std::string_view{producer_name},
                                diaspora::BatchSize::Adaptive(),
                                diaspora::Ordering::Loose);
        dbg("init: producer constructed");

        /* publish opaque handles to the C side; mC.producer != NULL is
         * the "we are connected" signal for darshan_mofka_connector_send */
        mC.driver   = static_cast<void*>(&g_state->driver);
        mC.topic    = static_cast<void*>(&g_state->topic);
        mC.producer = static_cast<void*>(&g_state->producer);
        dbg("init: handles published to mC; SUCCESS");

        /* Success log only when explicitly opted in: instrumented
         * apps' stderr is sacred upstream. Errors still print
         * unconditionally below. */
        if (std::getenv("DARSHAN_MOFKA_VERBOSE")) {
            std::fprintf(stderr,
                "darshan-mofka: producer '%s' connected to topic '%s' "
                "via group_file '%s'\n",
                producer_name, topic_name, group_file);
        }
    } catch (const std::exception& e) {
        dbg("init: EXCEPTION (%s); rolling back state", e.what());
        std::fprintf(stderr,
            "darshan-mofka: failed to initialize producer (%s); "
            "records will not be streamed.\n", e.what());
        g_state.reset();
        mC.driver   = nullptr;
        mC.topic    = nullptr;
        mC.producer = nullptr;
        return;
    }
}

/*
 * Per-module enable bitmask check. Returns 1 if records from
 * `mod_name` (e.g. "POSIX", "STDIO", "MPIIO", "H5F", "H5D") should
 * flow to mofka, 0 otherwise. Mirrors the LDMS pattern at
 * darshan-{posix,stdio,...}.c call sites where the enable check
 * happens before invoking the connector.
 *
 * (Currently called from darshan-posix.c via the inline guard
 * `if (mC.posix_enable_mofka)` so this helper is defensive — the
 * caller has already gated. Kept here in case a future call site
 * forgets the guard.)
 */
static int module_enabled_for(const char* mod_name)
{
    if (mod_name == nullptr) return 0;
    if (std::strcmp(mod_name, "POSIX") == 0) return mC.posix_enable_mofka;
    if (std::strcmp(mod_name, "MPIIO") == 0) return mC.mpiio_enable_mofka;
    if (std::strcmp(mod_name, "STDIO") == 0) return mC.stdio_enable_mofka;
    if (std::strcmp(mod_name, "H5F")   == 0) return mC.hdf5_enable_mofka;
    if (std::strcmp(mod_name, "H5D")   == 0) return mC.hdf5_enable_mofka;
    return 0;
}

/*
 * Escape a string for embedding in a JSON string literal. Handles "
 * and \ which are the two characters that can corrupt our snprintf'd
 * JSON if a path contains them (legal in POSIX filenames). Out buffer
 * grows up to 2x input length plus null terminator.
 *
 * Returns true on success, false if the output buffer is too small.
 * On failure, the output is set to "?" so the JSON stays well-formed.
 */
static bool json_escape(const char *in, char *out, size_t out_size)
{
    if (out_size == 0) return false;
    if (in == nullptr) {
        out[0] = '?'; if (out_size > 1) out[1] = '\0'; else out[0] = '\0';
        return true;
    }
    size_t o = 0;
    for (const char *p = in; *p; ++p) {
        char c = *p;
        if (c == '"' || c == '\\') {
            if (o + 2 >= out_size) { out[0]='?'; out[1]='\0'; return false; }
            out[o++] = '\\';
            out[o++] = c;
        } else if ((unsigned char)c < 0x20) {
            /* Control characters: emit \uXXXX. Cheap path: emit '?'. */
            if (o + 1 >= out_size) { out[0]='?'; out[1]='\0'; return false; }
            out[o++] = '?';
        } else {
            if (o + 1 >= out_size) { out[0]='?'; out[1]='\0'; return false; }
            out[o++] = c;
        }
    }
    if (o >= out_size) { out[0]='?'; out[1]='\0'; return false; }
    out[o] = '\0';
    return true;
}

extern "C" void darshan_mofka_connector_send_impl(uint64_t record_id, int64_t rank,
                                                  int64_t record_count, char *rwo,
                                                  int64_t offset, int64_t length,
                                                  int64_t max_byte, int64_t rw_switch,
                                                  int64_t flushes,
                                                  double start_time, double end_time,
                                                  double total_time,
                                                  char *mod_name, char *data_type,
                                                  const char *file_path)
{
    /* C5b re-entrancy guard (per architectural_decisions.md AD-21).
     * Our own stderr writes (dbg, breaker line) are themselves
     * instrumented I/O once STDIO sites are active; without this guard
     * a single push() can recurse through STDIO_RECORD_WRITE back into
     * send_impl. Thread-local so concurrent producer threads stay
     * independent. */
    static thread_local int in_send = 0;
    if (in_send) return;
    struct guard { guard() { in_send = 1; } ~guard() { in_send = 0; } } _g;

    /* fast-path no-op when producer not connected (init failed or the
     * user did not set DARSHAN_MOFKA_ENABLE) */
    if (g_state == nullptr || mC.producer == nullptr) {
        dbg("send: SKIP (producer not connected) op=%s mod=%s rec=%016llx",
            rwo ? rwo : "?", mod_name ? mod_name : "?",
            (unsigned long long)record_id);
        return;
    }

    /* Circuit breaker: once tripped (broker died, push threw), all
     * subsequent sends are silent no-ops. The application's I/O hot
     * path must not pay for a broken broker. */
    if (push_broken.load(std::memory_order_relaxed)) {
        dbg("send: SKIP (circuit broken) op=%s rec=%016llx",
            rwo ? rwo : "?", (unsigned long long)record_id);
        return;
    }

    /* S4 stall-detector ("B2"). Same hot-path short-circuit shape as
     * the breaker, separate bit so the trip line is greppable as
     * "STALL detected" rather than "push failed". Sibling check; one
     * extra relaxed atomic load per send when not tripped. */
    if (push_stalled.load(std::memory_order_relaxed)) {
        dbg("send: SKIP (stalled) op=%s rec=%016llx",
            rwo ? rwo : "?", (unsigned long long)record_id);
        return;
    }

    /* S4 stall-detector age check (runs IFF threshold > 0). Done before
     * the JSON build so a stalled producer doesn't even pay the snprintf
     * cost. Lazy clear: if the tracked Future has completed (or is
     * somehow invalid), reset stall_oldest and don't trip. Trip is
     * one-shot (static once_flag for the stderr line). */
    if (int sth = stall_timeout_sec(); sth > 0) {
        bool should_trip = false;
        double age_sec = 0.0;
        {
            std::lock_guard<std::mutex> lk(stall_mtx);
            if (stall_oldest) {
                /* operator bool guards completed() against the default-
                 * constructed-Future throw path (Future.hpp:80-84).
                 * For push()-returned Futures, completed() is a non-
                 * blocking atomic read into Promise's m_is_set
                 * (Promise.hpp:46-50, 65-67; verified MX-S4 §Q2). */
                if (!static_cast<bool>(*stall_oldest) ||
                    stall_oldest->completed())
                {
                    stall_oldest.reset();   /* lazy clear */
                } else {
                    std::chrono::duration<double> age =
                        std::chrono::steady_clock::now() - stall_oldest_t0;
                    age_sec = age.count();
                    if (age_sec > static_cast<double>(sth)) {
                        should_trip = true;
                    }
                }
            }
        }
        if (should_trip) {
            push_stalled.store(true, std::memory_order_relaxed);
            /* One-shot stderr line. Fixed-string-stable for harness grep:
             * "darshan-mofka: STALL detected". Do NOT mutate this prefix
             * without updating the S4 monitor scripts. */
            static std::once_flag stall_once;
            std::call_once(stall_once, [age_sec, sth] {
                std::fprintf(stderr,
                    "darshan-mofka: STALL detected: oldest push pending "
                    "%.3fs > %ds threshold; subsequent pushes will be "
                    "dropped silently.\n",
                    age_sec, sth);
            });
            dbg("send: SKIP (stall trip first hit, age=%.3fs) op=%s rec=%016llx",
                age_sec, rwo ? rwo : "?", (unsigned long long)record_id);
            return;
        }
    }

    /* defensive: per-module enable check (call sites already gate, but
     * the cost of strcmp on a short literal is negligible compared
     * to the JSON build + push below) */
    if (!module_enabled_for(mod_name)) {
        dbg("send: SKIP (module disabled) op=%s mod=%s rec=%016llx",
            rwo ? rwo : "?", mod_name ? mod_name : "?",
            (unsigned long long)record_id);
        return;
    }

    /* Phase 3 ledger: this breadcrumb is the 'macro_fires' column.
     * Count via: grep 'send: FIRE' workload-stderr.log | awk '{print $NF}' | sort | uniq -c */
    dbg("send: FIRE op=%s mod=%s rec=%016llx", rwo ? rwo : "?",
        mod_name ? mod_name : "?", (unsigned long long)record_id);

    /* sanitize nullable string args to avoid sprintf %s on NULL */
    const char* op_str    = (rwo       != nullptr) ? rwo       : "?";
    const char* mod_str   = (mod_name  != nullptr) ? mod_name  : "?";
    const char* dtype_str = (data_type != nullptr) ? data_type : "?";

    /*
     * Build an LDMS-format-compatible JSON record. This is the same
     * field layout as darshan-ldms.c:253. We preserve it verbatim at
     * C3 so:
     *   (a) existing flowcept consumers (which already deserialize the
     *       post-hoc darshan_to_mofka.py output, same JSON shape)
     *       handle our records with zero changes;
     *   (b) any future tool that already understands the LDMS wire
     *       format reads our stream too;
     *   (c) the diff for the eventual upstream PR is reviewable as
     *       "we publish the same records to a different sink", not
     *       "we reinvented the schema".
     *
     * Slim per-module schemas (drop HDF5 padding for POSIX records,
     * etc.) are an obvious follow-up but deliberately deferred to
     * keep C3 small.
     *
     * 1500 bytes is comfortably above the 1024-byte buffer LDMS uses
     * (line 208 of darshan-ldms.c) and well below any pathological
     * mofka batch limit. The largest reasonable JSON record at this
     * shape is ~700 bytes; doubling that gives headroom for unusual
     * file paths.
     */
    /*
     * Field naming matches the conventions of flowcept's
     * DocumentInserter.message_handler() which inspects msg["type"]
     * (must be "task" or "workflow" or None) and falls back to
     * inferring task-ness from the presence of "task_id" or
     * "activity_id". We set:
     *   "type": "task"             -> tells flowcept to dispatch as a task
     *   "activity_id": "darshan_POSIX" (etc.)
     *                              -> matches the post-hoc
     *                                 darshan_to_mofka.py convention
     *                                 so mongo task records look
     *                                 identical to the post-hoc path
     *   "task_id": "<record_id-pid-counter>"
     *                              -> deduplication key; flowcept
     *                                 requires it on task records
     *   "event_type": "<MET/MOD>"  -> what LDMS called "type"; we
     *                                 rename to avoid colliding with
     *                                 flowcept's "type" field
     *   "schema_version": 1        -> explicit wire-format version
     *
     * Everything else (file path, offset, length, counters, timings)
     * remains LDMS-style so future LDMS-aware tooling still parses it.
     */
    /* Patch A: identity block + per-producer monotonic seq.
     * Bumped to schema_version=2 since the wire layout changed:
     * v1 = C3 layout (LDMS fields + flowcept envelope)
     * v2 = v1 + hostname/job_id/uid/file/seq/t0_epoch */
    uint64_t my_seq = g_seq.fetch_add(1, std::memory_order_relaxed) + 1;

    char file_esc[1024];
    char host_esc[256];
    (void)json_escape(file_path, file_esc, sizeof(file_esc));
    (void)json_escape(g_identity.hostname, host_esc, sizeof(host_esc));

    char jbuf[2048];
    int n = std::snprintf(jbuf, sizeof(jbuf),
        "{"
          "\"type\":\"task\","
          "\"activity_id\":\"darshan_%s\","
          "\"task_id\":\"darshan-%llu-%ld-%lld\","
          "\"schema\":\"darshan_runtime\","
          "\"schema_version\":2,"
          /* identity block (schema v2): cross-stream correlation keys */
          "\"hostname\":\"%s\","
          "\"job_id\":%lld,"
          "\"pid\":%ld,"
          "\"uid\":%lld,"
          "\"file\":\"%s\","
          "\"seq\":%llu,"
          "\"t0_epoch\":%0.6f,"
          /* per-record fields */
          "\"module\":\"%s\","
          "\"event_type\":\"%s\","
          "\"op\":\"%s\","
          /* record_id is a 64-bit hash; emit as STRING so values with
           * the top bit set don't overflow BSON int64 in MongoDB. */
          "\"record_id\":\"%016llx\","
          "\"rank\":%lld,"
          "\"cnt\":%lld,"
          "\"max_byte\":%lld,"
          "\"switches\":%lld,"
          "\"flushes\":%lld,"
          "\"off\":%lld,"
          "\"len\":%lld,"
          "\"started_at\":%0.6f,"
          "\"ended_at\":%0.6f,"
          "\"dur\":%0.6f,"
          "\"total\":%0.6f,"
          "\"status\":\"FINISHED\""
        "}",
        mod_str,
        /* task_id is <record_id>-<pid>-<seq>; seq is the per-producer
         * monotonic counter from Patch A so each send is globally unique.
         * Using record_count here (per-record counter, often 1) caused
         * flowcept's task-upsert to collapse repeated ops on the same file
         * into a single mongo doc -- see C5_PHASE3_LEDGER.md Finding 3. */
        (unsigned long long)record_id, (long)getpid(), (long long)my_seq,
        /* identity block */
        host_esc,
        (long long)g_identity.jobid,
        (long)getpid(),
        (long long)g_identity.uid,
        file_esc,
        (unsigned long long)my_seq,
        g_identity.t0_epoch,
        /* per-record */
        mod_str, dtype_str, op_str,
        (unsigned long long)record_id,
        (long long)rank,
        (long long)record_count,
        (long long)max_byte,
        (long long)rw_switch,
        (long long)flushes,
        (long long)offset,
        (long long)length,
        start_time, end_time, end_time - start_time, total_time);

    if (n < 0 || n >= static_cast<int>(sizeof(jbuf))) {
        dbg("send: DROP (json truncated, n=%d)", n);
        /* truncated; safer to drop than to push a malformed record */
        return;
    }

    /*
     * Push as Metadata-only event. The Future returned by push() is
     * intentionally discarded — fire-and-forget, sanctioned by
     * producer.rst line 169. Mofka's adaptive batching coalesces
     * back-to-back pushes from the application I/O hot path into a
     * single network roundtrip per batch.
     *
     * Wrapped in try/catch because mofka push() can throw if
     * MaxNumBatches backpressure cannot be honored (e.g. broker
     * disappeared mid-run). We log once-per-failure and continue;
     * the host application's I/O must not fail because of telemetry.
     */
    try {
        auto* producer = static_cast<diaspora::Producer*>(mC.producer);
        diaspora::Metadata meta{std::string{jbuf}};
        auto fut = producer->push(meta);
        /* S4 stall-detector: adopt-only-when-empty. If we're already
         * tracking an oldest unacked Future, leave it (we want the
         * OLDEST, not the most recent -- otherwise a slow steady
         * stream of pushes would never trip). Cost: one mutex acquire
         * per push when no oldest tracked + one move-construct; zero
         * when oldest is engaged. Move-construct is cheap (Future is
         * two std::function slots = ~48 bytes; defaulted move).
         * Lifetime: Promise's m_state is a shared_ptr held inside the
         * Future's closures, so the Future outlives the producer if
         * needed (MX-S4 §Q5 architectural note). */
        if (stall_timeout_sec() > 0) {
            std::lock_guard<std::mutex> lk(stall_mtx);
            if (!stall_oldest) {
                stall_oldest = std::move(fut);
                stall_oldest_t0 = std::chrono::steady_clock::now();
            }
        }
        dbg("send: PUSH ok (json_bytes=%d)", n);
    } catch (const std::exception& e) {
        dbg("send: PUSH EXCEPTION (%s); tripping circuit breaker", e.what());
        /* Trip the circuit breaker permanently. First failure also
         * prints once to stderr; subsequent failures are absorbed by
         * the early-return check above (no log spam either). */
        push_broken.store(true, std::memory_order_relaxed);
        std::fprintf(stderr,
            "darshan-mofka: push failed (%s); circuit broken, subsequent "
            "records will be dropped silently.\n", e.what());
    }
}

/*
 * Flush in-flight batches at shutdown, bounded by
 * DARSHAN_MOFKA_FLUSH_TIMEOUT_SEC (default 5s; 0 = skip flush entirely).
 *
 * The wait() on Future<optional<Flushed>> returns an empty optional on
 * timeout (per producer.rst's optional-returning Future contract);
 * exceptions are reserved for invalid futures and transport errors.
 * On timeout OR exception, we leak the handles deliberately so their
 * destructors do not re-block on an unreachable broker. Idempotent.
 *
 * Idempotent skip path also releases (not resets) any half-built
 * g_state so a partially-failed init doesn't leave a Driver/Topic
 * destructor to run during static destruction, where ordering against
 * margo/argobots teardown is undefined.
 */
extern "C" void darshan_mofka_connector_finalize_impl(void)
{
    dbg("finalize: ENTER (g_state=%p mC.producer=%p)",
        (void*)g_state.get(), mC.producer);

    if (g_state == nullptr || mC.producer == nullptr) {
        dbg("finalize: SKIP (idempotent: never fully initialized or already finalized)");
        mC.producer = nullptr;
        mC.topic    = nullptr;
        mC.driver   = nullptr;
        /* If init half-succeeded (Driver up, producer not), do NOT let
         * unique_ptr's destructor run ~Driver during static destruction. */
        if (g_state) (void)g_state.release();
        return;
    }

    int timeout_ms = 5000;
    bool skip_flush = false;
    if (const char* env = std::getenv("DARSHAN_MOFKA_FLUSH_TIMEOUT_SEC")) {
        char* end = nullptr;
        long sec = std::strtol(env, &end, 10);
        if (end != env) {
            if (sec == 0) {
                skip_flush = true;
            } else if (sec > 0 && sec <= 3600) {
                timeout_ms = static_cast<int>(sec * 1000);
            }
        }
    }

    bool clean = false;

    if (skip_flush) {
        dbg("finalize: SKIP flush (timeout=0 explicitly requested); leaking handles");
        std::fprintf(stderr,
            "darshan-mofka: flush skipped at shutdown "
            "(DARSHAN_MOFKA_FLUSH_TIMEOUT_SEC=0); leaking handles, "
            "in-flight records will be lost.\n");
        /* clean stays false -> release path below */
    } else {
        dbg("finalize: timeout_ms=%d", timeout_ms);
        /* DEBUG: measure how long flush().wait() actually takes */
        struct timespec t0, t1;
        clock_gettime(CLOCK_MONOTONIC, &t0);

        try {
            auto* producer = static_cast<diaspora::Producer*>(mC.producer);
            dbg("finalize: calling producer->flush().wait(%d)", timeout_ms);
            auto flushed = producer->flush().wait(timeout_ms);
            clock_gettime(CLOCK_MONOTONIC, &t1);
            long elapsed_ms = (t1.tv_sec - t0.tv_sec) * 1000L
                            + (t1.tv_nsec - t0.tv_nsec) / 1000000L;
            if (flushed.has_value()) {
                clean = true;
                dbg("finalize: flush CLEAN in %ldms", elapsed_ms);
            } else {
                /* Empty optional = timeout per documented contract.
                 * Leak handles; broker may be unreachable. */
                dbg("finalize: flush TIMEOUT after %ldms (empty optional)", elapsed_ms);
                std::fprintf(stderr,
                    "darshan-mofka: flush timed out after %dms; "
                    "leaking handles to avoid blocking exit. "
                    "In-flight records may be lost.\n", timeout_ms);
            }
        } catch (const std::exception& e) {
            clock_gettime(CLOCK_MONOTONIC, &t1);
            long elapsed_ms = (t1.tv_sec - t0.tv_sec) * 1000L
                            + (t1.tv_nsec - t0.tv_nsec) / 1000000L;
            dbg("finalize: flush EXCEPTION after %ldms (%s)", elapsed_ms, e.what());
            std::fprintf(stderr,
                "darshan-mofka: flush failed at shutdown (%s); "
                "leaking handles to avoid blocking exit.\n", e.what());
        } catch (...) {
            clock_gettime(CLOCK_MONOTONIC, &t1);
            long elapsed_ms = (t1.tv_sec - t0.tv_sec) * 1000L
                            + (t1.tv_nsec - t0.tv_nsec) / 1000000L;
            dbg("finalize: flush UNKNOWN-THROW after %ldms", elapsed_ms);
            std::fprintf(stderr,
                "darshan-mofka: flush threw at shutdown; "
                "leaking handles to avoid blocking exit.\n");
        }
    }

    mC.producer = nullptr;
    mC.topic    = nullptr;
    mC.driver   = nullptr;
    dbg("finalize: nulled mC handles; %s g_state",
        clean ? "reset" : "release (deliberate leak)");

    if (clean) {
        g_state.reset();
    } else {
        (void)g_state.release();  /* deliberate; OS reaps at exit */
    }
    dbg("finalize: DONE");
}
