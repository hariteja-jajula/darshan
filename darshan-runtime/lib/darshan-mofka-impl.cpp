/*
 * (C) 2026 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

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

#include <string>
#include <string_view>
#include <memory>
#include <exception>

#include <diaspora/Driver.hpp>
#include <diaspora/TopicHandle.hpp>
#include <diaspora/Producer.hpp>
#include <diaspora/Metadata.hpp>
#include <diaspora/Ordering.hpp>
#include <diaspora/BatchParams.hpp>

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

/* read an env var, return default if unset or empty */
const char* env_or(const char* name, const char* default_value)
{
    const char* v = std::getenv(name);
    if (v == nullptr || *v == '\0') return default_value;
    return v;
}

} // namespace

extern "C" void darshan_mofka_connector_initialize_impl(struct darshan_core_runtime *init_core)
{
    (void)init_core; /* C5: read jobid / uid / exepath from here for record context */

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

    /* mofka connection parameters */
    const char* group_file = std::getenv("DARSHAN_MOFKA_GROUP_FILE");
    if (group_file == nullptr || *group_file == '\0') {
        std::fprintf(stderr,
            "darshan-mofka: DARSHAN_MOFKA_GROUP_FILE is not set; "
            "Mofka producer not started.\n");
        return;
    }
    const char* topic_name    = env_or("DARSHAN_MOFKA_TOPIC", "darshan");

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

        g_state = std::make_unique<MofkaState>();
        g_state->driver   = diaspora::Driver::New("mofka", driver_options);
        g_state->topic    = g_state->driver.openTopic(topic_name);
        /* Wrap producer_name in an explicit std::string_view: the
         * TopicHandle::producer() template uses decltype(auto)
         * deduction over its variadic options and cannot reconcile a
         * raw char array (char(&)[64]) against the expected default
         * string_view type. */
        g_state->producer = g_state->topic.producer(
                                std::string_view{producer_name},
                                diaspora::BatchSize::Adaptive(),
                                diaspora::Ordering::Strict);

        /* publish opaque handles to the C side; mC.producer != NULL is
         * the "we are connected" signal for darshan_mofka_connector_send */
        mC.driver   = static_cast<void*>(&g_state->driver);
        mC.topic    = static_cast<void*>(&g_state->topic);
        mC.producer = static_cast<void*>(&g_state->producer);

        std::fprintf(stderr,
            "darshan-mofka: producer '%s' connected to topic '%s' "
            "via group_file '%s'\n",
            producer_name, topic_name, group_file);
    } catch (const std::exception& e) {
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

extern "C" void darshan_mofka_connector_send_impl(uint64_t record_id, int64_t rank,
                                                  int64_t record_count, char *rwo,
                                                  int64_t offset, int64_t length,
                                                  int64_t max_byte, int64_t rw_switch,
                                                  int64_t flushes,
                                                  double start_time, double end_time,
                                                  double total_time,
                                                  char *mod_name, char *data_type)
{
    /* fast-path no-op when producer not connected (init failed or the
     * user did not set DARSHAN_MOFKA_ENABLE) */
    if (g_state == nullptr || mC.producer == nullptr) return;

    /* defensive: per-module enable check (call sites already gate, but
     * the cost of strcmp on a short literal is negligible compared
     * to the JSON build + push below) */
    if (!module_enabled_for(mod_name)) return;

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
    char jbuf[1500];
    int n = std::snprintf(jbuf, sizeof(jbuf),
        "{"
          "\"type\":\"task\","
          "\"activity_id\":\"darshan_%s\","
          "\"task_id\":\"darshan-%llu-%ld-%lld\","
          "\"schema\":\"darshan_runtime\","
          "\"schema_version\":1,"
          "\"module\":\"%s\","
          "\"event_type\":\"%s\","
          "\"op\":\"%s\","
          /* record_id is a 64-bit hash; emit as STRING so values with
           * the top bit set don't overflow BSON int64 in MongoDB.
           * Mongo's BSON int is signed 8-byte; darshan hashes routinely
           * exceed 2^63. Renderable as hex for grep-ability. */
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
        (unsigned long long)record_id, (long)getpid(), (long long)record_count,
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
        (void)producer->push(meta);  /* Future intentionally dropped */
    } catch (const std::exception& e) {
        /*
         * Don't spam: only print the first failure per process.
         * Subsequent failures are silently dropped so a flapping
         * broker doesn't flood the application's stderr.
         */
        static bool reported = false;
        if (!reported) {
            std::fprintf(stderr,
                "darshan-mofka: push failed (%s); further failures suppressed.\n",
                e.what());
            reported = true;
        }
    }
}

extern "C" void darshan_mofka_connector_finalize_impl(void)
{
    /* C5: producer->flush(); destroy handles in order; reset
     * mC.producer = NULL so any later send() calls become no-ops. */
    return;
}
