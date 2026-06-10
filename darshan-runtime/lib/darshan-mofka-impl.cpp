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
 * C1 status: function bodies are stubs that do nothing. C2 will add
 * the real driver / topic / producer setup. C3 onward adds the
 * serialization and publish path.
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

extern "C" void darshan_mofka_connector_send_impl(uint64_t record_id, int64_t rank,
                                                  int64_t record_count, char *rwo,
                                                  int64_t offset, int64_t length,
                                                  int64_t max_byte, int64_t rw_switch,
                                                  int64_t flushes,
                                                  double start_time, double end_time,
                                                  double total_time,
                                                  char *mod_name, char *data_type)
{
    (void)record_id;  (void)rank;        (void)record_count; (void)rwo;
    (void)offset;     (void)length;      (void)max_byte;     (void)rw_switch;
    (void)flushes;    (void)start_time;  (void)end_time;     (void)total_time;
    (void)mod_name;   (void)data_type;
    /* C3/C4: serialize record and producer->push it. */
    return;
}

extern "C" void darshan_mofka_connector_finalize_impl(void)
{
    /* C5: producer->flush(); destroy handles in order; reset
     * mC.producer = NULL so any later send() calls become no-ops. */
    return;
}
