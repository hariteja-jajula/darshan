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

extern "C" void darshan_mofka_connector_initialize_impl(struct darshan_core_runtime *init_core)
{
    (void)init_core;
    /* C2: open mofka driver, topic handle, and producer; store opaque
     * pointers in mC.driver / mC.topic / mC.producer. */
    return;
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
