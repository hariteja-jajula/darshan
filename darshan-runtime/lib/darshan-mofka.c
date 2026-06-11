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
#include <time.h>       /* clock_gettime for t0_epoch */
#include <unistd.h>     /* gethostname */
#include "darshan-mofka.h"
#include "darshan.h"

/*
 * C-side public interface for the mofka connector module.
 *
 * The body of each function is implemented in darshan-mofka-impl.cpp
 * (mofka is C++ only at the time of writing; this file keeps the
 * Darshan source tree's public ABI in C). Forwarders below call into
 * the C++ implementation through extern "C" symbols.
 *
 * When --with-mofka is NOT given to configure, HAVE_MOFKA is undefined
 * and the stub bodies at the bottom of this file are used. The stubs
 * match the signatures so per-module call sites compile either way.
 */

#ifdef HAVE_MOFKA

/* the single global connector state; opaque handles inside */
struct darshanMofkaConnector mC = {
    .mofka_lib = 1,
    .driver = NULL,
    .topic = NULL,
    .producer = NULL,
};

/*
 * extern "C" entry points implemented in darshan-mofka-impl.cpp.
 *
 * Patch A: the C side extracts darshan-canonical identity (uid, jobid)
 * from init_core and resolves record_id -> file path via
 * darshan_core_lookup_record_name (which requires darshan.h, hence
 * lives C-side). The C++ side receives the resolved values and is
 * spared having to know about darshan_core_runtime internals.
 */
extern void darshan_mofka_connector_initialize_impl(
    int64_t uid, int64_t jobid,
    const char *hostname,
    double t0_epoch);

extern void darshan_mofka_connector_send_impl(uint64_t record_id, int64_t rank,
                                              int64_t record_count, char *rwo,
                                              int64_t offset, int64_t length,
                                              int64_t max_byte, int64_t rw_switch,
                                              int64_t flushes,
                                              double start_time, double end_time,
                                              double total_time,
                                              char *mod_name, char *data_type,
                                              const char *file_path);
extern void darshan_mofka_connector_finalize_impl(void);

void darshan_mofka_connector_initialize(struct darshan_core_runtime *init_core)
{
    /* Identity extraction: uid + jobid come from the darshan-canonical
     * source (init_core->log_job_p); hostname from gethostname();
     * t0_epoch from CLOCK_REALTIME at init. All cached C++ side so
     * every record carries them. */
    int64_t uid    = init_core ? init_core->log_job_p->uid   : (int64_t)0;
    int64_t jobid  = init_core ? init_core->log_job_p->jobid : (int64_t)0;

    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) != 0)
        snprintf(hostname, sizeof(hostname), "unknown");
    hostname[sizeof(hostname) - 1] = '\0';

    struct timespec ts;
    double t0_epoch = 0.0;
    if (clock_gettime(CLOCK_REALTIME, &ts) == 0)
        t0_epoch = (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;

    darshan_mofka_connector_initialize_impl(uid, jobid, hostname, t0_epoch);
}

void darshan_mofka_connector_send(uint64_t record_id, int64_t rank,
                                  int64_t record_count, char *rwo,
                                  int64_t offset, int64_t length,
                                  int64_t max_byte, int64_t rw_switch,
                                  int64_t flushes,
                                  double start_time, double end_time,
                                  double total_time,
                                  char *mod_name, char *data_type)
{
    /* Resolve record_id -> file path on the C side (the C++ impl
     * cannot include darshan.h). Returns NULL for unknown ids; the
     * C++ side handles that. The returned pointer is borrowed into
     * darshan-core's name hash; we pass it through to the C++ impl
     * which uses it synchronously within this call. */
    const char *file_path = (const char *)darshan_core_lookup_record_name(record_id);

    darshan_mofka_connector_send_impl(record_id, rank, record_count, rwo,
                                      offset, length, max_byte, rw_switch,
                                      flushes, start_time, end_time, total_time,
                                      mod_name, data_type, file_path);
}

void darshan_mofka_connector_finalize(void)
{
    darshan_mofka_connector_finalize_impl();
}

#else

/* mofka not built in: stubs so per-module call sites still link */

struct darshanMofkaConnector mC = {
    .mofka_lib = 0,
};

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
                                  char *mod_name, char *data_type)
{
    (void)record_id;  (void)rank;        (void)record_count; (void)rwo;
    (void)offset;     (void)length;      (void)max_byte;     (void)rw_switch;
    (void)flushes;    (void)start_time;  (void)end_time;     (void)total_time;
    (void)mod_name;   (void)data_type;
    return;
}

void darshan_mofka_connector_finalize(void)
{
    return;
}

#endif /* HAVE_MOFKA */
