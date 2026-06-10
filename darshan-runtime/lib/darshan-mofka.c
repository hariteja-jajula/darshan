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

/* extern "C" entry points implemented in darshan-mofka-impl.cpp */
extern void darshan_mofka_connector_initialize_impl(struct darshan_core_runtime *init_core);
extern void darshan_mofka_connector_send_impl(uint64_t record_id, int64_t rank,
                                              int64_t record_count, char *rwo,
                                              int64_t offset, int64_t length,
                                              int64_t max_byte, int64_t rw_switch,
                                              int64_t flushes,
                                              double start_time, double end_time,
                                              double total_time,
                                              char *mod_name, char *data_type);
extern void darshan_mofka_connector_finalize_impl(void);

void darshan_mofka_connector_initialize(struct darshan_core_runtime *init_core)
{
    darshan_mofka_connector_initialize_impl(init_core);
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
    darshan_mofka_connector_send_impl(record_id, rank, record_count, rwo,
                                      offset, length, max_byte, rw_switch,
                                      flushes, start_time, end_time, total_time,
                                      mod_name, data_type);
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
