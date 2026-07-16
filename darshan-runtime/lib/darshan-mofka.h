/*
 * (C) 2026 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#ifndef __DARSHAN_MOFKA_H
#define __DARSHAN_MOFKA_H

#include <stdint.h>

struct darshan_core_runtime;

typedef struct darshanMofkaConnector {
    int mofka_lib;
} darshanMofkaConnector;

extern struct darshanMofkaConnector mC;

#ifdef __cplusplus
extern "C" {
#endif

void darshan_mofka_connector_initialize(struct darshan_core_runtime *init_core);

void darshan_mofka_connector_send(uint64_t record_id, int64_t rank,
                                  int64_t record_count, char *rwo,
                                  int64_t offset, int64_t length,
                                  int64_t max_byte, int64_t rw_switch,
                                  int64_t flushes,
                                  double start_time, double end_time,
                                  double total_time,
                                  char *mod_name, char *data_type,
                                  const void *rec, uint64_t rec_size);

void darshan_mofka_connector_finalize(void);

#ifdef __cplusplus
}
#endif

#ifdef HAVE_MOFKA
#define DARSHAN_MOFKA_SEND(...) darshan_mofka_connector_send(__VA_ARGS__)
#else
#define DARSHAN_MOFKA_SEND(...) do {} while(0)
#endif

#endif /* __DARSHAN_MOFKA_H */
