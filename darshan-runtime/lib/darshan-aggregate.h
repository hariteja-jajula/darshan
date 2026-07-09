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
    int posix_enable_mofka;
    int mpiio_enable_mofka;
    int stdio_enable_mofka;
    int hdf5_enable_mofka;
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
                                  char *mod_name, char *data_type);

void darshan_mofka_connector_finalize(void);

#ifdef __cplusplus
}
#endif

#endif /* __DARSHAN_MOFKA_H */
