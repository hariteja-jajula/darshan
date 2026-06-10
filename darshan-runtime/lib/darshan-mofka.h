/*
 * (C) 2026 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#ifndef __DARSHAN_MOFKA_H
#define __DARSHAN_MOFKA_H

#include <stdint.h>

/*
 * Forward declaration only. We deliberately do NOT include darshan.h
 * here because darshan.h uses C11 <stdatomic.h> features in its lock
 * macros (atomic_flag, atomic_flag_test_and_set) that the C++ frontend
 * cannot parse. Translation units that need the full definition of
 * darshan_core_runtime should include darshan.h directly; this header
 * only needs the forward declaration to use it in a pointer parameter.
 */
struct darshan_core_runtime;

/*
 * Mofka integration for Darshan.
 *
 * This module is structurally analogous to darshan-ldms.{c,h}: it
 * provides an optional runtime path that streams Darshan I/O records
 * into a Mochi-based Mofka topic as they are recorded, in addition to
 * (not in place of) the standard end-of-run binary log.
 *
 * The Mofka client API is C++ only at the time of this writing
 * (mofka 0.9.x). To keep Darshan's public C ABI clean, this header
 * declares C-callable functions; their bodies in darshan-mofka.c are
 * thin forwarders to a C++ implementation in darshan-mofka-impl.cpp.
 * All mofka handles are held as opaque (void *) pointers in the
 * darshanMofkaConnector struct so that no C++ types leak into C
 * translation units.
 */

#ifdef HAVE_MOFKA
#include <pthread.h>

typedef struct darshanMofkaConnector {
    int mofka_lib;              /* 1 if built with mofka, 0 otherwise */
    int posix_enable_mofka;
    int mpiio_enable_mofka;
    int stdio_enable_mofka;
    int hdf5_enable_mofka;
    /* opaque handles owned by the C++ implementation */
    void *driver;
    void *topic;
    void *producer;
    pthread_mutex_t ln_lock;
} darshanMofkaConnector;

#else

typedef struct darshanMofkaConnector {
    int mofka_lib;              /* always 0 when mofka not built in */
    int posix_enable_mofka;
    int mpiio_enable_mofka;
    int stdio_enable_mofka;
    int hdf5_enable_mofka;
} darshanMofkaConnector;

#endif /* HAVE_MOFKA */

extern struct darshanMofkaConnector mC;

#ifdef __cplusplus
extern "C" {
#endif

/*
 * darshan_mofka_connector_initialize()
 *
 * Called from darshan_core_initialize() when DARSHAN_MOFKA_ENABLE is set
 * in the environment. Establishes the mofka driver / topic / producer
 * handles, reads DARSHAN_MOFKA_GROUP_FILE, DARSHAN_MOFKA_TOPIC, and
 * other DARSHAN_MOFKA_* environment variables to configure the producer.
 *
 * On failure (broker unreachable, missing group file, etc.) leaves
 * mC.producer == NULL so that subsequent darshan_mofka_connector_send()
 * calls become no-ops. Failures are logged via darshan_core_fprintf
 * but never abort the host application.
 */
void darshan_mofka_connector_initialize(struct darshan_core_runtime *init_core);

/*
 * darshan_mofka_connector_send()
 *
 * Called from per-module instrumentation (POSIX, STDIO, MPI-IO, HDF5)
 * at every traced I/O operation. Serializes one record and publishes
 * it on the configured mofka topic. The argument list mirrors
 * darshan_ldms_connector_send() exactly so call sites can mechanically
 * add a mofka call alongside the LDMS one.
 *
 * If mC.producer == NULL (mofka disabled or never connected) this
 * function returns immediately.
 */
void darshan_mofka_connector_send(uint64_t record_id, int64_t rank,
                                  int64_t record_count, char *rwo,
                                  int64_t offset, int64_t length,
                                  int64_t max_byte, int64_t rw_switch,
                                  int64_t flushes,
                                  double start_time, double end_time,
                                  double total_time,
                                  char *mod_name, char *data_type);

/*
 * darshan_mofka_connector_finalize()
 *
 * Called from darshan_core_shutdown() to flush in-flight records and
 * destroy mofka handles cleanly. Required because mofka producers
 * buffer messages and need an explicit flush before destruction;
 * without this hook, in-flight records between the last send and
 * process exit are lost (and the destroy-mid-batch path can segfault
 * the producer thread).
 *
 * After return mC.producer == NULL so any later send() calls are
 * no-ops. Safe to call when mC.producer is already NULL.
 */
void darshan_mofka_connector_finalize(void);

#ifdef __cplusplus
}
#endif

#endif /* __DARSHAN_MOFKA_H */
