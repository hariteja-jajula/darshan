/*
 * (C) 2026 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#ifndef __DARSHAN_MOFKA_H
#define __DARSHAN_MOFKA_H

#include <stdint.h>

struct darshan_core_runtime;

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

/* Stream the FINAL state of every in-memory module record (opt-in via DARSHAN_MOFKA_FINAL_SWEEP=1).
 * Call at finalize BEFORE module buffers are freed. reconstruct max-seq dedup makes re-sending
 * already-streamed records harmless; this recovers records whose ops were never streamed live.
 * DISABLED BY DEFAULT: enabling it hangs python-ml at shutdown (mofka progress-pool send stall);
 * see the KNOWN ISSUE note on the definition in darshan-mofka.c. */
void darshan_mofka_connector_flush_records(struct darshan_core_runtime *core);

#ifdef __cplusplus
}
#endif

/* One API, LDMS-style: modules call darshan_mofka_connector_send() directly
 * (no DARSHAN_MOFKA_SEND macro). The !HAVE_MOFKA compile-out lives in a stubbed
 * function body in darshan-mofka.c, exactly like darshan_ldms_connector_send(). */

#endif /* __DARSHAN_MOFKA_H */
