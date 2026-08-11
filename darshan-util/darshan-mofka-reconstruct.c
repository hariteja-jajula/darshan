/*
 * Copyright (C) 2026 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 * darshan-mofka-reconstruct.c -- reconstruct per-process .darshan logs from
 * Darshan->Mofka JSONL captures.
 *
 * Input is the JSONL produced by server/capture.py in the parent demo repo.
 * The tool keeps the latest rec_hex snapshot for each (module, record_id, rank, pid),
 * groups records by producing process (pid), and writes ONE native-style .darshan log
 * per process into the output directory -- mirroring native Darshan's per-process log
 * output for any workload (non-MPI C, python, MPI). pid is part of the key because
 * non-MPI Darshan reports rank 0 for every process, so keying on rank alone would
 * collapse all processes that touch the same filenames into one record.
 *
 * Usage: darshan-mofka-reconstruct <events.jsonl> <output_directory>
 */
#ifdef HAVE_CONFIG_H
# include "darshan-util-config.h"
#endif

#define _GNU_SOURCE
#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

#include "uthash-1.9.2/src/uthash.h"
#include "darshan-logutils.h"

struct rec_key
{
    int mod_id;
    uint64_t record_id;
    int64_t rank;
    int64_t pid;    /* per-producer identity: keeps distinct processes' final records
                     * separate. Non-MPI Darshan stamps rank 0 on every process, so
                     * (mod,record_id,rank) alone collapses N processes writing the same
                     * filenames into one record; adding pid mirrors native's N per-process
                     * logs (each a separate rank-0 record that pydarshan then sums). For
                     * real MPI each rank has a distinct rank AND pid, so this is a no-op. */
};

struct stream_record
{
    struct rec_key key;
    void *buf;
    size_t len;
    unsigned long long seq;
    double ended_at;
    UT_hash_handle hlink;
};

/* Per-process job metadata. Native Darshan writes one log per process, each with
 * that process's own uid/jobid/start_time/hostname/exe+mounts. The stream carries
 * all processes interleaved, so we key job info by pid (uthash) and reconstruct one
 * log per pid -- mirroring native's per-process output for every workload type. */
struct job_info
{
    int64_t pid;          /* hash key: the producing process (-1 for the legacy/global entry) */
    int have_uid;
    int have_jobid;
    int64_t uid;
    int64_t jobid;
    double start_time;
    double end_time;
    char hostname[256];
    char exemnt[4096];    /* core's exe+mounts buffer: "<exe>\n<type>\t<path>..." */
    UT_hash_handle hlink;
};

static int hex_value(int c);   /* fwd decl: used by json_get_string's \u case */

static void usage(const char *prog)
{
    fprintf(stderr, "Usage: %s <events.jsonl> <output_directory>\n", prog);
    fprintf(stderr,
        "  Reconstructs one .darshan log per producing process (pid) found in the\n"
        "  JSONL stream, mirroring native Darshan's per-process log output. Files are\n"
        "  named like native logs and written into <output_directory> (created if absent).\n");
}

/* Look up (or create) the per-pid job_info entry in the hash. pid is the hash key. */
static struct job_info *job_for_pid(struct job_info **jobs, int64_t pid)
{
    struct job_info *j;
    HASH_FIND(hlink, *jobs, &pid, sizeof(int64_t), j);
    if(j) return j;
    j = calloc(1, sizeof(*j));
    if(!j) return NULL;
    j->pid = pid;
    HASH_ADD(hlink, *jobs, pid, sizeof(int64_t), j);
    return j;
}

static void free_jobs(struct job_info *jobs)
{
    struct job_info *j, *tmp;
    HASH_ITER(hlink, jobs, j, tmp)
    {
        HASH_DELETE(hlink, jobs, j);
        free(j);
    }
}

/* FNV-1a 64-bit: a small self-contained deterministic hash for the log filename's
 * "logmod" field. Native computes logmod as a Jenkins hash of the hostname seeded by
 * the microsecond wall-clock time at log-open (shutdown) -- that seed is random and is
 * NOT recorded in the stream, so native's exact value cannot be reproduced. We instead
 * derive a stable, native-looking 64-bit number from data we DO have (hostname + the
 * process's recorded start epoch in microseconds), so reconstructed names are unique
 * per process and reproducible from the same stream. */
static uint64_t fnv1a64(const void *data, size_t len, uint64_t seed)
{
    const unsigned char *p = (const unsigned char *)data;
    uint64_t h = 1469598103934665603ULL ^ seed;
    size_t i;
    for(i = 0; i < len; i++)
    {
        h ^= p[i];
        h *= 1099511628211ULL;
    }
    return h;
}

/* Basename of the exe command line stored in exemnt ("<exe> <args>\n<mounts...>").
 * Native uses glibc __progname == basename(argv[0]); mirror that: take the first
 * whitespace-delimited token, then its trailing path component. Writes into out. */
static void exe_progname(const char *exemnt, char *out, size_t outsz)
{
    const char *end, *slash;
    size_t n;

    out[0] = '\0';
    if(!exemnt || !*exemnt) { snprintf(out, outsz, "unknown"); return; }

    /* first token: up to first space or newline */
    end = exemnt;
    while(*end && *end != ' ' && *end != '\t' && *end != '\n') end++;

    /* basename within [exemnt, end) */
    slash = end;
    while(slash > exemnt && slash[-1] != '/') slash--;

    n = (size_t)(end - slash);
    if(n == 0 || n >= outsz) { snprintf(out, outsz, "unknown"); return; }
    memcpy(out, slash, n);
    out[n] = '\0';
}

/* Reconstruct native Darshan's per-process log filename as faithfully as the stream
 * allows (darshan-core.c darshan_get_logfile_name):
 *   <cuser>_<progname>_id<jobid>-<pid>_<mon>-<mday>-<secs_of_day>-<logmod>_1.darshan
 * cuser: resolved from uid via getpwuid (native uses LOGNAME; uid->name matches on the
 *        same system), falling back to the numeric uid. date fields: localtime of the
 *        process start epoch (verified to match native exactly). logmod: see fnv1a64
 *        (native's true value is unreproducible). trailing _N: native's near-universal 1. */
static void build_native_logname(char *out, size_t outsz, const struct job_info *j)
{
    char cuser[256];
    char progname[256];
    struct tm *lt;
    time_t st;
    uint64_t logmod;
    int secs_of_day;

    /* cuser */
    if(j->have_uid && j->uid >= 0)
    {
        struct passwd *pw = getpwuid((uid_t)j->uid);
        if(pw && pw->pw_name && *pw->pw_name)
            snprintf(cuser, sizeof(cuser), "%s", pw->pw_name);
        else
            snprintf(cuser, sizeof(cuser), "%u", (unsigned)j->uid);
    }
    else
    {
        snprintf(cuser, sizeof(cuser), "unknown");
    }

    exe_progname(j->exemnt, progname, sizeof(progname));

    st = (time_t)(j->start_time > 0.0 ? j->start_time : time(NULL));
    lt = localtime(&st);
    if(!lt) { time_t now = time(NULL); lt = localtime(&now); }
    secs_of_day = lt->tm_hour * 3600 + lt->tm_min * 60 + lt->tm_sec;

    logmod = fnv1a64(j->hostname, strlen(j->hostname),
        (uint64_t)(j->start_time * 1000000.0));

    snprintf(out, outsz,
        "%s_%s_id%lld-%lld_%d-%d-%d-%" PRIu64 "_1.darshan",
        cuser, progname,
        (long long)(j->have_jobid ? j->jobid : -1),
        (long long)j->pid,
        lt->tm_mon + 1, lt->tm_mday, secs_of_day, logmod);
}

static const char *json_find_key(const char *line, const char *key)
{
    char needle[128];
    snprintf(needle, sizeof(needle), "\"%s\":", key);
    return strstr(line, needle);
}

/* Return a pointer to a key's value, positioned just past the ':' and any
 * whitespace, or NULL if the key (or its colon) is absent. Shared prologue for
 * all the json_get_* scalar/string extractors. */
static const char *json_find_value(const char *line, const char *key)
{
    const char *p = json_find_key(line, key);
    if(!p) return NULL;
    p = strchr(p, ':');
    if(!p) return NULL;
    p++;
    while(*p && isspace((unsigned char)*p)) p++;
    return p;
}

static char *json_get_string(const char *line, const char *key)
{
    const char *p = json_find_value(line, key);
    char *out;
    size_t cap, n;

    if(!p) return NULL;
    if(*p != '"') return NULL;
    p++;

    cap = strlen(p) + 1;
    out = malloc(cap);
    if(!out) return NULL;
    n = 0;

    while(*p)
    {
        if(*p == '"')
            break;
        if(*p == '\\' && p[1])
        {
            p++;
            switch(*p)
            {
                case '"': out[n++] = '"'; break;
                case '\\': out[n++] = '\\'; break;
                case '/': out[n++] = '/'; break;
                case 'b': out[n++] = '\b'; break;
                case 'f': out[n++] = '\f'; break;
                case 'n': out[n++] = '\n'; break;
                case 'r': out[n++] = '\r'; break;
                case 't': out[n++] = '\t'; break;
                case 'u':
                {
                    /* \uXXXX -> UTF-8. capture.py's json.dumps uses the default
                     * ensure_ascii=True, so every non-ASCII filename byte arrives
                     * here as \uXXXX; without this it decoded to the literal
                     * "uXXXX" (e.g. "resume" from an accented name). UTF-8 output
                     * is always shorter than the \uXXXX input, so out[] (sized
                     * strlen(p)+1) never overflows. */
                    int h0, h1, h2, h3;
                    unsigned int cp;
                    if(p[1] && p[2] && p[3] && p[4] &&
                       (h0 = hex_value((unsigned char)p[1])) >= 0 &&
                       (h1 = hex_value((unsigned char)p[2])) >= 0 &&
                       (h2 = hex_value((unsigned char)p[3])) >= 0 &&
                       (h3 = hex_value((unsigned char)p[4])) >= 0)
                    {
                        cp = (unsigned int)((h0 << 12) | (h1 << 8) | (h2 << 4) | h3);
                        p += 4;   /* consume the 4 hex digits (outer p++ eats 'u') */
                        /* high surrogate: try to pair with a following \uXXXX */
                        if(cp >= 0xD800 && cp <= 0xDBFF &&
                           p[1] == '\\' && p[2] == 'u' &&
                           p[3] && p[4] && p[5] && p[6] &&
                           (h0 = hex_value((unsigned char)p[3])) >= 0 &&
                           (h1 = hex_value((unsigned char)p[4])) >= 0 &&
                           (h2 = hex_value((unsigned char)p[5])) >= 0 &&
                           (h3 = hex_value((unsigned char)p[6])) >= 0)
                        {
                            unsigned int lo = (unsigned int)((h0 << 12) | (h1 << 8) | (h2 << 4) | h3);
                            if(lo >= 0xDC00 && lo <= 0xDFFF)
                            {
                                cp = 0x10000 + (((cp - 0xD800) << 10) | (lo - 0xDC00));
                                p += 6;   /* consume "\uXXXX" of the low surrogate */
                            }
                        }
                        if(cp < 0x80)
                            out[n++] = (char)cp;
                        else if(cp < 0x800)
                        {
                            out[n++] = (char)(0xC0 | (cp >> 6));
                            out[n++] = (char)(0x80 | (cp & 0x3F));
                        }
                        else if(cp < 0x10000)
                        {
                            out[n++] = (char)(0xE0 | (cp >> 12));
                            out[n++] = (char)(0x80 | ((cp >> 6) & 0x3F));
                            out[n++] = (char)(0x80 | (cp & 0x3F));
                        }
                        else
                        {
                            out[n++] = (char)(0xF0 | (cp >> 18));
                            out[n++] = (char)(0x80 | ((cp >> 12) & 0x3F));
                            out[n++] = (char)(0x80 | ((cp >> 6) & 0x3F));
                            out[n++] = (char)(0x80 | (cp & 0x3F));
                        }
                    }
                    else
                    {
                        out[n++] = *p;   /* malformed \u: keep old passthrough */
                    }
                    break;
                }
                default: out[n++] = *p; break;
            }
            p++;
        }
        else
        {
            out[n++] = *p++;
        }
    }

    out[n] = '\0';
    return out;
}

static int json_get_i64(const char *line, const char *key, int64_t *out)
{
    const char *p = json_find_value(line, key);
    char *end = NULL;
    long long v;

    if(!p) return 0;
    v = strtoll(p, &end, 10);
    if(end == p) return 0;
    *out = (int64_t)v;
    return 1;
}

static int json_get_u64(const char *line, const char *key, uint64_t *out)
{
    const char *p = json_find_value(line, key);
    char *end = NULL;
    unsigned long long v;

    if(!p) return 0;
    v = strtoull(p, &end, 10);
    if(end == p) return 0;
    *out = (uint64_t)v;
    return 1;
}

static int json_get_u64_hex_or_dec(const char *line, const char *key, uint64_t *out)
{
    char *s = json_get_string(line, key);
    char *end = NULL;
    unsigned long long v;

    if(s)
    {
        v = strtoull(s, &end, 16);
        if(end != s)
        {
            *out = (uint64_t)v;
            free(s);
            return 1;
        }
        free(s);
    }

    return json_get_u64(line, key, out);
}

static int json_get_double(const char *line, const char *key, double *out)
{
    const char *p = json_find_value(line, key);
    char *end = NULL;
    double v;

    if(!p) return 0;
    v = strtod(p, &end);
    if(end == p) return 0;
    *out = v;
    return 1;
}

/* Epoch seconds from a key that may be a number OR an ISO datetime string. The
 * consumer (FlowCept) rewrites started_at/ended_at into "YYYY-MM-DD HH:MM:SS.ffffff"
 * (UTC), so a plain numeric parse fails; fall back to strptime + timegm + fraction. */
static int json_get_epoch(const char *line, const char *key, double *out)
{
    if(json_get_double(line, key, out)) return 1;   /* still numeric */
    char *s = json_get_string(line, key);
    if(!s) return 0;
    struct tm tm;
    memset(&tm, 0, sizeof(tm));
    const char *dot = strchr(s, '.');
    double frac = dot ? atof(dot) : 0.0;             /* ".ffffff" -> fractional seconds */
    char *end = strptime(s, "%Y-%m-%dT%H:%M:%S", &tm);
    if(!end) { memset(&tm, 0, sizeof(tm)); end = strptime(s, "%Y-%m-%d %H:%M:%S", &tm); }
    free(s);
    if(!end) return 0;
    time_t base = timegm(&tm);
    if(base == (time_t)-1) return 0;
    *out = (double)base + frac;
    return 1;
}

static int hex_value(int c)
{
    if(c >= '0' && c <= '9') return c - '0';
    if(c >= 'a' && c <= 'f') return c - 'a' + 10;
    if(c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

static int module_name_to_id(const char *module)
{
    int i;
    if(!module) return -1;
    /* the producer emits "MPIIO" but darshan_module_names[] stores "MPI-IO" */
    if(strcmp(module, "MPIIO") == 0) return DARSHAN_MPIIO_MOD;
    for(i = 0; i < DARSHAN_KNOWN_MODULE_COUNT; i++)
        if(strcmp(module, darshan_module_names[i]) == 0) return i;
    return -1;
}

static int expected_record_size(int mod_id)
{
    switch(mod_id)
    {
        case DARSHAN_POSIX_MOD: return sizeof(struct darshan_posix_file);
        case DARSHAN_STDIO_MOD: return sizeof(struct darshan_stdio_file);
        case DARSHAN_MPIIO_MOD: return sizeof(struct darshan_mpiio_file);
        case DARSHAN_H5F_MOD: return sizeof(struct darshan_hdf5_file);
        case DARSHAN_H5D_MOD: return sizeof(struct darshan_hdf5_dataset);
        default: return -1;
    }
}

/* ---- PER-OP COUNTER ACCUMULATOR -----------------------------------------
 *
 * The stream no longer carries a close-time raw-record snapshot or counters[]/
 * fcounters[] arrays. Instead the connector emits ONLY the slim per-op envelope
 * (op/offset/len/started_at/ended_at + identity) for EVERY op, and we DERIVE the
 * per-file counters here by accumulating those events -- mirroring the runtime's
 * own per-op counter updates in darshan-posix.c / darshan-stdio.c.
 *
 * This (a) removes the snapshot from the stream and (b) fixes the "files still open
 * at exit have no close snapshot" gap: a record's counters come from the ops we
 * already saw, so a file that never closes still yields a full record.
 *
 * Keyed by (mod_id, record_id, rank, pid) -- same identity write_log groups by.
 * DARSHAN_BUCKET_INC bucket edges and the SEQ/CONSEC/MAX_BYTE logic are copied
 * verbatim from the runtime so the derived subset matches native bit-for-bit. */

/* the 10 DARSHAN_BUCKET_INC size buckets; bucket index for a byte count. */
static int size_bucket(int64_t v)
{
    if(v < 101) return 0;
    if(v < 1025) return 1;
    if(v < 10241) return 2;
    if(v < 102401) return 3;
    if(v < 1048577) return 4;
    if(v < 4194305) return 5;
    if(v < 10485761) return 6;
    if(v < 104857601) return 7;
    if(v < 1073741825) return 8;
    return 9;
}

/* Track up to this many distinct access sizes per record for the ACCESS1-4/
 * COUNT top-4 (native tracks 32 at runtime; 64 is ample for a derived report). */
#define ACC_MAX_TRACK 64

struct acc_record
{
    struct rec_key key;

    int64_t opens, reads, writes, seeks, fdopens, flushes;
    int64_t bytes_read, bytes_written;
    int64_t max_byte_read, max_byte_written;
    int64_t rw_switches;
    int64_t size_read[10], size_write[10];

    /* consec/seq tracking (mirror runtime rec_ref->last_byte_{read,write}) */
    int64_t last_byte_read, last_byte_written;
    int64_t consec_reads, consec_writes, seq_reads, seq_writes;
    int have_last_read, have_last_write;

    /* cumulative + max timers and first/last timestamps */
    double read_time, write_time, meta_time;
    double max_read_time, max_write_time;
    int64_t max_read_time_size, max_write_time_size;   /* POSIX only */
    double open_start, open_end, read_start, read_end;
    double write_start, write_end, close_start, close_end;
    int have_open_ts, have_read_ts, have_write_ts, have_close_ts;

    /* last op class for rw-switch detection: 0=none, 1=read, 2=write */
    int last_io_type;

    /* distinct access-size frequency table for ACCESS1-4 */
    int64_t acc_val[ACC_MAX_TRACK];
    int64_t acc_cnt[ACC_MAX_TRACK];
    int acc_n;

    UT_hash_handle hlink;
};

static struct acc_record *acc_lookup(struct acc_record **accs, int mod_id,
    uint64_t record_id, int64_t rank, int64_t pid)
{
    struct rec_key key;
    struct acc_record *a;

    memset(&key, 0, sizeof(key));
    key.mod_id = mod_id;
    key.record_id = record_id;
    key.rank = rank;
    key.pid = pid;

    HASH_FIND(hlink, *accs, &key, sizeof(key), a);
    if(a) return a;
    a = calloc(1, sizeof(*a));
    if(!a) return NULL;
    a->key = key;
    HASH_ADD(hlink, *accs, key, sizeof(a->key), a);
    return a;
}

/* Record a distinct access-size occurrence for the ACCESS1-4 top-4. */
static void acc_track_size(struct acc_record *a, int64_t sz)
{
    int i;
    for(i = 0; i < a->acc_n; i++)
        if(a->acc_val[i] == sz) { a->acc_cnt[i]++; return; }
    if(a->acc_n < ACC_MAX_TRACK)
    {
        a->acc_val[a->acc_n] = sz;
        a->acc_cnt[a->acc_n] = 1;
        a->acc_n++;
    }
}

/* Fold one per-op event into its record's accumulator. op/offset/len/timestamps
 * come straight from the slim envelope. Called once per streamed op event. */
static void acc_update(struct acc_record *a, const char *op,
    int64_t offset, int64_t len, double started, double ended)
{
    double dur = (ended > started) ? (ended - started) : 0.0;
    int is_read = 0, is_write = 0, is_open = 0, is_close = 0, is_seek = 0, is_stat = 0;

    if(!op) return;

    /* classify: match the op strings the wrappers emit. refopen counts as an open. */
    if(strcmp(op, "read") == 0) is_read = 1;
    else if(strcmp(op, "write") == 0) is_write = 1;
    else if(strcmp(op, "open") == 0 || strcmp(op, "refopen") == 0) is_open = 1;
    else if(strcmp(op, "close") == 0) is_close = 1;
    else if(strcmp(op, "seek") == 0) is_seek = 1;
    else if(strcmp(op, "stat") == 0) is_stat = 1;
    else return;   /* FINAL / unknown ops carry no derivable counter */

    if(is_read)
    {
        if(len < 0) len = 0;
        /* seq/consec (runtime: this_offset vs last_byte_read) */
        if(offset >= 0)
        {
            if(a->have_last_read && offset > a->last_byte_read) a->seq_reads++;
            if(a->have_last_read && offset == a->last_byte_read + 1) a->consec_reads++;
            a->last_byte_read = offset + len - 1;
            a->have_last_read = 1;
            if(a->max_byte_read < offset + len - 1) a->max_byte_read = offset + len - 1;
        }
        a->bytes_read += len;
        a->reads++;
        a->size_read[size_bucket(len)]++;
        acc_track_size(a, len);
        a->read_time += dur;
        if(dur > a->max_read_time) { a->max_read_time = dur; a->max_read_time_size = len; }
        if(!a->have_read_ts || started < a->read_start) a->read_start = started;
        a->read_end = ended; a->have_read_ts = 1;
        if(a->last_io_type == 2) a->rw_switches++;
        a->last_io_type = 1;
    }
    else if(is_write)
    {
        if(len < 0) len = 0;
        if(offset >= 0)
        {
            if(a->have_last_write && offset > a->last_byte_written) a->seq_writes++;
            if(a->have_last_write && offset == a->last_byte_written + 1) a->consec_writes++;
            a->last_byte_written = offset + len - 1;
            a->have_last_write = 1;
            if(a->max_byte_written < offset + len - 1) a->max_byte_written = offset + len - 1;
        }
        a->bytes_written += len;
        a->writes++;
        a->size_write[size_bucket(len)]++;
        acc_track_size(a, len);
        a->write_time += dur;
        if(dur > a->max_write_time) { a->max_write_time = dur; a->max_write_time_size = len; }
        if(!a->have_write_ts || started < a->write_start) a->write_start = started;
        a->write_end = ended; a->have_write_ts = 1;
        if(a->last_io_type == 1) a->rw_switches++;
        a->last_io_type = 2;
    }
    else if(is_open)
    {
        a->opens++;
        a->meta_time += dur;
        if(!a->have_open_ts || started < a->open_start) a->open_start = started;
        a->open_end = ended; a->have_open_ts = 1;
    }
    else if(is_close)
    {
        a->meta_time += dur;
        if(!a->have_close_ts || started < a->close_start) a->close_start = started;
        a->close_end = ended; a->have_close_ts = 1;
    }
    else if(is_seek)
    {
        a->seeks++;
        a->meta_time += dur;   /* seeks charge meta time in the runtime */
    }
    else if(is_stat)
    {
        /* stat/lstat/fstat: charges meta time. The current wrappers do not emit a
         * "stat" op, so this branch is dormant; kept so POSIX_STATS becomes derivable
         * the moment a stat op is streamed. (POSIX_STATS itself left 0 until then.) */
        a->meta_time += dur;
    }
}

/* Copy the top-4 most frequent access sizes (by count, then by value desc, matching
 * DARSHAN_UPDATE_COMMON_VAL_COUNTERS ordering) into acc_dst[0..3] / cnt_dst[0..3]. */
static void acc_emit_top4(const struct acc_record *a,
    int64_t *acc_dst, int64_t *cnt_dst)
{
    int used[ACC_MAX_TRACK];
    int i, slot;
    memset(used, 0, sizeof(used));
    for(slot = 0; slot < 4; slot++)
    {
        int best = -1;
        for(i = 0; i < a->acc_n; i++)
        {
            if(used[i]) continue;
            if(best < 0 ||
               a->acc_cnt[i] > a->acc_cnt[best] ||
               (a->acc_cnt[i] == a->acc_cnt[best] && a->acc_val[i] > a->acc_val[best]))
                best = i;
        }
        if(best < 0) break;
        used[best] = 1;
        acc_dst[slot] = a->acc_val[best];
        cnt_dst[slot] = a->acc_cnt[best];
    }
}

/* Materialize an accumulated POSIX/STDIO record into a native module struct.
 * Returns a malloc'd buffer of expected_record_size(mod_id), or NULL. Only the
 * DERIVABLE counters are set; the rest stay 0 (Darshan's default for untouched). */
static void *build_record_from_acc(const struct acc_record *a, size_t exp_size,
    size_t *out_len)
{
    char *buf = calloc(1, exp_size);
    struct darshan_base_record *base;
    if(!buf) return NULL;

    base = (struct darshan_base_record *)buf;
    base->id = (darshan_record_id)a->key.record_id;
    base->rank = a->key.rank;

    if(a->key.mod_id == DARSHAN_POSIX_MOD)
    {
        struct darshan_posix_file *r = (struct darshan_posix_file *)buf;
        int64_t *c = r->counters;
        double *f = r->fcounters;

        c[POSIX_OPENS] = a->opens;
        c[POSIX_READS] = a->reads;
        c[POSIX_WRITES] = a->writes;
        c[POSIX_SEEKS] = a->seeks;
        c[POSIX_BYTES_READ] = a->bytes_read;
        c[POSIX_BYTES_WRITTEN] = a->bytes_written;
        c[POSIX_MAX_BYTE_READ] = a->max_byte_read;
        c[POSIX_MAX_BYTE_WRITTEN] = a->max_byte_written;
        c[POSIX_CONSEC_READS] = a->consec_reads;
        c[POSIX_CONSEC_WRITES] = a->consec_writes;
        c[POSIX_SEQ_READS] = a->seq_reads;
        c[POSIX_SEQ_WRITES] = a->seq_writes;
        c[POSIX_RW_SWITCHES] = a->rw_switches;
        c[POSIX_MAX_READ_TIME_SIZE] = a->max_read_time_size;
        c[POSIX_MAX_WRITE_TIME_SIZE] = a->max_write_time_size;
        { int i; for(i = 0; i < 10; i++) {
            c[POSIX_SIZE_READ_0_100 + i] = a->size_read[i];
            c[POSIX_SIZE_WRITE_0_100 + i] = a->size_write[i]; } }
        acc_emit_top4(a, &c[POSIX_ACCESS1_ACCESS], &c[POSIX_ACCESS1_COUNT]);

        f[POSIX_F_OPEN_START_TIMESTAMP] = a->have_open_ts ? a->open_start : 0.0;
        f[POSIX_F_OPEN_END_TIMESTAMP] = a->have_open_ts ? a->open_end : 0.0;
        f[POSIX_F_READ_START_TIMESTAMP] = a->have_read_ts ? a->read_start : 0.0;
        f[POSIX_F_READ_END_TIMESTAMP] = a->have_read_ts ? a->read_end : 0.0;
        f[POSIX_F_WRITE_START_TIMESTAMP] = a->have_write_ts ? a->write_start : 0.0;
        f[POSIX_F_WRITE_END_TIMESTAMP] = a->have_write_ts ? a->write_end : 0.0;
        f[POSIX_F_CLOSE_START_TIMESTAMP] = a->have_close_ts ? a->close_start : 0.0;
        f[POSIX_F_CLOSE_END_TIMESTAMP] = a->have_close_ts ? a->close_end : 0.0;
        f[POSIX_F_READ_TIME] = a->read_time;
        f[POSIX_F_WRITE_TIME] = a->write_time;
        f[POSIX_F_META_TIME] = a->meta_time;
        f[POSIX_F_MAX_READ_TIME] = a->max_read_time;
        f[POSIX_F_MAX_WRITE_TIME] = a->max_write_time;

        /* single-process (nprocs=1) log: this rank is both fastest and slowest */
        c[POSIX_FASTEST_RANK] = base->rank;
        c[POSIX_SLOWEST_RANK] = base->rank;
        c[POSIX_FASTEST_RANK_BYTES] = a->bytes_read + a->bytes_written;
        c[POSIX_SLOWEST_RANK_BYTES] = a->bytes_read + a->bytes_written;
        f[POSIX_F_FASTEST_RANK_TIME] = a->read_time + a->write_time + a->meta_time;
        f[POSIX_F_SLOWEST_RANK_TIME] = a->read_time + a->write_time + a->meta_time;
    }
    else if(a->key.mod_id == DARSHAN_STDIO_MOD)
    {
        struct darshan_stdio_file *r = (struct darshan_stdio_file *)buf;
        int64_t *c = r->counters;
        double *f = r->fcounters;

        c[STDIO_OPENS] = a->opens;
        c[STDIO_READS] = a->reads;
        c[STDIO_WRITES] = a->writes;
        c[STDIO_SEEKS] = a->seeks;
        c[STDIO_FLUSHES] = a->flushes;
        c[STDIO_BYTES_WRITTEN] = a->bytes_written;
        c[STDIO_BYTES_READ] = a->bytes_read;
        c[STDIO_MAX_BYTE_READ] = a->max_byte_read;
        c[STDIO_MAX_BYTE_WRITTEN] = a->max_byte_written;

        f[STDIO_F_META_TIME] = a->meta_time;
        f[STDIO_F_WRITE_TIME] = a->write_time;
        f[STDIO_F_READ_TIME] = a->read_time;
        f[STDIO_F_OPEN_START_TIMESTAMP] = a->have_open_ts ? a->open_start : 0.0;
        f[STDIO_F_OPEN_END_TIMESTAMP] = a->have_open_ts ? a->open_end : 0.0;
        f[STDIO_F_CLOSE_START_TIMESTAMP] = a->have_close_ts ? a->close_start : 0.0;
        f[STDIO_F_CLOSE_END_TIMESTAMP] = a->have_close_ts ? a->close_end : 0.0;
        f[STDIO_F_WRITE_START_TIMESTAMP] = a->have_write_ts ? a->write_start : 0.0;
        f[STDIO_F_WRITE_END_TIMESTAMP] = a->have_write_ts ? a->write_end : 0.0;
        f[STDIO_F_READ_START_TIMESTAMP] = a->have_read_ts ? a->read_start : 0.0;
        f[STDIO_F_READ_END_TIMESTAMP] = a->have_read_ts ? a->read_end : 0.0;

        c[STDIO_FASTEST_RANK] = base->rank;
        c[STDIO_SLOWEST_RANK] = base->rank;
        c[STDIO_FASTEST_RANK_BYTES] = a->bytes_read + a->bytes_written;
        c[STDIO_SLOWEST_RANK_BYTES] = a->bytes_read + a->bytes_written;
        f[STDIO_F_FASTEST_RANK_TIME] = a->read_time + a->write_time + a->meta_time;
        f[STDIO_F_SLOWEST_RANK_TIME] = a->read_time + a->write_time + a->meta_time;
    }
    else
    {
        free(buf);
        return NULL;
    }

    *out_len = exp_size;
    return buf;
}

static void free_accs(struct acc_record *accs)
{
    struct acc_record *a, *tmp;
    HASH_ITER(hlink, accs, a, tmp)
    {
        HASH_DELETE(hlink, accs, a);
        free(a);
    }
}

/* Return 1 if a record should be pruned from the reconstructed log.
 *
 * This mirrors native Darshan's own filtering in stdio_output()
 * (darshan-runtime/lib/darshan-stdio.c): it drops ONLY the <STDIN>/<STDOUT>/
 * <STDERR> STDIO stream records when they had no read or write activity
 * (STDIO_READS == 0 && STDIO_WRITES == 0). No other records -- including
 * regular files and other modules -- are pruned by native Darshan, so we
 * do not prune them here either.
 *
 * We identify the three streams by name rather than by record id: the util
 * cannot call the runtime's darshan_core_gen_record_id(), and matching the
 * "<STDIN>"/"<STDOUT>"/"<STDERR>" name Darshan assigns them is equivalent. */
static int record_is_empty(int mod_id, const void *buf, const char *name)
{
    const struct darshan_stdio_file *r;

    if(mod_id != DARSHAN_STDIO_MOD || !name)
        return 0;

    if(strcmp(name, "<STDIN>") != 0 &&
       strcmp(name, "<STDOUT>") != 0 &&
       strcmp(name, "<STDERR>") != 0)
        return 0;

    r = buf;
    return (r->counters[STDIO_READS] == 0 && r->counters[STDIO_WRITES] == 0);
}

/* Look up a record's file name from the name hash by id. */
static const char *lookup_record_name(struct darshan_name_record_ref *name_hash,
    uint64_t id)
{
    struct darshan_name_record_ref *ref;
    HASH_FIND(hlink, name_hash, &id, sizeof(darshan_record_id), ref);
    return ref ? ref->name_record->name : NULL;
}

/* Build a darshan_name_record_ref directly (the hash darshan_log_put_namehash
 * consumes), mirroring darshan's own deserializer at darshan-logutils.c:1030-1049.
 * This replaces the old two-stage id->name / name->ref hashing. */
static void add_name_record(struct darshan_name_record_ref **hash,
    uint64_t id, const char *name)
{
    struct darshan_name_record_ref *ref;
    size_t rec_len;

    if(!name || !*name) return;
    HASH_FIND(hlink, *hash, &id, sizeof(darshan_record_id), ref);
    if(ref) return;

    rec_len = sizeof(darshan_record_id) + strlen(name) + 1;
    ref = calloc(1, sizeof(*ref));
    if(!ref) return;
    ref->name_record = malloc(rec_len);
    if(!ref->name_record)
    {
        free(ref);
        return;
    }
    ref->name_record->id = (darshan_record_id)id;
    memcpy(ref->name_record->name, name, strlen(name) + 1);
    HASH_ADD(hlink, *hash, name_record->id, sizeof(darshan_record_id), ref);
}

static int should_replace(struct stream_record *old, unsigned long long seq, double ended_at)
{
    if(!old) return 1;
    if(seq > old->seq) return 1;
    if(seq == old->seq && ended_at > old->ended_at) return 1;
    return 0;
}

static void add_record(struct stream_record **records, int mod_id, uint64_t record_id,
    int64_t rank, int64_t pid, void *buf, size_t len, unsigned long long seq,
    double ended_at)
{
    struct rec_key key;
    struct stream_record *ent;

    memset(&key, 0, sizeof(key));
    key.mod_id = mod_id;
    key.record_id = record_id;
    key.rank = rank;
    key.pid = pid;

    HASH_FIND(hlink, *records, &key, sizeof(key), ent);
    if(!should_replace(ent, seq, ended_at))
    {
        free(buf);
        return;
    }

    if(!ent)
    {
        ent = calloc(1, sizeof(*ent));
        if(!ent)
        {
            free(buf);
            return;
        }
        ent->key = key;
        HASH_ADD(hlink, *records, key, sizeof(ent->key), ent);
    }
    else
    {
        free(ent->buf);
    }

    ent->buf = buf;
    ent->len = len;
    ent->seq = seq;
    ent->ended_at = ended_at;
}

static void update_job_info(struct job_info *job, const char *line)
{
    int64_t v;
    double d;
    char *s;

    if(!job->have_uid && json_get_i64(line, "uid", &v))
    {
        job->uid = v;
        job->have_uid = 1;
    }
    if(!job->have_jobid && json_get_i64(line, "job_id", &v))
    {
        job->jobid = v;
        job->have_jobid = 1;
    }
    if(json_get_epoch(line, "started_at", &d))
    {
        if(job->start_time == 0.0 || d < job->start_time) job->start_time = d;
    }
    if(json_get_epoch(line, "ended_at", &d))
    {
        if(d > job->end_time) job->end_time = d;
    }
    if(job->start_time == 0.0 && json_get_double(line, "t0_epoch", &d))
        job->start_time = d;

    s = json_get_string(line, "hostname");
    if(s)
    {
        if(job->hostname[0] == '\0')
        {
            snprintf(job->hostname, sizeof(job->hostname), "%s", s);
        }
        free(s);
    }

    /* exe + mounts arrive once, on the connector's metadata event (core format) */
    s = json_get_string(line, "exemnt");
    if(s)
    {
        if(job->exemnt[0] == '\0')
            snprintf(job->exemnt, sizeof(job->exemnt), "%s", s);
        free(s);
    }
}

static void free_namehash(struct darshan_name_record_ref *hash)
{
    struct darshan_name_record_ref *ref, *tmp;
    HASH_ITER(hlink, hash, ref, tmp)
    {
        HASH_DELETE(hlink, hash, ref);
        free(ref->name_record);
        free(ref);
    }
}

static void free_records(struct stream_record *records)
{
    struct stream_record *ent, *tmp;
    HASH_ITER(hlink, records, ent, tmp)
    {
        HASH_DELETE(hlink, records, ent);
        free(ent->buf);
        free(ent);
    }
}

/* Build a fresh name_hash containing only the names referenced by target_pid's
 * records, resolved against the global name hash. Native writes one namehash per
 * per-process log holding just that process's files; this reproduces that so each
 * reconstructed log is self-contained. Caller frees with free_namehash(). */
static struct darshan_name_record_ref *build_pid_namehash(
    struct stream_record *records, int64_t target_pid,
    struct darshan_name_record_ref *global_names)
{
    struct darshan_name_record_ref *sub = NULL;
    struct stream_record *rec, *tmp;

    HASH_ITER(hlink, records, rec, tmp)
    {
        const char *name;
        if(rec->key.pid != target_pid) continue;
        name = lookup_record_name(global_names, rec->key.record_id);
        if(name) add_name_record(&sub, rec->key.record_id, name);
    }
    return sub;
}

/* ---- HEATMAP reconstruction ---------------------------------------------
 * Darshan's runtime builds a per-rank HEATMAP record for each active module by
 * time-binning every read/write op (darshan-runtime/lib/darshan-heatmap.c).
 * The stream carries one event per op (op/len/started_at/ended_at), so we can
 * rebuild the same records: capture ops during read_events, then bin them with
 * the same algorithm (0.1s bins, doubling until <=200 bins fit, byte volume
 * apportioned across bins weighted by time overlap). The bin constants live in
 * the runtime .c (off the util include path), so mirror them here. */
#define HM_MAX_BINS          200
#define HM_INITIAL_BIN_WIDTH 0.1

struct hm_op {
    int mod_id;
    int64_t rank;
    int64_t pid;      /* producing process: heatmaps are rebuilt per pid, one per log */
    int is_write;
    int64_t bytes;
    double start_abs;
    double end_abs;
};
static struct hm_op *g_hm_ops = NULL;
static size_t g_hm_n = 0, g_hm_cap = 0;

static void hm_capture(int mod_id, int64_t rank, int64_t pid, int is_write,
    int64_t bytes, double start_abs, double end_abs)
{
    struct hm_op *o;
    if(bytes <= 0 || start_abs <= 0.0) return;
    if(end_abs < start_abs) end_abs = start_abs;
    if(g_hm_n == g_hm_cap)
    {
        size_t ncap = g_hm_cap ? g_hm_cap * 2 : 256;
        struct hm_op *n = realloc(g_hm_ops, ncap * sizeof(*n));
        if(!n) return;
        g_hm_ops = n;
        g_hm_cap = ncap;
    }
    o = &g_hm_ops[g_hm_n++];
    o->mod_id = mod_id;
    o->rank = rank;
    o->pid = pid;
    o->is_write = is_write;
    o->bytes = bytes;
    o->start_abs = start_abs;
    o->end_abs = end_abs;
}

static int read_events(const char *path, struct stream_record **records,
    struct darshan_name_record_ref **name_hash, int64_t *max_rank,
    struct job_info **jobs, unsigned long long *event_count,
    struct acc_record **accs)
{
    FILE *fp;
    char *line = NULL;
    size_t cap = 0;
    ssize_t nread;

    fp = fopen(path, "r");
    if(!fp)
    {
        fprintf(stderr, "Error: cannot open %s: %s\n", path, strerror(errno));
        return -1;
    }

    while((nread = getline(&line, &cap, fp)) != -1)
    {
        char *module = NULL, *file = NULL, *op = NULL;
        uint64_t record_id = 0;
        int64_t rank = -1, pid = -1;
        int64_t offset = -1, len_bytes = -1;
        double started_at = 0.0, ended_at = 0.0;
        int mod_id, exp_size;
        struct job_info *job;

        (void)nread;
        /* Route job metadata to this line's pid so every process gets its own
         * uid/jobid/start_time/hostname/exe+mounts (metadata events and module
         * events both carry pid). This is what lets us write one native-style
         * per-process log per pid, for any workload. */
        { int64_t jp = -1; json_get_i64(line, "pid", &jp);
          job = job_for_pid(jobs, jp);
          if(job) update_job_info(job, line); }

        module = json_get_string(line, "module");
        mod_id = module_name_to_id(module);
        if(mod_id < 0) goto next;

        if(!json_get_u64_hex_or_dec(line, "record_id", &record_id)) goto next;
        json_get_i64(line, "rank", &rank);
        json_get_i64(line, "pid", &pid);
        json_get_i64(line, "offset", &offset);
        json_get_i64(line, "len", &len_bytes);
        json_get_epoch(line, "started_at", &started_at);
        json_get_epoch(line, "ended_at", &ended_at);

        op = json_get_string(line, "op");

        /* HEATMAP: capture every read/write op for later time-binning. Only the
         * modules Darshan populates a heatmap for (POSIX/MPI-IO/STDIO). */
        if(op && (mod_id == DARSHAN_POSIX_MOD || mod_id == DARSHAN_STDIO_MOD ||
                  mod_id == DARSHAN_MPIIO_MOD))
        {
            int is_w = strstr(op, "write") != NULL;
            int is_r = strstr(op, "read") != NULL;
            if(is_w || is_r)
                hm_capture(mod_id, rank, pid, is_w, len_bytes, started_at, ended_at);
        }

        exp_size = expected_record_size(mod_id);
        if(exp_size <= 0) goto next;

        /* Register the file name for every event so the record's name is known
         * even if the file never closes (open-at-exit gap). */
        file = json_get_string(line, "file");
        if(file) add_name_record(name_hash, record_id, file);
        if(rank >= 0 && rank > *max_rank) *max_rank = rank;

        /* DERIVE counters: fold this per-op event into its record's accumulator.
         * Only POSIX/STDIO carry a derivable counter set today (matches the modules
         * whose wrappers stream per-op events with offset/len/timestamps). Every op
         * of the record's lifetime contributes -- no close snapshot needed, so files
         * still open at exit get a full record too. */
        if(mod_id == DARSHAN_POSIX_MOD || mod_id == DARSHAN_STDIO_MOD)
        {
            struct acc_record *a = acc_lookup(accs, mod_id, record_id, rank, pid);
            if(a) acc_update(a, op, offset, len_bytes, started_at, ended_at);
        }
        (*event_count)++;

next:
        free(module);
        free(file);
        free(op);
    }

    free(line);
    fclose(fp);
    return 0;
}

/* After all events are read, materialize each accumulated record into a native
 * module struct and hand it to add_record() (records hash). Uses a fixed seq so
 * the existing dedup treats it as authoritative (there is exactly one per key). */
static void materialize_acc_records(struct stream_record **records,
    struct acc_record *accs)
{
    struct acc_record *a, *tmp;
    HASH_ITER(hlink, accs, a, tmp)
    {
        int exp_size = expected_record_size(a->key.mod_id);
        size_t len = 0;
        void *buf;
        if(exp_size <= 0) continue;
        buf = build_record_from_acc(a, (size_t)exp_size, &len);
        if(!buf) continue;
        add_record(records, a->key.mod_id, a->key.record_id, a->key.rank,
                   a->key.pid, buf, len, /*seq*/ ~0ULL, /*ended_at*/ 0.0);
    }
}

static void fill_job(struct darshan_job *out, const struct job_info *in,
    int64_t nprocs)
{
    double start = in->start_time;
    double end = in->end_time;

    memset(out, 0, sizeof(*out));
    out->uid = in->have_uid ? in->uid : -1;
    out->jobid = in->have_jobid ? in->jobid : -1;
    out->nprocs = nprocs > 0 ? nprocs : 1;

    if(start <= 0.0) start = (double)time(NULL);
    if(end < start) end = start;

    out->start_time_sec = (int64_t)start;
    out->start_time_nsec = (int64_t)((start - floor(start)) * 1000000000.0);
    out->end_time_sec = (int64_t)end;
    out->end_time_nsec = (int64_t)((end - floor(end)) * 1000000000.0);

    snprintf(out->metadata, sizeof(out->metadata),
        "lib_ver=%s\nreconstructor_ver=%s\nreconstructed_from=mofka_jsonl\npartial=true\nhostname=%s\n",
        darshan_log_get_lib_version(),
        darshan_log_get_lib_version(),
        in->hostname[0] ? in->hostname : "unknown");
}

/* Write one .darshan log for a single process (target_pid). Only records whose
 * key.pid == target_pid (plus this pid's heatmap records, keyed with the same pid)
 * are emitted, so each output file is a faithful per-process log -- exactly the shape
 * native Darshan writes (one nprocs=1 log per process) for every workload type.
 * A per-pid name_hash (only the names this pid's records reference) is passed in. */
static int write_log(const char *outfile, struct stream_record *records,
    int64_t target_pid, struct darshan_name_record_ref *name_hash, int64_t max_rank,
    const struct job_info *job_info,
    unsigned *written_out, unsigned *pruned_out)
{
    darshan_fd out;
    struct darshan_job job;
    struct stream_record *rec, *tmp;
    struct darshan_mnt_info mnt;
    uint64_t partial = 0;
    int ret;
    /* One process per output log, matching native's per-process nprocs=1 logs. */
    int64_t nprocs = 1;

    (void)max_rank;

    /* partial stays 0: a reconstructed log contains every streamed record (not memory-limited). */
    out = darshan_log_create(outfile, DARSHAN_ZLIB_COMP, partial);
    if(!out)
    {
        fprintf(stderr, "Error: cannot create %s\n", outfile);
        return -1;
    }

    fill_job(&job, job_info, nprocs);
    ret = darshan_log_put_job(out, &job);
    if(ret < 0) goto fail;

    /* exemnt is core's job buffer: the exe command line, then one
     * "<fstype>\t<mount point>" per line (darshan_get_exe_and_mounts / add_entry). */
    {
        char tmp[sizeof(job_info->exemnt)];
        struct darshan_mnt_info mnts[64];
        int nmnt = 0;
        char *mstr = NULL, *nl;

        snprintf(tmp, sizeof(tmp), "%s", job_info->exemnt);
        nl = strchr(tmp, '\n');
        if(nl) { *nl = '\0'; mstr = nl + 1; }

        ret = darshan_log_put_exe(out,
            tmp[0] ? tmp : "reconstructed-from-mofka-stream");
        if(ret < 0) goto fail;

        if(mstr)
        {
            char *save = NULL, *entry;
            for(entry = strtok_r(mstr, "\n", &save);
                entry && nmnt < (int)(sizeof(mnts)/sizeof(mnts[0]));
                entry = strtok_r(NULL, "\n", &save))
            {
                char *tab = strchr(entry, '\t');
                if(!tab) continue;
                *tab = '\0';
                memset(&mnts[nmnt], 0, sizeof(mnts[nmnt]));
                snprintf(mnts[nmnt].mnt_type, sizeof(mnts[nmnt].mnt_type),
                    "%s", entry);
                snprintf(mnts[nmnt].mnt_path, sizeof(mnts[nmnt].mnt_path),
                    "%s", tab + 1);
                nmnt++;
            }
        }
        if(nmnt == 0)
        {
            memset(&mnt, 0, sizeof(mnt));
            snprintf(mnt.mnt_type, sizeof(mnt.mnt_type), "unknown");
            snprintf(mnt.mnt_path, sizeof(mnt.mnt_path), "/");
            ret = darshan_log_put_mounts(out, &mnt, 1);
        }
        else
        {
            ret = darshan_log_put_mounts(out, mnts, nmnt);
        }
        if(ret < 0) goto fail;
    }

    ret = darshan_log_put_namehash(out, name_hash);
    if(ret < 0) goto fail;

    /* Write module records in ascending module-id order. darshan_log_dzwrite
     * hard-fails if a record is written to a module region id lower than the
     * previous one, and modules interleave in the stream (POSIX=1, MPIIO=2,
     * STDIO=9), so writing in hash/first-seen order rejects any normal
     * multi-module capture. The outer loop over ascending module ids matches
     * darshan's own writers (darshan-convert.c / darshan-merge.c) and also
     * bounds the mod_logutils[] index correctly (it has DARSHAN_KNOWN_MODULE_COUNT
     * entries, not DARSHAN_MAX_MODS). */
    {
        int m;
        for(m = 0; m < DARSHAN_KNOWN_MODULE_COUNT; m++)
        {
            if(!mod_logutils[m]) continue;
            HASH_ITER(hlink, records, rec, tmp)
            {
                if(rec->key.mod_id != m) continue;
                if(rec->key.pid != target_pid) continue;
                /* prune unused std streams, matching native Darshan */
                if(record_is_empty(rec->key.mod_id, rec->buf,
                    lookup_record_name(name_hash, rec->key.record_id)))
                {
                    if(pruned_out) (*pruned_out)++;
                    continue;
                }
                /* Each output log is a single-process log (nprocs=1), but the
                 * streamed record buffer still carries its ORIGINAL global rank
                 * (0..N-1). Darshan's accumulator asserts rank < job_nprocs
                 * (darshan-logutils-accumulator.c:145), so a record with rank=31
                 * in an nprocs=1 log aborts darshan-parser and pydarshan's summary
                 * (Assertion `rank < acc->job_nprocs' / SIGABRT). Normalize the
                 * base-record rank to 0 -- the sole process of this per-pid log --
                 * exactly as native nprocs=1 logs carry rank 0. strict_compare
                 * folds by pid and never reads record rank, so this is compare-safe. */
                ((struct darshan_base_record *)rec->buf)->rank = 0;
                ret = mod_logutils[m]->log_put_record(out, rec->buf);
                if(ret < 0)
                {
                    fprintf(stderr, "Error: failed writing module record mod_id=%d record_id=%" PRIu64 "\n",
                        rec->key.mod_id, rec->key.record_id);
                    goto fail;
                }
                if(written_out) (*written_out)++;
            }
        }
    }

    darshan_log_close(out);
    return 0;

fail:
    fprintf(stderr, "Error: failed writing %s\n", outfile);
    darshan_log_close(out);
    return -1;
}

/* Canonical Darshan record ids for the per-module heatmap name records. These
 * are the fixed darshan_core_gen_record_id("heatmap:<MOD>") values, and pydarshan
 * hard-codes the same map (report.read_all_heatmap_records) to resolve a heatmap
 * record back to its submodule; using anything else makes it fall back to the
 * raw integer id and the HTML summary crashes. Keep in sync with that map. */
static uint64_t heatmap_ident(int mod_id, const char **name_out)
{
    switch(mod_id)
    {
        case DARSHAN_POSIX_MOD: *name_out = "heatmap:POSIX"; return 16592106915301738621ULL;
        case DARSHAN_STDIO_MOD: *name_out = "heatmap:STDIO"; return 3989511027826779520ULL;
        case DARSHAN_MPIIO_MOD: *name_out = "heatmap:MPIIO"; return 3668870418325792824ULL;
        default:                *name_out = NULL;            return 0;
    }
}

/* Build HEATMAP records for a SINGLE process (target_pid) from its captured ops,
 * one record per (module, rank) that pid touched, keyed with target_pid so write_log
 * emits them into that pid's log. Each per-process log thus gets its own heatmap with
 * a single consistent nbins -- which is what pydarshan requires to render (a merged
 * multi-process log has mixed nbins and pydarshan rejects it; native has the same
 * limitation on merged logs, so we mirror native by staying per-process).
 * Does NOT free g_hm_ops (reused across pids); caller frees once at the end. */
static void build_heatmap_records_for_pid(struct stream_record **records,
    struct darshan_name_record_ref **name_hash, const struct job_info *job,
    int64_t target_pid)
{
    double t0 = job->start_time;
    size_t i, j;
    char *done;

    if(g_hm_n == 0 || t0 <= 0.0)
        return;
    done = calloc(g_hm_n, 1);
    if(!done) return;

    for(i = 0; i < g_hm_n; i++)
    {
        int mod_id = g_hm_ops[i].mod_id;
        int64_t rank = g_hm_ops[i].rank;
        double max_end = 0.0, bin_width = HM_INITIAL_BIN_WIDTH;
        int nbins, b;
        size_t bufsz;
        char *hbuf;
        struct darshan_heatmap_record *hr;
        int64_t *wb, *rb;
        const char *hmname = NULL;
        uint64_t id;

        if(done[i]) continue;
        if(g_hm_ops[i].pid != target_pid) { continue; }  /* leave other pids' ops for their pass */
        id = heatmap_ident(mod_id, &hmname);
        if(!hmname) { done[i] = 1; continue; }  /* module has no heatmap */

        /* pass 1: latest op end time (relative to job start) for this group */
        for(j = i; j < g_hm_n; j++)
        {
            double e;
            if(g_hm_ops[j].pid != target_pid) continue;
            if(g_hm_ops[j].mod_id != mod_id || g_hm_ops[j].rank != rank) continue;
            e = g_hm_ops[j].end_abs - t0;
            if(e > max_end) max_end = e;
        }
        if(max_end < 0.0) max_end = 0.0;

        /* choose bin width like the runtime: double until <= MAX bins fit */
        while(max_end > bin_width * HM_MAX_BINS)
            bin_width *= 2.0;
        nbins = (int)ceil(max_end / bin_width);
        if(nbins < 1) nbins = 1;
        if(nbins > HM_MAX_BINS) nbins = HM_MAX_BINS;

        bufsz = sizeof(struct darshan_heatmap_record)
              + (size_t)nbins * 2 * sizeof(int64_t);
        hbuf = calloc(1, bufsz);
        if(!hbuf) break;
        hr = (struct darshan_heatmap_record *)hbuf;
        wb = (int64_t *)(hbuf + sizeof(struct darshan_heatmap_record));
        rb = wb + nbins;

        /* pass 2: apportion each op's bytes across the bins it spans, weighted
         * by time overlap (mirrors darshan_heatmap_update) */
        for(j = i; j < g_hm_n; j++)
        {
            double s, e, dur;
            int64_t *bins;
            int b0, b1;
            if(g_hm_ops[j].pid != target_pid) continue;
            if(g_hm_ops[j].mod_id != mod_id || g_hm_ops[j].rank != rank) continue;
            done[j] = 1;
            s = g_hm_ops[j].start_abs - t0; if(s < 0.0) s = 0.0;
            e = g_hm_ops[j].end_abs - t0;   if(e < s) e = s;
            dur = e - s;
            bins = g_hm_ops[j].is_write ? wb : rb;
            b0 = (int)(s / bin_width);
            b1 = (int)(e / bin_width);
            if(b0 < 0) b0 = 0;
            if(b1 >= nbins) b1 = nbins - 1;
            if(b0 > b1) b0 = b1;
            if(dur <= 0.0)
            {
                bins[b0] += g_hm_ops[j].bytes;
            }
            else
            {
                for(b = b0; b <= b1; b++)
                {
                    double bs = b * bin_width, be = (b + 1) * bin_width;
                    double lo = s > bs ? s : bs;
                    double hi = e < be ? e : be;
                    double sec = hi - lo;
                    if(sec <= 0.0) continue;
                    bins[b] += (int64_t)(
                        (double)g_hm_ops[j].bytes * sec / dur + 0.5);
                }
            }
        }

        hr->bin_width_seconds = bin_width;
        hr->nbins = nbins;
        hr->write_bins = wb;   /* fixed up on read; set for local consistency */
        hr->read_bins = rb;
        hr->base_rec.id = id;
        hr->base_rec.rank = rank;

        add_name_record(name_hash, id, hmname);
        /* add_record takes ownership of hbuf; keyed with target_pid so it lands in
         * this process's log alongside its module records. */
        add_record(records, DARSHAN_HEATMAP_MOD, id, rank, target_pid, hbuf, bufsz, 1, 0.0);
    }

    free(done);
}

/* Ensure the output directory exists (like `mkdir -p` for a single level; the
 * demo always passes an existing parent). Returns 0 on success. */
static int ensure_dir(const char *path)
{
    struct stat st;
    if(stat(path, &st) == 0)
        return S_ISDIR(st.st_mode) ? 0 : -1;
    if(mkdir(path, 0755) == 0)
        return 0;
    return -1;
}

int main(int argc, char **argv)
{
    struct stream_record *records = NULL;
    struct darshan_name_record_ref *name_hash = NULL;   /* global id->name */
    struct job_info *jobs = NULL;                        /* per-pid metadata hash */
    struct acc_record *accs = NULL;                      /* per-record op accumulator */
    struct job_info *job, *jtmp;
    int64_t max_rank = -1;
    unsigned long long event_count = 0;
    const char *outdir;
    unsigned files_ok = 0, files_fail = 0, npids = 0;
    int ret;

    if(argc != 3)
    {
        usage(argv[0]);
        return 1;
    }
    outdir = argv[2];

    ret = read_events(argv[1], &records, &name_hash, &max_rank, &jobs,
        &event_count, &accs);
    if(ret < 0)
        return 1;

    /* DERIVE per-file records: fold the accumulated per-op counters (built in
     * read_events via acc_update) into native POSIX/STDIO structs and add them to
     * `records`. This is what replaces the old close-time counters[] snapshot -- and
     * because every op contributes, a file still open at exit (no close event) also
     * gets a full record. The per-pid heatmap records (from op/len/started_at/
     * ended_at) are built LATER at build_heatmap_records_for_pid(). Bail only if there
     * is nothing at all to reconstruct: no module records AND no heatmap ops. */
    materialize_acc_records(&records, accs);

    if(HASH_CNT(hlink, records) == 0 && g_hm_n == 0)
    {
        fprintf(stderr, "Error: no reconstructable records or heatmap ops found in %s\n", argv[1]);
        free_records(records);
        free_namehash(name_hash);
        free_jobs(jobs);
        free_accs(accs);
        return 1;
    }

    if(ensure_dir(outdir) != 0)
    {
        fprintf(stderr, "Error: cannot create/use output directory %s: %s\n",
            outdir, strerror(errno));
        free_records(records);
        free_namehash(name_hash);
        free_jobs(jobs);
        free_accs(accs);
        return 1;
    }

    /* One native-style .darshan log per producing process, mirroring native's
     * per-process output. Skip the synthetic pid=-1 bucket (metadata-only lines
     * with no pid); real producers all carry a pid. */
    HASH_ITER(hlink, jobs, job, jtmp)
    {
        char logname[512];
        char outpath[4096];
        struct darshan_name_record_ref *pid_names;
        unsigned written = 0, pruned = 0;
        int64_t pid = job->pid;

        if(pid < 0) continue;   /* no-pid metadata bucket */
        npids++;

        /* build this pid's heatmap records (keyed with pid) then its name subhash
         * (must run after heatmaps so heatmap name records are included) */
        build_heatmap_records_for_pid(&records, &name_hash, job, pid);
        pid_names = build_pid_namehash(records, pid, name_hash);

        build_native_logname(logname, sizeof(logname), job);
        snprintf(outpath, sizeof(outpath), "%s/%s", outdir, logname);

        ret = write_log(outpath, records, pid, pid_names, max_rank, job,
            &written, &pruned);
        if(ret == 0) files_ok++;
        else         files_fail++;

        free_namehash(pid_names);
    }

    fprintf(stderr,
        "reconstructed %u per-process .darshan logs (%u pids, %u failed) "
        "from %llu streamed events into %s/\n",
        files_ok, npids, files_fail, event_count, outdir);

    free_records(records);
    free_namehash(name_hash);
    free_jobs(jobs);
    free_accs(accs);
    free(g_hm_ops);
    g_hm_ops = NULL; g_hm_n = g_hm_cap = 0;
    return files_fail == 0 ? 0 : 1;
}
