/*
 * Copyright (C) 2026 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 * darshan-mofka-reconstruct.c -- best-effort partial .darshan reconstruction
 * from Darshan->Mofka JSONL captures.
 *
 * Input is the JSONL produced by server/capture.py in the parent demo repo.
 * The tool keeps the latest rec_hex snapshot for each (module, record_id, rank)
 * and writes those module records into a partial Darshan log.
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "uthash-1.9.2/src/uthash.h"
#include "darshan-logutils.h"

struct rec_key
{
    int mod_id;
    uint64_t record_id;
    int64_t rank;
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

struct job_info
{
    int have_uid;
    int have_jobid;
    int64_t uid;
    int64_t jobid;
    double start_time;
    double end_time;
    char hostname[256];
};

static int hex_value(int c);   /* fwd decl: used by json_get_string's \u case */

static void usage(const char *prog)
{
    fprintf(stderr, "Usage: %s <events.jsonl> <job_partial.darshan>\n", prog);
}

static const char *json_find_key(const char *line, const char *key)
{
    char needle[128];
    snprintf(needle, sizeof(needle), "\"%s\":", key);
    return strstr(line, needle);
}

static char *json_get_string(const char *line, const char *key)
{
    const char *p = json_find_key(line, key);
    const char *s;
    char *out;
    size_t cap, n;

    if(!p) return NULL;
    p = strchr(p, ':');
    if(!p) return NULL;
    p++;
    while(*p && isspace((unsigned char)*p)) p++;
    if(*p != '"') return NULL;
    p++;
    s = p;

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

    (void)s;
    out[n] = '\0';
    return out;
}

static int json_get_i64(const char *line, const char *key, int64_t *out)
{
    const char *p = json_find_key(line, key);
    char *end = NULL;
    long long v;

    if(!p) return 0;
    p = strchr(p, ':');
    if(!p) return 0;
    p++;
    while(*p && isspace((unsigned char)*p)) p++;
    v = strtoll(p, &end, 10);
    if(end == p) return 0;
    *out = (int64_t)v;
    return 1;
}

static int json_get_u64(const char *line, const char *key, uint64_t *out)
{
    const char *p = json_find_key(line, key);
    char *end = NULL;
    unsigned long long v;

    if(!p) return 0;
    p = strchr(p, ':');
    if(!p) return 0;
    p++;
    while(*p && isspace((unsigned char)*p)) p++;
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
    const char *p = json_find_key(line, key);
    char *end = NULL;
    double v;

    if(!p) return 0;
    p = strchr(p, ':');
    if(!p) return 0;
    p++;
    while(*p && isspace((unsigned char)*p)) p++;
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

static void *decode_hex(const char *hex, size_t expected, size_t *out_len)
{
    size_t hex_len, n, i;
    unsigned char *buf;

    if(!hex) return NULL;
    hex_len = strlen(hex);
    if(hex_len % 2 != 0) return NULL;
    n = hex_len / 2;
    /* For a fixed-layout module struct, only an exact size match is safe.
     * Trimming an oversized payload would silently pass a structurally-wrong
     * record (masking runtime/logutils version skew), so reject it instead. */
    if(expected > 0 && n != expected) return NULL;

    buf = malloc(n ? n : 1);
    if(!buf) return NULL;
    for(i = 0; i < n; i++)
    {
        int hi = hex_value((unsigned char)hex[2*i]);
        int lo = hex_value((unsigned char)hex[2*i + 1]);
        if(hi < 0 || lo < 0)
        {
            free(buf);
            return NULL;
        }
        buf[i] = (unsigned char)((hi << 4) | lo);
    }
    *out_len = n;
    return buf;
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
    int64_t rank, void *buf, size_t len, unsigned long long seq, double ended_at)
{
    struct rec_key key;
    struct stream_record *ent;

    memset(&key, 0, sizeof(key));
    key.mod_id = mod_id;
    key.record_id = record_id;
    key.rank = rank;

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
    int is_write;
    int64_t bytes;
    double start_abs;
    double end_abs;
};
static struct hm_op *g_hm_ops = NULL;
static size_t g_hm_n = 0, g_hm_cap = 0;

static void hm_capture(int mod_id, int64_t rank, int is_write, int64_t bytes,
    double start_abs, double end_abs)
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
    o->is_write = is_write;
    o->bytes = bytes;
    o->start_abs = start_abs;
    o->end_abs = end_abs;
}

static int read_events(const char *path, struct stream_record **records,
    struct darshan_name_record_ref **name_hash, int64_t *max_rank,
    struct job_info *job, unsigned long long *event_count)
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
        char *module = NULL, *file = NULL, *hex = NULL;
        uint64_t record_id = 0;
        int64_t rank = -1, rec_size_i = 0;
        unsigned long long seq = 0;
        double ended_at = 0.0;
        int mod_id, exp_size;
        void *buf;
        size_t len;

        (void)nread;
        update_job_info(job, line);

        module = json_get_string(line, "module");
        mod_id = module_name_to_id(module);
        if(mod_id < 0) goto next;

        if(!json_get_u64_hex_or_dec(line, "record_id", &record_id)) goto next;
        json_get_i64(line, "rank", &rank);
        json_get_i64(line, "rec_size", &rec_size_i);
        { uint64_t seq_u = 0; if(json_get_u64(line, "seq", &seq_u)) seq = (unsigned long long)seq_u; }
        json_get_double(line, "ended_at", &ended_at);

        /* HEATMAP: capture every read/write op for later time-binning, even if
         * the module record below is filtered out by rec_size. Only the modules
         * that Darshan populates a heatmap for (POSIX/MPI-IO/STDIO). */
        if(mod_id == DARSHAN_POSIX_MOD || mod_id == DARSHAN_STDIO_MOD ||
           mod_id == DARSHAN_MPIIO_MOD)
        {
            char *op = json_get_string(line, "op");
            if(op)
            {
                int is_w = strstr(op, "write") != NULL;
                int is_r = strstr(op, "read") != NULL;
                if(is_w || is_r)
                {
                    int64_t nbytes = 0;
                    double s = 0.0, e = 0.0;
                    json_get_i64(line, "len", &nbytes);
                    json_get_epoch(line, "started_at", &s);
                    json_get_epoch(line, "ended_at", &e);
                    hm_capture(mod_id, rank, is_w, nbytes, s, e);
                }
                free(op);
            }
        }

        exp_size = expected_record_size(mod_id);
        if(exp_size <= 0) goto next;
        if(rec_size_i > 0 && rec_size_i != exp_size)
        {
            /* For v1, only reconstruct fixed-size records that match this build. */
            goto next;
        }

        hex = json_get_string(line, "rec_hex");
        buf = decode_hex(hex, (size_t)exp_size, &len);
        if(!buf || len != (size_t)exp_size)
        {
            free(buf);
            goto next;
        }

        file = json_get_string(line, "file");
        if(file) add_name_record(name_hash, record_id, file);
        if(rank >= 0 && rank > *max_rank) *max_rank = rank;
        add_record(records, mod_id, record_id, rank, buf, len, seq, ended_at);
        (*event_count)++;

next:
        free(module);
        free(file);
        free(hex);
    }

    free(line);
    fclose(fp);
    return 0;
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
        "lib_ver=unknown\nreconstructor_ver=%s\nreconstructed_from=mofka_jsonl\npartial=true\nhostname=%s\n",
        darshan_log_get_lib_version(),
        in->hostname[0] ? in->hostname : "unknown");
}

static int write_log(const char *outfile, struct stream_record *records,
    struct darshan_name_record_ref *name_hash, int64_t max_rank,
    const struct job_info *job_info,
    unsigned *written_out, unsigned *pruned_out)
{
    darshan_fd out;
    struct darshan_job job;
    struct stream_record *rec, *tmp;
    struct darshan_mnt_info mnt;
    uint64_t partial = 0;
    int ret;
    int64_t nprocs = max_rank + 1;

    HASH_ITER(hlink, records, rec, tmp)
    {
        if(record_is_empty(rec->key.mod_id, rec->buf,
            lookup_record_name(name_hash, rec->key.record_id))) continue;
        DARSHAN_MOD_FLAG_SET(partial, rec->key.mod_id);
    }

    out = darshan_log_create(outfile, DARSHAN_ZLIB_COMP, partial);
    if(!out)
    {
        fprintf(stderr, "Error: cannot create %s\n", outfile);
        return -1;
    }

    fill_job(&job, job_info, nprocs);
    ret = darshan_log_put_job(out, &job);
    if(ret < 0) goto fail;

    ret = darshan_log_put_exe(out, "reconstructed-from-mofka-stream");
    if(ret < 0) goto fail;

    memset(&mnt, 0, sizeof(mnt));
    snprintf(mnt.mnt_type, sizeof(mnt.mnt_type), "unknown");
    snprintf(mnt.mnt_path, sizeof(mnt.mnt_path), "/");
    ret = darshan_log_put_mounts(out, &mnt, 1);
    if(ret < 0) goto fail;

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
                /* prune unused std streams, matching native Darshan */
                if(record_is_empty(rec->key.mod_id, rec->buf,
                    lookup_record_name(name_hash, rec->key.record_id)))
                {
                    if(pruned_out) (*pruned_out)++;
                    continue;
                }
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

/* Build one HEATMAP record per (module, rank) seen in the op stream and add it
 * to the record + name hashes so write_log emits it like any other record. */
static void build_heatmap_records(struct stream_record **records,
    struct darshan_name_record_ref **name_hash, const struct job_info *job)
{
    double t0 = job->start_time;
    size_t i, j;
    char *done;

    if(g_hm_n == 0 || t0 <= 0.0)
    {
        free(g_hm_ops);
        g_hm_ops = NULL; g_hm_n = g_hm_cap = 0;
        return;
    }
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
        id = heatmap_ident(mod_id, &hmname);
        if(!hmname) { done[i] = 1; continue; }  /* module has no heatmap */

        /* pass 1: latest op end time (relative to job start) for this group */
        for(j = i; j < g_hm_n; j++)
        {
            double e;
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
        /* add_record takes ownership of hbuf */
        add_record(records, DARSHAN_HEATMAP_MOD, id, rank, hbuf, bufsz, 1, 0.0);
    }

    free(done);
    free(g_hm_ops);
    g_hm_ops = NULL; g_hm_n = g_hm_cap = 0;
}

int main(int argc, char **argv)
{
    struct stream_record *records = NULL;
    struct darshan_name_record_ref *name_hash = NULL;
    int64_t max_rank = -1;
    struct job_info job;
    unsigned long long event_count = 0;
    int ret;

    if(argc != 3)
    {
        usage(argv[0]);
        return 1;
    }

    memset(&job, 0, sizeof(job));

    ret = read_events(argv[1], &records, &name_hash, &max_rank, &job, &event_count);
    if(ret < 0)
        return 1;

    /* rebuild per-module HEATMAP records from the captured op stream */
    build_heatmap_records(&records, &name_hash, &job);

    if(HASH_CNT(hlink, records) == 0)
    {
        fprintf(stderr, "Error: no reconstructable module records found in %s\n", argv[1]);
        free_records(records);
        free_namehash(name_hash);
        return 1;
    }

    {
        unsigned written = 0, pruned = 0;
        ret = write_log(argv[2], records, name_hash, max_rank, &job,
            &written, &pruned);
        if(ret == 0)
        {
            fprintf(stderr,
                "reconstructed %u module records (%u pruned as empty) from %llu streamed events into %s\n",
                written, pruned, event_count, argv[2]);
        }
    }

    free_records(records);
    free_namehash(name_hash);
    return ret == 0 ? 0 : 1;
}
