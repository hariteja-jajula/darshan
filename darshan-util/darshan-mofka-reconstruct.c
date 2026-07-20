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
#include <sys/stat.h>
#include <sys/types.h>

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
    fprintf(stderr,
        "Usage: %s [--per-rank] <events.jsonl|-> <output>\n"
        "  default:     <output> is a single partial .darshan log (all ranks)\n"
        "  --per-rank:  <output> is a directory; writes one rank<N>.darshan per\n"
        "               rank, suitable for `darshan-merge --shared-redux`.\n",
        prog);
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
    if(json_get_double(line, "started_at", &d))
    {
        if(job->start_time == 0.0 || d < job->start_time) job->start_time = d;
    }
    if(json_get_double(line, "ended_at", &d))
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

static int read_events(const char *path, struct stream_record **records,
    struct darshan_name_record_ref **name_hash, int64_t *max_rank,
    struct job_info *job, unsigned long long *event_count)
{
    FILE *fp;
    char *line = NULL;
    size_t cap = 0;
    ssize_t nread;

    if(strcmp(path, "-") == 0)
    {
        fp = stdin;
    }
    else
    {
        fp = fopen(path, "r");
        if(!fp)
        {
            fprintf(stderr, "Error: cannot open %s: %s\n", path, strerror(errno));
            return -1;
        }
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
    if(fp != stdin)
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

/* Write module records to a single .darshan log.
 *
 * rank_filter < 0  : write every rank's records into one log (default mode).
 * rank_filter >= 0 : write only records for that rank (per-rank mode); the
 *                    output log's nprocs still reflects the full job so that
 *                    darshan-merge --shared-redux reduces correctly.
 */
static int write_log(const char *outfile, struct stream_record *records,
    struct darshan_name_record_ref *name_hash, int64_t max_rank,
    const struct job_info *job_info, int64_t rank_filter)
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
        if(rank_filter >= 0 && rec->key.rank != rank_filter)
            continue;
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
                if(rank_filter >= 0 && rec->key.rank != rank_filter) continue;
                ret = mod_logutils[m]->log_put_record(out, rec->buf);
                if(ret < 0)
                {
                    fprintf(stderr, "Error: failed writing module record mod_id=%d record_id=%" PRIu64 "\n",
                        rec->key.mod_id, rec->key.record_id);
                    goto fail;
                }
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

/* Per-rank mode: write outdir/rank<N>.darshan for every rank that has at least
 * one record. Each file carries only that rank's records but reports the full
 * job's nprocs, so the resulting set is a drop-in for:
 *     darshan-merge --shared-redux --output job.darshan outdir/*.darshan
 */
static int write_per_rank(const char *outdir, struct stream_record *records,
    struct darshan_name_record_ref *name_hash, int64_t max_rank,
    const struct job_info *job_info)
{
    int64_t r;
    int files = 0;

    if(mkdir(outdir, 0755) != 0 && errno != EEXIST)
    {
        fprintf(stderr, "Error: cannot create output directory %s: %s\n",
            outdir, strerror(errno));
        return -1;
    }

    for(r = 0; r <= max_rank; r++)
    {
        struct stream_record *rec, *tmp;
        char path[PATH_MAX];
        int has = 0;

        /* skip ranks that produced no records */
        HASH_ITER(hlink, records, rec, tmp)
        {
            if(rec->key.rank == r) { has = 1; break; }
        }
        if(!has)
            continue;

        snprintf(path, sizeof(path), "%s/rank%" PRId64 ".darshan", outdir, r);
        if(write_log(path, records, name_hash, max_rank, job_info, r) != 0)
            return -1;
        files++;
    }

    if(files == 0)
    {
        fprintf(stderr, "Error: no rank-tagged records found; nothing written to %s\n", outdir);
        return -1;
    }

    fprintf(stderr, "wrote %d per-rank log(s) under %s (nprocs=%" PRId64 ")\n",
        files, outdir, max_rank + 1);
    return 0;
}

int main(int argc, char **argv)
{
    struct stream_record *records = NULL;
    struct darshan_name_record_ref *name_hash = NULL;
    int64_t max_rank = -1;
    struct job_info job;
    unsigned long long event_count = 0;
    int per_rank = 0;
    int argi = 1;
    const char *infile, *output;
    int ret;

    /* optional --per-rank flag before the two positional args */
    if(argc >= 2 && strcmp(argv[argi], "--per-rank") == 0)
    {
        per_rank = 1;
        argi++;
    }

    if(argc - argi != 2)
    {
        usage(argv[0]);
        return 1;
    }
    infile = argv[argi];
    output = argv[argi + 1];

    memset(&job, 0, sizeof(job));

    ret = read_events(infile, &records, &name_hash, &max_rank, &job, &event_count);
    if(ret < 0)
        return 1;

    if(HASH_CNT(hlink, records) == 0)
    {
        fprintf(stderr, "Error: no reconstructable module records found in %s\n",
            strcmp(infile, "-") == 0 ? "stdin" : infile);
        free_records(records);
        free_namehash(name_hash);
        return 1;
    }

    if(per_rank)
    {
        ret = write_per_rank(output, records, name_hash, max_rank, &job);
    }
    else
    {
        ret = write_log(output, records, name_hash, max_rank, &job, -1);
        if(ret == 0)
        {
            fprintf(stderr, "reconstructed %u module records from %llu streamed events into %s\n",
                (unsigned)HASH_CNT(hlink, records), event_count, output);
        }
    }

    free_records(records);
    free_namehash(name_hash);
    return ret == 0 ? 0 : 1;
}
