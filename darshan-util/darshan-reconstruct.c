/*
 * darshan-reconstruct.c  (mofka-reconstructor branch)
 *
 * Rebuilds a .darshan log from a stream of per-record SNAPSHOTS instead of the
 * runtime's clean shutdown. Goal: recover ~98% of the log even when the app
 * crashes / MPI_Finalize never runs.
 *
 * Two subcommands (Milestone 1 -- offline, no mofka yet):
 *   dump  <in.darshan> <out.snap>   emit the log as a framed snapshot stream
 *                                   (stands in for the runtime emitter + mofka)
 *   build <in.snap>    <out.darshan> reconstruct: keep the LATEST snapshot per
 *                                   (module, record_id), then write a valid log
 *
 * The reconstructor half (build) is what will later consume from mofka: swap the
 * file reader for a topic consumer; the assemble+write logic is unchanged. Write
 * path mirrors darshan-convert.c (create -> put_job -> put_exe -> put_mounts ->
 * put_namehash -> per-module log_put_record -> close).
 *
 * Snapshot stream (native binary, framed):
 *   'F' u8 partial_flag
 *   'J' u32 len, bytes(struct darshan_job)
 *   'E' u32 len, bytes(exe string incl NUL)
 *   'M' u32 count, then count x [u32 len, bytes(struct darshan_mnt_info)]
 *   'N' u64 id, u32 len, bytes(name incl NUL)        (one per name record)
 *   'R' u8 mod_id, u32 len, bytes(record)            (repeated; dup id -> latest wins)
 *   'Z' end
 */
#ifdef HAVE_CONFIG_H
# include "darshan-util-config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "darshan-logutils.h"
#include "darshan-heatmap-log-format.h"

/* Record byte size. Some modules (e.g. HEATMAP) don't provide log_sizeof_record,
 * so fall back to the module's own on-disk size formula. Returns 0 if unknown. */
static uint32_t rec_size(int mod, void *buf)
{
    if(mod_logutils[mod]->log_sizeof_record)
        return (uint32_t)mod_logutils[mod]->log_sizeof_record(buf);
    if(mod == DARSHAN_HEATMAP_MOD) {
        struct darshan_heatmap_record *h = (struct darshan_heatmap_record *)buf;
        return (uint32_t)(sizeof(struct darshan_heatmap_record) +
                          h->nbins * 2 * (int64_t)sizeof(int64_t));
    }
    return 0;
}

/* ---- little framed-stream helpers ---------------------------------------- */
static void w_u8 (FILE *f, uint8_t  v) { fwrite(&v, 1, 1, f); }
static void w_u32(FILE *f, uint32_t v) { fwrite(&v, 4, 1, f); }
static void w_u64(FILE *f, uint64_t v) { fwrite(&v, 8, 1, f); }

static int r_u8 (FILE *f, uint8_t  *v) { return fread(v, 1, 1, f) == 1 ? 0 : -1; }
static int r_u32(FILE *f, uint32_t *v) { return fread(v, 4, 1, f) == 1 ? 0 : -1; }
static int r_u64(FILE *f, uint64_t *v) { return fread(v, 8, 1, f) == 1 ? 0 : -1; }

/* ---- dedup table: latest record per (module, record_id) ------------------ */
struct rec_ent {
    darshan_record_id id;      /* hash key */
    uint32_t          len;
    void             *buf;
    UT_hash_handle    hh;
};
static struct rec_ent *rec_head[DARSHAN_MAX_MODS];   /* one hash per module */

static void rec_upsert(int mod, darshan_record_id id, const void *buf, uint32_t len)
{
    struct rec_ent *e;
    HASH_FIND(hh, rec_head[mod], &id, sizeof(id), e);
    if(e) {                         /* newer snapshot wins */
        free(e->buf);
    } else {
        e = calloc(1, sizeof(*e));
        e->id = id;
        HASH_ADD(hh, rec_head[mod], id, sizeof(id), e);
    }
    e->len = len;
    e->buf = malloc(len);
    memcpy(e->buf, buf, len);
}

/* ---- dump: log -> snapshot stream (stand-in for emitter+mofka) ----------- */
static int do_dump(const char *in, const char *out)
{
    darshan_fd fd = darshan_log_open(in);
    if(!fd) { fprintf(stderr, "reconstruct: cannot open %s\n", in); return -1; }
    FILE *f = fopen(out, "wb");
    if(!f) { darshan_log_close(fd); return -1; }

    w_u8(f, 'F'); w_u8(f, (uint8_t)fd->partial_flag);

    struct darshan_job job;
    if(darshan_log_get_job(fd, &job) == 0) {
        w_u8(f, 'J'); w_u32(f, sizeof(job)); fwrite(&job, sizeof(job), 1, f);
    }

    char *exe = calloc(1, DEF_MOD_BUF_SIZE);
    if(darshan_log_get_exe(fd, exe) == 0) {
        uint32_t n = (uint32_t)strlen(exe) + 1;
        w_u8(f, 'E'); w_u32(f, n); fwrite(exe, n, 1, f);
    }
    free(exe);

    struct darshan_mnt_info *mnts = NULL; int nmnt = 0;
    if(darshan_log_get_mounts(fd, &mnts, &nmnt) == 0) {
        w_u8(f, 'M'); w_u32(f, (uint32_t)nmnt);
        for(int i = 0; i < nmnt; i++) {
            w_u32(f, sizeof(struct darshan_mnt_info));
            fwrite(&mnts[i], sizeof(struct darshan_mnt_info), 1, f);
        }
    }

    struct darshan_name_record_ref *nh = NULL, *ref, *tmp;
    if(darshan_log_get_namehash(fd, &nh) == 0) {
        HASH_ITER(hlink, nh, ref, tmp) {
            uint32_t n = (uint32_t)strlen(ref->name_record->name) + 1;
            w_u8(f, 'N'); w_u64(f, (uint64_t)ref->name_record->id); w_u32(f, n);
            fwrite(ref->name_record->name, n, 1, f);
        }
    }

    /* mbuf may be realloc'd (moved) by log_get_record for variable-length
     * modules like HEATMAP -- pass its address and let it follow, like
     * darshan-convert. DXT uses a NULL (malloc'd-per-record) buffer. */
    char *mbuf = malloc(DEF_MOD_BUF_SIZE);
    for(int i = 0; i < DARSHAN_MAX_MODS; i++) {
        if(fd->mod_map[i].len == 0 || i >= DARSHAN_KNOWN_MODULE_COUNT || !mod_logutils[i])
            continue;
        int dxt = (i == DXT_POSIX_MOD || i == DXT_MPIIO_MOD), ret, n = 0;
        if(dxt) {
            for(;;) {
                void *dp = NULL;
                if((ret = mod_logutils[i]->log_get_record(fd, &dp)) != 1) break;
                uint32_t sz = rec_size(i, dp);
                if(sz) { w_u8(f, 'R'); w_u8(f, (uint8_t)i); w_u32(f, sz); fwrite(dp, sz, 1, f); n++; }
                free(dp);
            }
        } else {
            memset(mbuf, 0, DEF_MOD_BUF_SIZE);
            while((ret = mod_logutils[i]->log_get_record(fd, (void **)&mbuf)) == 1) {
                uint32_t sz = rec_size(i, mbuf);
                if(sz) { w_u8(f, 'R'); w_u8(f, (uint8_t)i); w_u32(f, sz); fwrite(mbuf, sz, 1, f); n++; }
                memset(mbuf, 0, DEF_MOD_BUF_SIZE);
            }
        }
        fprintf(stderr, "[dump] %-8s %d records\n", darshan_module_names[i], n);
    }
    free(mbuf);

    w_u8(f, 'Z');
    fclose(f);
    darshan_log_close(fd);
    return 0;
}

/* ---- build: snapshot stream -> reconstructed log (THE reconstructor) ------ */
static int do_build(const char *in, const char *out)
{
    FILE *f = fopen(in, "rb");
    if(!f) { fprintf(stderr, "reconstruct: cannot open %s\n", in); return -1; }

    struct darshan_job job; memset(&job, 0, sizeof(job)); int have_job = 0;
    char *exe = NULL;
    struct darshan_mnt_info *mnts = NULL; uint32_t nmnt = 0;
    struct darshan_name_record_ref *nh = NULL, *ref;
    uint8_t partial = 0;
    int tag;

    while((tag = fgetc(f)) != EOF) {
        uint32_t len, cnt; uint64_t id;
        switch(tag) {
        case 'F': r_u8(f, &partial); break;
        case 'J': r_u32(f, &len); if(fread(&job, len, 1, f)==1) have_job=1; break;
        case 'E': r_u32(f, &len); exe = malloc(len); if(fread(exe, len, 1, f)!=1){} break;
        case 'M':
            r_u32(f, &cnt); nmnt = cnt;
            mnts = calloc(cnt ? cnt : 1, sizeof(struct darshan_mnt_info));
            for(uint32_t k = 0; k < cnt; k++) { r_u32(f, &len); if(fread(&mnts[k], len, 1, f)!=1){} }
            break;
        case 'N':
            r_u64(f, &id); r_u32(f, &len);
            ref = malloc(sizeof(*ref));
            ref->name_record = malloc(sizeof(struct darshan_name_record) + len);
            ref->name_record->id = (darshan_record_id)id;
            if(fread(ref->name_record->name, len, 1, f)!=1){}
            HASH_ADD(hlink, nh, name_record->id, sizeof(darshan_record_id), ref);
            break;
        case 'R': {
            uint8_t m; r_u8(f, &m); r_u32(f, &len);
            void *buf = malloc(len);
            if(fread(buf, len, 1, f)==1) {
                darshan_record_id rid = ((struct darshan_base_record *)buf)->id;
                rec_upsert(m, rid, buf, len);
            }
            free(buf);
            break;
        }
        case 'Z': goto done;
        default: fprintf(stderr, "reconstruct: bad tag 0x%02x\n", tag); goto done;
        }
    }
done:
    fclose(f);

    /* --- write the reconstructed log (darshan-convert's sequence) --------- */
    darshan_fd out_fd = darshan_log_create(out, DARSHAN_ZLIB_COMP, partial);
    if(!out_fd) { fprintf(stderr, "reconstruct: cannot create %s\n", out); return -1; }

    if(!have_job) fprintf(stderr, "reconstruct: WARN no job record (crash before init?) -- synthesizing empty\n");
    darshan_log_put_job(out_fd, &job);
    darshan_log_put_exe(out_fd, exe ? exe : (char *)"");
    darshan_log_put_mounts(out_fd, mnts, (int)nmnt);
    darshan_log_put_namehash(out_fd, nh);

    int total = 0;
    for(int i = 0; i < DARSHAN_MAX_MODS; i++) {
        if(!rec_head[i] || !mod_logutils[i]) continue;
        struct rec_ent *e, *t;
        HASH_ITER(hh, rec_head[i], e, t) {
            if(mod_logutils[i]->log_put_record(out_fd, e->buf) == 0) total++;
        }
        fprintf(stderr, "reconstruct: %-8s %u records\n",
                darshan_module_names[i], HASH_CNT(hh, rec_head[i]));
    }
    darshan_log_close(out_fd);
    fprintf(stderr, "reconstruct: wrote %d records -> %s%s\n",
            total, out, partial ? " (partial)" : "");
    return 0;
}

int main(int argc, char **argv)
{
    if(argc == 4 && strcmp(argv[1], "dump")  == 0) return do_dump (argv[2], argv[3]) ? 1 : 0;
    if(argc == 4 && strcmp(argv[1], "build") == 0) return do_build(argv[2], argv[3]) ? 1 : 0;
    fprintf(stderr,
        "Usage:\n"
        "  %s dump  <in.darshan> <out.snap>     # emit snapshot stream (emitter stand-in)\n"
        "  %s build <in.snap>    <out.darshan>  # reconstruct log from snapshots\n",
        argv[0], argv[0]);
    return 2;
}
