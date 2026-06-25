/*
 * Copyright (C) 2015 University of Chicago.
 * See COPYRIGHT notice in top-level directory.
 *
 */

#ifdef HAVE_CONFIG_H
# include <darshan-runtime-config.h>
#endif

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <sys/syscall.h>
#ifdef HAVE_MPI
#include <mpi.h>
#endif

#include "darshan.h"
#include "darshan-dynamic.h"

/* EX-2026-06-25 finalize-end markers (L4.x): print monotonic timestamps
 * at darshan-core finalize boundaries so we can bracket how much wall
 * time is spent AFTER our last instruction. Subtracting this from bash's
 * wall_end measurement gives the cost of libmofka/libthallium static
 * destructors + kernel reap. Always printed when DARSHAN_ENABLE_NONMPI=1
 * (the only path that calls serial_finalize). Match darshan-mofka's
 * stderr prefix style for grep-ability. */
static inline long long _darshan_now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000000LL + (long long)ts.tv_nsec;
}

#ifdef HAVE_MPI
DARSHAN_FORWARD_DECL(PMPI_Finalize, int, ());
DARSHAN_FORWARD_DECL(PMPI_Init, int, (int *argc, char ***argv));
DARSHAN_FORWARD_DECL(PMPI_Init_thread, int, (int *argc, char ***argv, int required, int *provided));

int DARSHAN_DECL(MPI_Init)(int *argc, char ***argv)
{
    int ret, __darshan_disabled;

    MAP_OR_FAIL(PMPI_Init);
    (void)__darshan_disabled;

    ret = __real_PMPI_Init(argc, argv);
    if(ret != MPI_SUCCESS)
    {
        return(ret);
    }

    if(argc && argv)
    {
        darshan_core_initialize(*argc, *argv);
    }
    else
    {
        /* we don't see argc and argv here in fortran */
        darshan_core_initialize(0, NULL);
    }

    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_Init, int, (int *argc, char ***argv), MPI_Init)

int DARSHAN_DECL(MPI_Init_thread)(int *argc, char ***argv, int required, int *provided)
{
    int ret, __darshan_disabled;

    MAP_OR_FAIL(PMPI_Init_thread);
    (void)__darshan_disabled;

    ret = __real_PMPI_Init_thread(argc, argv, required, provided);
    if(ret != MPI_SUCCESS)
    {
        return(ret);
    }

    if(argc && argv)
    {
        darshan_core_initialize(*argc, *argv);
    }
    else
    {
        /* we don't see argc and argv here in fortran */
        darshan_core_initialize(0, NULL);
    }

    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_Init_thread, int, (int *argc, char ***argv, int required, int *provided), MPI_Init_thread)

int DARSHAN_DECL(MPI_Finalize)(void)
{
    int ret, __darshan_disabled;

    MAP_OR_FAIL(PMPI_Finalize);
    (void)__darshan_disabled;

    darshan_core_shutdown(1);

    ret = __real_PMPI_Finalize();
    return(ret);
}
DARSHAN_WRAPPER_MAP(PMPI_Finalize, int, (void), MPI_Finalize)
#endif

/*
 * Initialization hook that does not rely on MPI
 */
#ifdef __GNUC__
__attribute__((constructor)) void serial_init(void)
{
    char *no_mpi = getenv("DARSHAN_ENABLE_NONMPI");
    if (no_mpi)
        darshan_core_initialize(0, NULL);
    return;
}

__attribute__((destructor)) void serial_finalize(void)
{
    char *no_mpi = getenv("DARSHAN_ENABLE_NONMPI");
    if (!no_mpi) return;

    /* Bracket the entire darshan-core finalize work with monotonic
     * timestamps. T_FINALIZE_BEGIN fires at the START of serial_finalize;
     * T_FINALIZE_END fires at the END, immediately before this function
     * returns. The delta is the cost of darshan_core_shutdown (which
     * itself calls darshan_mofka_connector_finalize early on).
     *
     * After T_FINALIZE_END, control returns to the C runtime which then
     * runs C++ static destructors for all dynamically-loaded shared
     * objects (libmofka, libthallium, libargobots, libmercury, libfabric,
     * ...). Compare T_FINALIZE_END to bash's wall_end timestamp to get
     * the cost of those static dtors. */
    long long t_begin_ns = _darshan_now_ns();
    fprintf(stderr,
            "darshan-core[T_FINALIZE_BEGIN] pid=%ld t_wall_ns=%lld\n",
            (long)getpid(), t_begin_ns);
    fflush(stderr);

    darshan_core_shutdown(1);

    long long t_end_ns = _darshan_now_ns();
    fprintf(stderr,
            "darshan-core[T_FINALIZE_END] pid=%ld t_wall_ns=%lld "
            "delta_ms=%.3f\n",
            (long)getpid(), t_end_ns,
            (double)(t_end_ns - t_begin_ns) / 1.0e6);
    fflush(stderr);

    /* EX-2026-06-25 DARSHAN_MOFKA_FAST_EXIT: kill the ~3.6s of libmofka
     * static C++ destructors that fire AFTER serial_finalize returns
     * during __cxa_finalize. By calling _exit(0) here, we bypass:
     *   - libstdc++ atexit chain
     *   - libmofka / libthallium / libargobots / libmercury / libfabric
     *     static C++ destructors
     *   - libc's exit-time cleanup
     * The kernel immediately reaps all process resources via exit_group.
     *
     * Safety: PROF has already been flushed (above). The .darshan log
     * has been written (inside darshan_core_shutdown). Mongo events
     * already in-flight to broker will complete on the broker side
     * independently of producer exit.
     *
     * This is the strongest possible "fire-and-forget" semantic: matches
     * LDMS's "no shutdown function at all" pattern. The 3.6s of libmofka
     * static dtors that L4.33 attributed by elimination is now provably
     * skipped (process is gone before they could fire).
     *
     * Opt-in: set DARSHAN_MOFKA_FAST_EXIT=1. Defaults to OFF so that
     * any downstream code expecting normal exit semantics keeps working. */
    if (getenv("DARSHAN_MOFKA_FAST_EXIT") != NULL) {
        long long t_fastexit_ns = _darshan_now_ns();
        fprintf(stderr,
                "darshan-core[T_FAST_EXIT] pid=%ld t_wall_ns=%lld "
                "(syscall exit_group bypassing C++ static dtors)\n",
                (long)getpid(), t_fastexit_ns);
        fflush(stderr);
        fflush(stdout);
        /* Use raw exit_group syscall to bypass any libc/libdarshan
         * exit wrapping (the __DARSHAN_ENABLE_EXIT_WRAPPER build option
         * would intercept _exit() and re-enter darshan_core_shutdown,
         * causing a loop). */
        syscall(SYS_exit_group, 0);
        /* Should never reach here, but in case: */
        _exit(0);
    }
    return;
}
#endif

#if defined(DARSHAN_PRELOAD) && defined(__DARSHAN_ENABLE_EXIT_WRAPPER)
void (*__real__exit)(int status) __attribute__ ((noreturn)) = NULL;
void _exit(int status)
{
    int __darshan_disabled;

    MAP_OR_FAIL(_exit);
    (void)__darshan_disabled;

    char *no_mpi = getenv("DARSHAN_ENABLE_NONMPI");
    if (no_mpi)
        darshan_core_shutdown(1);

    __real__exit(status);
}
#endif

/*
 * Local variables:
 *  c-indent-level: 4
 *  c-basic-offset: 4
 * End:
 *
 * vim: ts=8 sts=4 sw=4 expandtab
 */
