#!/bin/bash
# build.sh -- build this darshan (the mofka / diaspora-c fork) into a standalone
# install. Self-contained: the only hard dependency is a diaspora-c install and
# a C compiler. Run it from the repo root:
#
#   DIASPORA_C=/path/to/diaspora-c-install ./build.sh
#
# Env knobs (only DIASPORA_C is required):
#   DIASPORA_C            diaspora-c install prefix (has include/diaspora/diaspora_c.h + lib/)
#   PREFIX               install dir                       (default: ./install)
#   CC / CXX             compilers                         (default: gcc/g++, else cc/c++)
#   DARSHAN_INCREMENTAL=1 reuse a matching _build for fast same-machine rebuilds
#
# The log-file location is intentionally NOT baked in: we configure with
# --with-log-path-by-env=DARSHAN_LOGPATH, so the CONSUMER (the harness) chooses
# the log dir at run time via $DARSHAN_LOGPATH. That keeps this build independent
# of any particular harness layout.
set -e
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

: "${DIASPORA_C:?set DIASPORA_C to your diaspora-c install prefix (dir with include/diaspora/diaspora_c.h)}"
[ -f "$DIASPORA_C/include/diaspora/diaspora_c.h" ] || {
    echo "build.sh: no include/diaspora/diaspora_c.h under DIASPORA_C=$DIASPORA_C"; exit 1; }

SRC="$HERE/darshan-runtime"
if [ "${DARSHAN_MPI:-0}" = "1" ]; then
    CC="${CC:-$(command -v mpicc)}"
    CXX="${CXX:-$(command -v mpicxx || command -v mpic++)}"
    PREFIX="${PREFIX:-$HERE/install-mpi}"
    BUILD="$HERE/_build-mpi"
    MPI_FLAG=""
    echo "=== WITH-MPI build: CC=$CC prefix=$PREFIX ==="
else
    CC="${CC:-$(command -v gcc || command -v cc)}"
    CXX="${CXX:-$(command -v g++ || command -v c++)}"
    PREFIX="${PREFIX:-$HERE/install}"
    BUILD="$HERE/_build"
    MPI_FLAG="--without-mpi"
fi

# --- ensure the build tree belongs to THIS srcdir ----------------------------
# config.status records the srcdir it was configured with; a stale/foreign one
# (copied machine, moved path) makes `make` chase files that don't exist. Wipe
# unless the caller asked to keep a matching tree.
if [ -f "$BUILD/config.status" ]; then
    cfg_srcdir="$("$BUILD/config.status" --config 2>/dev/null \
                    | tr ' ' '\n' | sed -n "s/^'\?--srcdir=//p" | tr -d \"\' | head -1)"
    if [ "${DARSHAN_INCREMENTAL:-0}" = "1" ] && [ "$cfg_srcdir" = "$SRC" ]; then
        echo "=== reusing _build (DARSHAN_INCREMENTAL=1) ==="
    else
        echo "=== wiping stale _build (was: ${cfg_srcdir:-unknown}) ==="; rm -rf "$BUILD"
    fi
fi
mkdir -p "$BUILD" "$PREFIX"

# bootstrap autotools only if the generated configure is missing. darshan-runtime
# is its own autotools package (no prepare.sh of its own; the repo-root prepare.sh
# is just `autoreconf -fi`), so bootstrap it directly here.
[ -f "$SRC/configure" ] || ( cd "$SRC" && autoreconf -fi )

# defeat maintainer-mode regeneration: bump generated files >= their sources so
# `make` doesn't try to re-run aclocal/automake (dies if the local automake
# version differs from the one that produced the committed output).
if [ -f "$SRC/configure" ]; then
    touch "$SRC/aclocal.m4"                          2>/dev/null || true
    touch "$SRC/configure" "$SRC"/*.h.in             2>/dev/null || true
    find "$SRC" -name 'Makefile.in' -exec touch {} + 2>/dev/null || true
fi

JOBS="$( (command -v nproc >/dev/null 2>&1 && nproc) || echo 4)"

cd "$BUILD"
sh "$SRC/configure" \
    ${CC:+CC="$CC"} ${CXX:+CXX="$CXX"} \
    --prefix="$PREFIX" \
    --with-log-path-by-env=DARSHAN_LOGPATH \
    --with-jobid-env=PBS_JOBID \
    $MPI_FLAG \
    --with-diaspora-c="$DIASPORA_C" \
    DIASPORA_C_CFLAGS="-I$DIASPORA_C/include" \
    DIASPORA_C_LIBS="-L$DIASPORA_C/lib -ldiaspora-c -ldiaspora-stream-api -lstdc++" \
    PKG_CONFIG_PATH="$DIASPORA_C/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
make -j"$JOBS"
make install

# some libtool/install combos skip the unversioned dev symlink; LD_PRELOAD and
# the harness's darshan_lib() rely on it, so create it if missing.
if [ ! -e "$PREFIX/lib/libdarshan.so" ]; then
    so="$(ls "$PREFIX"/lib/libdarshan.so.* 2>/dev/null | sort | head -1)"
    [ -n "$so" ] && ln -sf "$(basename "$so")" "$PREFIX/lib/libdarshan.so"
fi

echo "=== built: $(ls "$PREFIX"/lib/libdarshan.so* 2>/dev/null | tr '\n' ' ')==="

# --- darshan-util: darshan-parser + darshan-mofka-reconstruct (README step 11) ---
# Built into the same PREFIX. rpath so the tools load THIS libdarshan-util.so, not
# an older one that may sit ahead on LD_LIBRARY_PATH (e.g. a system/spack darshan).
UTIL="$HERE/darshan-util"
UBUILD="$UTIL/_build"
[ -f "$UTIL/configure" ] || ( cd "$UTIL" && autoreconf -fi )
mkdir -p "$UBUILD"
cd "$UBUILD"
sh "$UTIL/configure" ${CC:+CC="$CC"} --prefix="$PREFIX" \
    LDFLAGS="-Wl,-rpath,$PREFIX/lib"
# make untars the bundled uthash with `tar xjf`; if the bzip2 on PATH is a broken
# symlink (seen with some spack views), pre-extract with a working bzip2 first.
if [ ! -d uthash-1.9.2 ]; then
    for bz in /usr/bin/bzip2 "$(command -v bzip2 2>/dev/null)"; do
        [ -x "$bz" ] && "$bz" --help >/dev/null 2>&1 || continue
        tar --use-compress-program="$bz" -xf "$UTIL/extern/uthash-1.9.2.tar.bz2" && break
    done
fi
make -j"$JOBS"
make install

echo "=== util: $(ls "$PREFIX"/bin/darshan-parser "$PREFIX"/bin/darshan-mofka-reconstruct 2>/dev/null | tr '\n' ' ')==="
echo "=== install prefix: $PREFIX ==="
echo "point the harness at it:  export DARSHAN_PREFIX=$PREFIX"
