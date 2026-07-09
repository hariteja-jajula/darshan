# check_diaspora_c.m4 — configure-time detection of the diaspora-c bindings.
# Replaces the mofka-era checks wholesale. Note everything that DISAPPEARS
# relative to check_mofka.m4: no AC_PROG_CXX requirement, no C++17 probe,
# no PKG_CHECK for nlohmann_json, no --with-nlohmann-json-schema flag —
# all of that machinery existed only because darshan linked the C++ client
# directly. The bindings hide it (private linkage; see diaspora-c.pc).
#
# HAVE_MOFKA is deliberately kept as the output macro so the ~20 guarded
# call sites and darshan-mofka.{h,c} need zero changes.

AC_DEFUN([DARSHAN_CHECK_DIASPORA_C],[
    AC_ARG_WITH([diaspora-c],
        [AS_HELP_STRING([--with-diaspora-c@<:@=DIR@:>@],
            [enable real-time event streaming via the Diaspora Stream API C
             bindings (mofka and other drivers); DIR is the diaspora-c
             install prefix if not on PKG_CONFIG_PATH])],
        [], [with_diaspora_c=no])

    AM_CONDITIONAL([HAVE_MOFKA], [false])
    AS_IF([test "x$with_diaspora_c" != "xno"], [
        AS_IF([test "x$with_diaspora_c" != "xyes"], [
            # honor an explicit prefix; check both lib and share pkgconfig
            # dirs (the lesson from nlohmann_json living in share/pkgconfig)
            export PKG_CONFIG_PATH="$with_diaspora_c/lib/pkgconfig:$with_diaspora_c/share/pkgconfig:$PKG_CONFIG_PATH"
        ])
        PKG_CHECK_MODULES([DIASPORA_C], [diaspora-c],
            [have_diaspora_c=yes],
            [AC_MSG_ERROR([--with-diaspora-c given but pkg-config cannot
                find diaspora-c; is the bindings install on
                PKG_CONFIG_PATH, or pass --with-diaspora-c=PREFIX])])
        # When --with-diaspora-c=DIR is given (not just =yes), append
        # an -Wl,-rpath to DIASPORA_C_LIBS so libdarshan.so carries
        # DT_RPATH/DT_RUNPATH for libdiaspora-c at runtime. Without
        # this, callers need LD_LIBRARY_PATH=DIR/lib to resolve the
        # bindings (the spack-env view does NOT carry user-installed
        # prefixes). Idiom mirrors check_mofka.m4:36,:38 (the
        # nlohmann_json_schema rpath handling). libdiaspora-c.so itself
        # already has RPATH to its own transitive deps (set by CMake
        # at install time), so a single -Wl,-rpath here is sufficient
        # for the whole chain. Per P3runtime_MX_response.md.
        AS_IF([test "x$with_diaspora_c" != "xyes"], [
            DIASPORA_C_LIBS="$DIASPORA_C_LIBS -Wl,-rpath=$with_diaspora_c/lib"
        ])
        AC_DEFINE([HAVE_MOFKA], [1],
            [Define if Diaspora/Mofka real-time streaming support is built])
        AM_CONDITIONAL([HAVE_MOFKA], [true])
        AC_SUBST([DIASPORA_C_CFLAGS])
        AC_SUBST([DIASPORA_C_LIBS])
    ])
])
