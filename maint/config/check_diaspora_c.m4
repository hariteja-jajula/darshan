# check_diaspora_c.m4 — configure-time detection of the diaspora-c bindings.
# Outputs HAVE_MOFKA (kept as the macro name so existing guarded call sites
# and darshan-mofka.{h,c} are unchanged).

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
            # honor an explicit prefix (check both lib and share pkgconfig dirs)
            export PKG_CONFIG_PATH="$with_diaspora_c/lib/pkgconfig:$with_diaspora_c/share/pkgconfig:$PKG_CONFIG_PATH"
        ])
        PKG_CHECK_MODULES([DIASPORA_C], [diaspora-c],
            [have_diaspora_c=yes],
            [AC_MSG_ERROR([--with-diaspora-c given but pkg-config cannot
                find diaspora-c; is the bindings install on
                PKG_CONFIG_PATH, or pass --with-diaspora-c=PREFIX])])
        # with an explicit prefix, rpath libdiaspora-c into libdarshan.so so
        # it resolves at runtime without LD_LIBRARY_PATH.
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
