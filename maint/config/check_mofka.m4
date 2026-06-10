AC_DEFUN([CHECK_MOFKA],
[
dnl Mofka's diaspora-stream-api headers transitively include several
dnl headers that are NOT declared as Requires: in mofka.pc:
dnl   * <nlohmann/json.hpp>           (from nlohmann-json,           via .pc)
dnl   * <nlohmann/json-schema.hpp>    (from nlohmann-json-schema-validator,
dnl                                    ships NO .pc; located manually)
dnl We therefore probe pkg-config for what we can, and accept a
dnl --with-nlohmann-json-schema=DIR override for the rest.
PKG_CHECK_MODULES([MOFKA_CORE], [mofka],
    [],
    [AC_MSG_ERROR([mofka.pc not found by pkg-config. Check PKG_CONFIG_PATH or use --with-mofka=PREFIX to point at the install.])])

PKG_CHECK_MODULES([NLOHMANN_JSON], [nlohmann_json],
    [],
    [AC_MSG_ERROR([nlohmann_json.pc not found by pkg-config. Mofka headers require nlohmann/json.hpp; install nlohmann-json or add its pkgconfig directory (often share/pkgconfig) to PKG_CONFIG_PATH.])])

dnl nlohmann-json-schema-validator has no pkg-config file; let the user
dnl point at the install with --with-nlohmann-json-schema=DIR, or auto-
dnl probe via the C++ header check if the headers are already on the
dnl include path.
AC_ARG_WITH([nlohmann-json-schema],
   [AS_HELP_STRING([--with-nlohmann-json-schema@<:@=DIR@:>@],
                   [Installation directory for nlohmann-json-schema-validator (required by Mofka; the validator package does not ship a pkg-config file).])]
)

NLOHMANN_JSON_SCHEMA_CFLAGS=""
NLOHMANN_JSON_SCHEMA_LIBS=""
if test "x$with_nlohmann_json_schema" != x && test "x$with_nlohmann_json_schema" != xyes ; then
    if test -f "$with_nlohmann_json_schema/include/nlohmann/json-schema.hpp" ; then
        NLOHMANN_JSON_SCHEMA_CFLAGS="-I$with_nlohmann_json_schema/include"
    else
        AC_MSG_ERROR([nlohmann/json-schema.hpp not found under $with_nlohmann_json_schema/include])
    fi
    if test -f "$with_nlohmann_json_schema/lib64/libnlohmann_json_schema_validator.so" ; then
        NLOHMANN_JSON_SCHEMA_LIBS="-L$with_nlohmann_json_schema/lib64 -Wl,-rpath=$with_nlohmann_json_schema/lib64 -lnlohmann_json_schema_validator"
    elif test -f "$with_nlohmann_json_schema/lib/libnlohmann_json_schema_validator.so" ; then
        NLOHMANN_JSON_SCHEMA_LIBS="-L$with_nlohmann_json_schema/lib -Wl,-rpath=$with_nlohmann_json_schema/lib -lnlohmann_json_schema_validator"
    else
        AC_MSG_ERROR([libnlohmann_json_schema_validator.so not found under $with_nlohmann_json_schema/lib or /lib64])
    fi
fi

dnl Mofka's diaspora-stream-api headers use std::string_view and other
dnl C++17 features (e.g. inline static members, structured bindings).
dnl Probe that the C++ compiler accepts -std=c++17 and add it to
dnl MOFKA_CFLAGS so only the mofka translation unit gets the upgrade
dnl (other Darshan C/C++ code is unaffected).
saved_CXXFLAGS="$CXXFLAGS"
CXXFLAGS="$CXXFLAGS -std=c++17"
AC_LANG_PUSH([C++])
AC_MSG_CHECKING([whether C++ compiler accepts -std=c++17])
AC_COMPILE_IFELSE(
    [AC_LANG_PROGRAM([[#include <string_view>]],
                     [[std::string_view sv{"x"}; (void)sv;]])],
    [AC_MSG_RESULT([yes])
     MOFKA_CXX_STD_FLAG="-std=c++17"],
    [AC_MSG_RESULT([no])
     AC_MSG_ERROR([C++ compiler does not support -std=c++17; required for Mofka headers (std::string_view, etc.). Install a more recent compiler or build without --with-mofka.])])
AC_LANG_POP([C++])
CXXFLAGS="$saved_CXXFLAGS"

dnl Sanity check: with the assembled CFLAGS, can the C++ compiler find
dnl json-schema.hpp? If not, the user must pass --with-nlohmann-json-schema.
saved_CPPFLAGS="$CPPFLAGS"
saved_CXXFLAGS="$CXXFLAGS"
CPPFLAGS="$CPPFLAGS $MOFKA_CORE_CFLAGS $NLOHMANN_JSON_CFLAGS $NLOHMANN_JSON_SCHEMA_CFLAGS"
CXXFLAGS="$CXXFLAGS $MOFKA_CXX_STD_FLAG"
AC_LANG_PUSH([C++])
AC_CHECK_HEADER([nlohmann/json-schema.hpp], [],
    [AC_MSG_ERROR([nlohmann/json-schema.hpp not found. Mofka transitively requires nlohmann-json-schema-validator; install it and rerun configure with --with-nlohmann-json-schema=DIR.])])
AC_LANG_POP([C++])
CPPFLAGS="$saved_CPPFLAGS"
CXXFLAGS="$saved_CXXFLAGS"

MOFKA_CFLAGS="$MOFKA_CORE_CFLAGS $NLOHMANN_JSON_CFLAGS $NLOHMANN_JSON_SCHEMA_CFLAGS"
MOFKA_CXXFLAGS="$MOFKA_CFLAGS $MOFKA_CXX_STD_FLAG"
MOFKA_LIBS="$MOFKA_CORE_LIBS $NLOHMANN_JSON_LIBS $NLOHMANN_JSON_SCHEMA_LIBS"
AC_SUBST(MOFKA_CFLAGS)
AC_SUBST(MOFKA_CXXFLAGS)
AC_SUBST(MOFKA_LIBS)

AC_DEFINE([HAVE_MOFKA], [1], [Define if mofka client library is available])
])
