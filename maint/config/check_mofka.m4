AC_DEFUN([CHECK_MOFKA],
[
PKG_CHECK_MODULES([MOFKA], [mofka],
    [AC_DEFINE([HAVE_MOFKA], [1], [Define if mofka client library is available])],
    [AC_MSG_ERROR([mofka.pc not found by pkg-config. Check PKG_CONFIG_PATH or use --with-mofka=PREFIX to point at the install.])])
])
