AC_INIT
AC_CONFIG_SRCDIR([tcpproxy.c])
PRODUCT=tcpproxy
VERSION=1.0
AC_CONFIG_HEADERS([config.h])
AC_PROG_CC
AC_DEFINE_UNQUOTED(PRODUCT, "$PRODUCT")
AC_DEFINE_UNQUOTED(VERSION, "$VERSION")
AC_SUBST(PRODUCT)
AC_SUBST(VERSION)

dnl Checks for programs.
AC_PROG_CC
if test $CC = "gcc"; then
	CFLAGS="$CFLAGS -Wall"
fi
AC_PROG_INSTALL
AC_PROG_LN_S

dnl Checks for header files.

dnl Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST

dnl Checks for library functions.

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
