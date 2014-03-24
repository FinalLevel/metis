AC_INIT([Metis Storage Server], [0.1], [bug-report@final-level.com], [metis], [https://github.com/FinalLevel/metis])
AC_PREREQ([2.59])
AC_PROG_RANLIB
AM_INIT_AUTOMAKE([1.10 -Wall foreign])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])
AC_ARG_ENABLE(debug,
[  --enable-debug    Turn on debugging],
[case "${enableval}" in
  yes) debug=true ;;
  no)  debug=false ;;
  *) AC_MSG_ERROR(bad value ${enableval} for --enable-debug) ;;
esac],[debug=false])
AM_CONDITIONAL(DEBUG, test x$debug = xtrue)
CXXFLAGS=-pthread
AS_IF([test "x$debug" = xtrue], [ CXXFLAGS+=' -g'])
AS_IF([test "x$debug" != xtrue], [ CXXFLAGS+=' -g -O3'])

AX_BOOST_BASE([1.41])

AC_PROG_CXX
AC_LANG([C++])
AX_CXX_COMPILE_STDCXX_11(noext,optional)
AS_IF([test "x$HAVE_CXX11" = "x1"], [CXXFLAGS+=' -Wall -std=c++11'])
AS_IF([test "x$HAVE_CXX11" != "x1"], [CXXFLAGS+=' -Wall -std=c++0x'])


AX_BOOST_UNIT_TEST_FRAMEWORK

AC_CHECK_HEADER(cstdint)

AC_SUBST(CXXFLAGS)
AC_OUTPUT(Makefile fl_libs/Makefile manager/Makefile storage/Makefile)
