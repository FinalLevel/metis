AUTOMAKE_OPTIONS = subdir-objects
ACLOCAL_AMFLAGS = ${ACLOCAL_FLAGS} -I m4

AM_CPPFLAGS=-I./fl_libs -DSYSCONFDIR=\"${sysconfdir}\" 

SUBDIRS = fl_libs
LDADD = fl_libs/libfl.a

METIS_FILES = metis_event.cpp index.cpp config.cpp metis_log.cpp

bin_PROGRAMS = metis
metis_SOURCES = metis.cpp $(METIS_FILES)

metisconfdir=$(sysconfdir)
metisconf_DATA = etc/metis.cnf

check_PROGRAMS = metis_test
metis_test_SOURCES = tests/test.cpp tests/index_test.cpp $(NOMOS_FILES)
metis_test_LDFLAGS = $(BOOST_LDFLAGS) $(BOOST_UNIT_TEST_FRAMEWORK_LIB)

TESTS = metis_test

