LDADD = ../fl_libs/libfl.a
AM_CPPFLAGS=-I../fl_libs -DSYSCONFDIR=\"${sysconfdir}\" 


METIS_STORAGE_FILES = config.cpp slice.cpp storage_event.cpp ../metis_log.cpp

bin_PROGRAMS = metis_storage
metis_storage_SOURCES = metis_storage.cpp $(METIS_STORAGE_FILES)

check_PROGRAMS = metis_storage_test
metis_storage_test_SOURCES = tests/test.cpp tests/slice_test.cpp $(METIS_STORAGE_FILES)
metis_storage_test_LDFLAGS = $(BOOST_LDFLAGS) $(BOOST_UNIT_TEST_FRAMEWORK_LIB)

TESTS = metis_storage_test

