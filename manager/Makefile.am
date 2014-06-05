LDADD = ../fl_libs/libfl.a
AM_CPPFLAGS=-I../fl_libs -DSYSCONFDIR=\"${sysconfdir}\" $(MYSQL_INCLUDE)

METIS_MANAGER_FILES = index.cpp manager.cpp cluster_manager.cpp config.cpp web.cpp cache.cpp \
  webdav.cpp cmd_event.cpp storage_cmd_event.cpp ../metis_log.cpp ../global_config.cpp

bin_PROGRAMS = metis_manager
metis_manager_SOURCES = metis_manager.cpp $(METIS_MANAGER_FILES)
metis_manager_LDFLAGS = $(MYSQL_LDFLAGS)

check_PROGRAMS = metis_manager_test
metis_manager_test_SOURCES = tests/test.cpp tests/cache_test.cpp tests/manager_test.cpp tests/test_config.cpp \
  $(METIS_MANAGER_FILES)
metis_manager_test_LDFLAGS = $(BOOST_LDFLAGS) $(BOOST_UNIT_TEST_FRAMEWORK_LIB) $(MYSQL_LDFLAGS)

TESTS = metis_manager_test
