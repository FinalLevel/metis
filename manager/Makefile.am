LDADD = ../fl_libs/libfl.a
AM_CPPFLAGS=-I../fl_libs -DSYSCONFDIR=\"${sysconfdir}\" $(MYSQL_INCLUDE)

bin_PROGRAMS = metis_manager
metis_manager_SOURCES = manager.cpp index.cpp metis_manager.cpp cluster_manager.cpp config.cpp web.cpp \
  webdav.cpp cmd_event.cpp ../metis_log.cpp ../global_config.cpp
metis_manager_LDFLAGS = $(MYSQL_LDFLAGS)