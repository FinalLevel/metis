AUTOMAKE_OPTIONS = subdir-objects
ACLOCAL_AMFLAGS = ${ACLOCAL_FLAGS} -I m4

AM_CPPFLAGS=-I./fl_libs -DSYSCONFDIR=\"${sysconfdir}\" 

SUBDIRS = fl_libs manager storage
LDADD = fl_libs/libfl.a

metisconfdir=$(sysconfdir)
metisconf_DATA = etc/metis.cnf
