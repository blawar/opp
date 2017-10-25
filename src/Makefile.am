# what flags you want to pass to the C compiler & linker
AM_CXXFLAGS = -I$(top_srcdir)/src

# this lists the binaries to produce, the (non-PHONY, binary) targets in
# the previous manual Makefile
lib_LTLIBRARIES = libopp.la
libopp_ladir = $(includedir)/opp
libopp_la_LIBADD = -lboost_system -lboost_filesystem -lboost_thread
libopp_la_SOURCES = \
cerberus.cpp \
cerberus.h \
expression.cpp \
expression.h \
storage/blob.cpp \
storage/blob.h \
storage/memory.cpp \
storage/memory.h \
storage/storage.cpp \
storage/storage.h \
storage/table.cpp \
storage/table.h \
storage/field.cpp \
storage/field.h

nobase_libopp_la_HEADERS = cerberus.h expression.h storage/table.h storage/storage.h storage/memory.h storage/blob.h storage/field.h
