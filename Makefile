MODULE_big = pg_directpaths
REGRESS := pg_directpaths

REGRESS_OPTS := \
	--temp-config=test/pg_directpaths.conf \
	--inputdir=test \
	--outputdir=test \
	--temp-instance=${PWD}/tmpdb

SRCS = \
		src/pg_directpaths.c \
		src/hooks.c \
		src/cscan.c \
		src/insert_append.c \
		src/insert_append_indexes.c \
		src/direct_paths_explain.c

OBJS = $(SRCS:.c=.o)

ifdef USE_PGXS
override PG_CPPFLAGS += -I$(CURDIR)/src/include
else
override PG_CPPFLAGS += -I$(top_srcdir)/$(subdir)/src/include
endif

PG_CONFIG ?= pg_config
PG_CPPFLAGS = -g -O2

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
