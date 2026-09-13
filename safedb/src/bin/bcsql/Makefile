#-------------------------------------------------------------------------
#
# Makefile for src/bin/bcsql
#
#
# src/bin/bcsql/Makefile
#
#-------------------------------------------------------------------------

PGFILEDESC = "bcsql - blockchain database portal"

subdir = src/bin/bcsql
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) -I$(libpq_srcdir) $(CPPFLAGS)
override CFLAGS := -ggdb -Og -g3 -fno-omit-frame-pointer $(CFLAGS)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)

OBJS = \
	startup.o

all: bcsql

bcsql: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	@if test -f bcsql$(X); then \
		$(INSTALL_SCRIPT) bcsql$(X) '$(DESTDIR)$(bindir)/bcsql$(X)'; \
	else \
		$(INSTALL_SCRIPT) '$(srcdir)/bcsql$(X)' '$(DESTDIR)$(bindir)/bcsql$(X)'; \
	fi

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/bcsql$(X)'

clean:
	rm -f $(OBJS) bcsql
