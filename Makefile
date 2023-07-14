PRODUCT = tcpproxy
VERSION = 1.0

SHELL = /bin/sh
top_srcdir = .
srcdir = .

.SUFFIXES:
.SUFFIXES: .c .o

CC = gcc
DEFINES = -DHAVE_CONFIG_H
CFLAGS = -I. -g -O2 -Wall $(DEFINES)
LDFLAGS = 
LIBS = 
INSTALL = /usr/bin/install -c
prefix = /usr/local
datarootdir = ${prefix}/share
exec_prefix = ${prefix}
bindir = ${exec_prefix}/bin
mandir = ${datarootdir}/man

DISTFILES =

TARGET=tcpproxy
SOURCES=tcpproxy.c hexdump.c
OBJS=tcpproxy.o hexdump.o
LIB_OBJS=

all: $(TARGET)

install: all
	$(top_srcdir)/mkinstalldirs $(bindir)
	$(INSTALL) $(TARGET) $(bindir)/$(TARGET)
##	$(top_srcdir)/mkinstalldirs $(mandir)/man1
##	$(INSTALL) $(MAN) $(mandir)/man1/$(MAN)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS) $(LDFLAGS) $(LIBS)

$(OBJS): $(SOURCES)

clean:
	/bin/rm -f $(TARGET) *.o core

distclean: clean config-clean

config-clean: confclean-recursive

confclean-recursive: cfg-clean

cfg-clean:
	/bin/rm -f Makefile config.h config.status config.cache config.log

mostlyclean: clean

maintainer-clean: clean

# automatic re-running of configure if the configure.in file has changed
${srcdir}/configure: configure.ac 
	cd ${srcdir} && autoconf

# autoheader might not change config.h.in, so touch a stamp file
${srcdir}/config.h.in: stamp-h.in
${srcdir}/stamp-h.in: configure.ac 
		cd ${srcdir} && autoheader
		echo timestamp > ${srcdir}/stamp-h.in

config.h: stamp-h
stamp-h: config.h.in config.status
	./config.status
Makefile: Makefile.in config.status
	./config.status
config.status: configure
	./config.status --recheck



