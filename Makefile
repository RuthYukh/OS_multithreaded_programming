CC=g++
CXX=g++
AR=ar
RANLIB=ranlib
RM=rm -f

LIBSRC=MapReduceFramework.cpp
LIBOBJ=$(LIBSRC:.cpp=.o)

INCS=-I.
CFLAGS = -Wall -std=c++11 -g $(INCS)
CXXFLAGS = -Wall -std=c++11 -g $(INCS)

TARGETS = libMapReduceFramework.a

TAR=tar
TARFLAGS=-cvf
TARNAME=ex3.tar
TARSRCS=$(LIBSRC) Makefile README

all: $(TARGETS)

$(TARGETS): $(LIBOBJ)
	$(AR) rcs $@ $^
	$(RANLIB) $@

clean:
	$(RM) $(TARGETS) $(LIBOBJ) *~ *core

depend:
	makedepend -- $(CFLAGS) -- $(SRC) $(LIBSRC)

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)

.PHONY: all clean depend tar