# Makefile for irmin-blocksci C example
#
# Prerequisites:
#   - Build libirmin: cd ~/caml/irmin-eio && dune build src/libirmin/lib/

IRMIN_DIR = $(HOME)/caml/irmin-eio/_build/default/src/libirmin/lib
IRMIN_INSTALL = $(HOME)/caml/irmin-eio/_build/install/default/lib/libirmin

CC = gcc
CFLAGS = -Wall -I$(IRMIN_DIR) -I$(IRMIN_INSTALL)/include
LDFLAGS = -L$(IRMIN_DIR) -L$(IRMIN_INSTALL) -Wl,-rpath,$(IRMIN_DIR)

# libirmin requires OCaml runtime libraries
LIBS = -lirmin

.PHONY: all clean run run-benchmark

all: query_block benchmark

query_block: query_block.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

benchmark: benchmark.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

run: query_block
	LD_LIBRARY_PATH=$(IRMIN_DIR):$(IRMIN_INSTALL) ./query_block

run-benchmark: benchmark
	LD_LIBRARY_PATH=$(IRMIN_DIR):$(IRMIN_INSTALL) ./benchmark

clean:
	rm -f query_block benchmark
