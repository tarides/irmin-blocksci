.PHONY: all build clean switch

all: build

switch:
	opam switch create . --deps-only

build:
	dune build

clean:
	dune clean

run:
	dune exec irmin-blocksci
