# C Example for irmin-blocksci

This directory contains a proof of concept for querying the irmin-blocksci store from C
using the libirmin C bindings.

## Status: Working (with path constraints)

The C bindings work with the **Eio branch** of Irmin, but require:

1. **Store on same filesystem** as the working directory
2. **Store path beneath cwd** - use relative paths like `./store` or `subdir/store`, not `..` or `/absolute/path`

### Working example:

```bash
# Copy store to local directory
cp -r /tmp/irmin-blocksci-store ./local-store

# Run from parent of store (store is "beneath" cwd)
./c_example/query_block ./local-store
```

### Why these constraints?

Eio uses `openat2` with `RESOLVE_BENEATH` for filesystem sandboxing. This is a security
feature that restricts file access to stay within the working directory tree.

## Alternative: GraphQL API

For paths outside the working directory (e.g., `/tmp`), use the GraphQL server:

```bash
# Start server
dune exec irmin-blocksci -- serve

# Query from C using libcurl
curl -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ block(height: 0) { height hash } }"}'
```

## Building the Example

```bash
# Build libirmin (from irmin-eio repo)
cd ~/caml/irmin-eio && dune build src/libirmin/lib/

# Compile this example
cd ~/caml/irmin-blocksci/c_example
make

# Copy store to local directory and run
cp -r /tmp/irmin-blocksci-store ../local-store
cd ..
./c_example/query_block ./local-store
```

## Output

```
=== irmin-blocksci C Query Example ===

1. Creating pack store config...
2. Setting store root to: ./local-store
3. Opening repository...
4. Getting main branch...
5. Creating path for 'block/0'...
6. Looking up block 0...

=== Block 0 (Genesis Block) ===
{"type":"block","height":0,"hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",...}

7. Cleaning up...
Done!
```

## Benchmark

A C port of the OCaml benchmark (`bench/benchmark.ml`) is also available:

```bash
# Build
cd ~/caml/irmin-blocksci/c_example
make benchmark

# Run (store must be beneath cwd)
cp -r /tmp/irmin-blocksci-store ../local-store
cd ..
./c_example/benchmark ./local-store
```

Output (CSV format):
```
Query,Time_ms,Result
Block count,123.456,274195
Tx count,234.567,101558
...
```

## Files

- `query_block.c` - C code demonstrating the libirmin API
- `benchmark.c` - C port of the benchmark suite
- `Makefile` - Build configuration
