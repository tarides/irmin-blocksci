# C Example for irmin-blocksci

This directory contains a proof of concept for querying the irmin-blocksci store from C
using the libirmin C bindings.

## Status: Known Limitations

The **Eio branch** of Irmin's libirmin has compatibility issues when called from C:

1. **Eio filesystem sandbox**: The Eio runtime uses `openat2` with restricted capabilities,
   which fails when the store path crosses filesystem boundaries or isn't accessible
   relative to the working directory.

2. **Error observed**:
   ```
   Fatal error: exception Eio.Io Fs Permission_denied Unix_error (Invalid cross-device link, "openat2", "")
   ```

## Working Alternatives

### 1. Use GraphQL API (Recommended)

Query the store via the GraphQL server:

```bash
# Start server
dune exec irmin-blocksci -- serve

# Query from C using libcurl
curl -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ block(height: 0) { height hash } }"}'
```

### 2. Use the Lwt-based irmin branch

The main (Lwt-based) Irmin branch has more mature C bindings. If you need direct
C integration, consider using the non-Eio version of Irmin.

### 3. Create a custom OCaml-to-C wrapper

Build a custom OCaml library that initializes the Eio environment properly and
exposes simple C functions specific to your use case.

## Building the Example (for reference)

```bash
# Build libirmin (from irmin-eio repo)
cd ~/caml/irmin-eio && dune build src/libirmin/lib/

# Compile this example
make

# Run (currently fails due to Eio limitations)
make run
```

## Files

- `query_block.c` - Proof of concept C code demonstrating the libirmin API
- `Makefile` - Build configuration
