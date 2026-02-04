# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OCaml project that stores Bitcoin transaction graph data from BlockSci CSV exports into Irmin. Uses the `eio` branch of Irmin from https://github.com/mirage/irmin for concurrent I/O.

## Build Commands

```bash
# Create local switch with all dependencies (pins are in irmin-blocksci.opam)
make switch
# or: opam switch create . --deps-only

# Build
make build

# Generate documentation
dune build @doc
# Output: _build/default/_doc/_html/index.html

# Clean
make clean
```

## Usage

```bash
# Import BlockSci CSV export into Irmin store (supports incremental import)
dune exec irmin-blocksci -- import <csv-export-dir>

# Query commands
dune exec irmin-blocksci -- query block <height>       # Query block by height
dune exec irmin-blocksci -- query tx <tx_id>           # Query transaction by ID
dune exec irmin-blocksci -- query balance <address_id> # Query address balance
dune exec irmin-blocksci -- query chain <start> -n <count>  # Query block range
dune exec irmin-blocksci -- query output <tx_id:vout>  # Query output
dune exec irmin-blocksci -- query info                 # Show store info (last block height)

# Start GraphQL server
dune exec irmin-blocksci -- serve                      # Start on default port 8080
dune exec irmin-blocksci -- serve -p 3000              # Start on custom port
# GraphiQL UI: http://localhost:8080/graphiql
# API endpoint: http://localhost:8080/graphql
```

## Dependencies

Requires OCaml 5.1+. All pin-depends are declared in `irmin-blocksci.opam`, so `opam switch create . --deps-only` handles everything automatically.

## Architecture

### Library (`lib/`)

- `types.ml` - Data types (block, transaction, output, input, address) with JSON serialization
- `store.ml` - Irmin store configuration using `Irmin.Contents.String`, path helpers for hierarchical storage
- `import.ml` - CSV parsing for BlockSci export files (nodes + relationships), supports incremental import
- `query.ml` - Query functions with equivalent Cypher queries documented in odoc comments:
  - `get_block`, `get_transaction`, `get_output`, `get_address` - basic lookups
  - `block_transactions`, `tx_inputs`, `tx_outputs` - graph traversals
  - `address_outputs`, `address_balance` - address queries
  - `block_chain`, `block_with_coinbase`, `tx_details` - aggregate queries
  - `last_block_height` - get highest block in store
  - `PathFinder.find_path_between_outputs`, `PathFinder.find_path_between_addresses` - path finding
- `graphql_server.ml` - GraphQL server with blockchain-specific queries:
  - `block`, `transaction`, `output`, `address` - single entity lookups
  - `blockTransactions`, `addressOutputs`, `blockChain` - list queries
  - `addressBalance`, `storeInfo` - computed values

### Binary (`bin/`)

- `main.ml` - CLI using Cmdliner with `import`, `query`, and `serve` subcommands

### C Example (`c_example/`)

Proof of concept for querying the store from C using libirmin bindings.

**Build:**
```bash
cd ~/caml/irmin-eio && dune build src/libirmin/lib/  # Build libirmin first
cd ~/caml/irmin-blocksci/c_example && make
```

**Run (store must be beneath cwd due to Eio sandbox):**
```bash
cp -r /tmp/irmin-blocksci-store ./local-store
./c_example/query_block ./local-store
```

**Path constraints:** Eio uses `openat2` with `RESOLVE_BENEATH` - store path must be relative and beneath the working directory (no `..` or absolute paths).

### Benchmark (`bench/`)

Benchmark application implementing queries from the BlockSci paper Table 7 with Cypher equivalents documented in odoc comments.

**Run:**
```bash
dune exec bench/benchmark.exe -- /tmp/irmin-blocksci-store
```

**Queries (from BlockSci paper Table 7):**
- Q1: Count transactions with locktime > 0
- Q2: Find maximum output value
- Q3: Find maximum transaction fee
- Q4: Sum outputs to a specific address (Satoshi Dice style)
- Q5: Count zero-conf outputs (spent in same block as created)
- Q6: Locktime change heuristic (privacy analysis)

### Store Schema

```
/block/<height>                      -> Block JSON
/tx/<tx_id>                          -> Transaction JSON
/output/<tx_id>/<vout>               -> Output JSON
/address/<address_id>                -> Address JSON
/index/block_txs/<height>/<idx>      -> TxRef (block contains tx)
/index/tx_inputs/<tx_id>/<idx>       -> Input JSON
/index/tx_outputs/<tx_id>/<vout>     -> OutputRef
/index/addr_outputs/<addr>/<ref>     -> OutputRef
/index/output_addr/<tx_id>/<vout>    -> AddrRef
/index/spent_by/<tx_id>/<vout>       -> TxRef (which tx spent this output)
/meta/block_tx_count/<height>        -> Meta (tx count for incremental import)
```

### CSV Export Format (from Neo4j)

**Nodes:** `addresses.csv`, `blocks.csv`, `outputs.csv`, `transactions.csv`
**Relationships:** `contains.csv`, `to_address.csv`, `tx_input.csv`, `tx_output.csv`

Default store location: `/tmp/irmin-blocksci-store`
