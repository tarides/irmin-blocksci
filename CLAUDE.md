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

# Clean
make clean
```

## Usage

```bash
# Import BlockSci CSV export into Irmin store
dune exec irmin-blocksci -- import <csv-export-dir>

# Query commands
dune exec irmin-blocksci -- query block <height>       # Query block by height
dune exec irmin-blocksci -- query tx <tx_id>           # Query transaction by ID
dune exec irmin-blocksci -- query balance <address_id> # Query address balance
dune exec irmin-blocksci -- query chain <start> -n <count>  # Query block range
dune exec irmin-blocksci -- query output <tx_id:vout>  # Query output
```

## Dependencies

Requires OCaml 5.1+. All pin-depends are declared in `irmin-blocksci.opam`, so `opam switch create . --deps-only` handles everything automatically.

## Architecture

### Library (`lib/`)

- `types.ml` - Data types (block, transaction, output, input, address) with JSON serialization
- `store.ml` - Irmin store configuration using `Irmin.Contents.String`, path helpers for hierarchical storage
- `import.ml` - CSV parsing for BlockSci export files (nodes + relationships)
- `query.ml` - Query functions: block traversal, transaction details, address balance, path finding

### Binary (`bin/`)

- `main.ml` - CLI using Cmdliner with `import` and `query` subcommands

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
```

### CSV Export Format (from Neo4j)

**Nodes:** `addresses.csv`, `blocks.csv`, `outputs.csv`, `transactions.csv`
**Relationships:** `contains.csv`, `to_address.csv`, `tx_input.csv`, `tx_output.csv`

Default store location: `/tmp/irmin-blocksci-store`
