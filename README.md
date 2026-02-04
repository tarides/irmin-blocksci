# irmin-blocksci

Store Bitcoin transaction graph data from [BlockSci](https://github.com/citp/BlockSci) CSV exports into [Irmin](https://irmin.org).

This project provides an OCaml-based blockchain analysis platform that imports BlockSci's Neo4j CSV exports into Irmin's content-addressable storage, enabling efficient queries on Bitcoin transaction graphs.

## Features

- **Import** BlockSci CSV exports (nodes and relationships)
- **Query** blocks, transactions, outputs, and addresses via CLI
- **GraphQL API** for programmatic access
- **Benchmark suite** implementing queries from the [BlockSci paper](https://www.usenix.org/conference/usenixsecurity20/presentation/kalodner) (Table 7)
- **C bindings** example using libirmin

## Installation

Requires OCaml 5.1+ and opam.

```bash
# Clone the repository
git clone https://github.com/tarides/irmin-blocksci.git
cd irmin-blocksci

# Create local switch with all dependencies
make switch
# or: opam switch create . --deps-only

# Build
make build
```

## Usage

### Import BlockSci CSV Export

```bash
# Import from CSV directory (supports incremental import)
dune exec irmin-blocksci -- import <csv-export-dir>
```

The CSV export should contain:
- **Nodes:** `addresses.csv`, `blocks.csv`, `outputs.csv`, `transactions.csv`
- **Relationships:** `contains.csv`, `to_address.csv`, `tx_input.csv`, `tx_output.csv`

Default store location: `/tmp/irmin-blocksci-store`

### Query Commands

```bash
# Query block by height
dune exec irmin-blocksci -- query block <height>

# Query transaction by ID
dune exec irmin-blocksci -- query tx <tx_id>

# Query output by tx_id:vout
dune exec irmin-blocksci -- query output <tx_id:vout>

# Query address balance
dune exec irmin-blocksci -- query balance <address_id>

# Query block range
dune exec irmin-blocksci -- query chain <start> -n <count>

# Show store info
dune exec irmin-blocksci -- query info
```

### GraphQL Server

```bash
# Start server on default port 8080
dune exec irmin-blocksci -- serve

# Start on custom port
dune exec irmin-blocksci -- serve -p 3000
```

- GraphiQL UI: http://localhost:8080/graphiql
- API endpoint: http://localhost:8080/graphql

Example query:
```graphql
{
  block(height: 0) {
    height
    hash
    timestamp
  }
  transaction(txId: "1") {
    txId
    fee
    outputs {
      value
      scriptType
    }
  }
}
```

### Benchmark

Run blockchain analysis queries from the BlockSci paper:

```bash
dune exec irmin-blocksci-bench -- /tmp/irmin-blocksci-store
```

Output (CSV format):
```
Query,Time_ms,Result
Block count,4304.813,274195
Tx count,6664.694,101558
...
```

See `queries.cypher` for the equivalent Cypher queries.

## Store Schema

```
/block/<height>                      -> Block JSON
/tx/<tx_id>                          -> Transaction JSON
/output/<tx_id>/<vout>               -> Output JSON
/address/<address_id>                -> Address JSON
/index/block_txs/<height>/<idx>      -> TxRef
/index/tx_inputs/<tx_id>/<idx>       -> Input JSON
/index/tx_outputs/<tx_id>/<vout>     -> OutputRef
/index/addr_outputs/<addr>/<ref>     -> OutputRef
/index/output_addr/<tx_id>/<vout>    -> AddrRef
/index/spent_by/<tx_id>/<vout>       -> TxRef
```

## Architecture

- `lib/` - Core library
  - `types.ml` - Data types with JSON serialization
  - `store.ml` - Irmin store configuration
  - `import.ml` - CSV parsing and import
  - `query.ml` - Query functions with Cypher equivalents in odoc
  - `graphql_server.ml` - GraphQL API
- `bin/` - CLI application
- `bench/` - Benchmark suite
- `c_bin/` - C bindings example

## Dependencies

Uses the `eio` branch of Irmin for concurrent I/O. All pin-depends are declared in `irmin-blocksci.opam`.

## References

- [BlockSci: Design and applications of a blockchain analysis platform](https://www.usenix.org/conference/usenixsecurity20/presentation/kalodner) (USENIX Security 2020)
- [Irmin](https://irmin.org) - Git-like distributed database

## License

ISC
