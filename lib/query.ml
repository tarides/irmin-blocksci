(** Query functions for the BlockSci graph stored in Irmin.

    This module provides OCaml equivalents of Cypher graph queries
    for exploring Bitcoin blockchain data. *)

open Types

(** Get a block by height.

    {v
    MATCH (b:Block {height: $height})
    RETURN b
    v} *)
let get_block store height =
  match Store.get store (Store.block_path height) with
  | Some (Block b) -> Some b
  | _ -> None

(** Get a transaction by ID.

    {v
    MATCH (t:Transaction {txId: $tx_id})
    RETURN t
    v} *)
let get_transaction store tx_id =
  match Store.get store (Store.tx_path tx_id) with
  | Some (Transaction tx) -> Some tx
  | _ -> None

(** Get an output by transaction ID and output index.

    {v
    MATCH (o:Output {txId: $tx_id, vout: $vout})
    RETURN o
    v} *)
let get_output store tx_id vout =
  match Store.get store (Store.output_path tx_id vout) with
  | Some (Output o) -> Some o
  | _ -> None

(** Get an address by its ID.

    {v
    MATCH (a:Address {addressId: $addr})
    RETURN a
    v} *)
let get_address store addr =
  match Store.get store (Store.address_path addr) with
  | Some (Address a) -> Some a
  | _ -> None

(** Get all transactions in a block.

    {v
    MATCH (b:Block {height: $height})-[:CONTAINS]->(t:Transaction)
    RETURN t
    ORDER BY t.txId
    v} *)
let block_transactions store height =
  let keys = Store.list store (Store.block_txs_path height) in
  List.filter_map
    (fun key ->
      match Store.get store (Store.block_txs_path height @ [ key ]) with
      | Some (TxRef tx_id) -> get_transaction store tx_id
      | _ -> None)
    keys

(** Get all inputs for a transaction.

    {v
    MATCH (t:Transaction {txId: $tx_id})-[i:TX_INPUT]->(o:Output)
    RETURN i, o
    ORDER BY i.index
    v} *)
let tx_inputs store tx_id =
  let keys = Store.list store (Store.tx_inputs_path tx_id) in
  List.filter_map
    (fun key ->
      match Store.get store (Store.tx_inputs_path tx_id @ [ key ]) with
      | Some (Input i) -> Some i
      | _ -> None)
    keys

(** Get all outputs for a transaction.

    {v
    MATCH (t:Transaction {txId: $tx_id})-[:TX_OUTPUT]->(o:Output)
    RETURN o
    ORDER BY o.vout
    v} *)
let tx_outputs store tx_id =
  let keys = Store.list store (Store.tx_outputs_path tx_id) in
  List.filter_map
    (fun key ->
      match Store.get store (Store.tx_outputs_path tx_id @ [ key ]) with
      | Some (OutputRef r) -> get_output store r.ref_tx_id r.ref_vout
      | _ -> None)
    keys

(** Get the address an output is locked to.

    {v
    MATCH (o:Output {txId: $tx_id, vout: $vout})-[:TO_ADDRESS]->(a:Address)
    RETURN a
    v} *)
let output_address store tx_id vout =
  match Store.get store (Store.output_addr_path tx_id vout) with
  | Some (AddrRef addr) -> Some addr
  | _ -> None

(** Get all outputs locked to an address.

    {v
    MATCH (a:Address {addressId: $addr})<-[:TO_ADDRESS]-(o:Output)
    RETURN o
    v} *)
let address_outputs store addr =
  let keys = Store.list store (Store.addr_outputs_path addr) in
  List.filter_map
    (fun key ->
      match Store.get store (Store.addr_outputs_path addr @ [ key ]) with
      | Some (OutputRef r) -> get_output store r.ref_tx_id r.ref_vout
      | _ -> None)
    keys

(** Get the transaction that spends an output.

    {v
    MATCH (o:Output {txId: $tx_id, vout: $vout})<-[:TX_INPUT]-(t:Transaction)
    RETURN t
    v} *)
let output_spent_by store tx_id vout =
  match Store.get store (Store.spent_by_path tx_id vout) with
  | Some (TxRef spending_tx_id) -> Some spending_tx_id
  | _ -> None

(** Check if an output has been spent.

    {v
    MATCH (o:Output {txId: $tx_id, vout: $vout})
    RETURN EXISTS { (o)<-[:TX_INPUT]-(:Transaction) } AS spent
    v} *)
let is_output_spent store tx_id vout =
  Option.is_some (output_spent_by store tx_id vout)

(** Calculate the balance of an address (sum of unspent outputs).

    {v
    MATCH (a:Address {addressId: $addr})<-[:TO_ADDRESS]-(o:Output)
    WHERE NOT EXISTS { (o)<-[:TX_INPUT]-(:Transaction) }
    RETURN sum(o.value) AS balance
    v} *)
let address_balance store addr =
  let outputs = address_outputs store addr in
  List.fold_left
    (fun acc (o : output) ->
      if is_output_spent store o.out_tx_id o.out_vout then acc
      else Int64.add acc o.out_value)
    0L outputs

(** Get a range of blocks from start_height to start_height + count - 1.

    {v
    MATCH (b:Block)
    WHERE b.height >= $start_height AND b.height < $start_height + $count
    RETURN b
    ORDER BY b.height
    v} *)
let block_chain store start_height count =
  let rec collect acc height remaining =
    if remaining <= 0 then List.rev acc
    else
      match get_block store height with
      | Some block -> collect (block :: acc) (height + 1) (remaining - 1)
      | None -> List.rev acc
  in
  collect [] start_height count

(** Get a block with its coinbase transaction (first transaction in block).

    {v
    MATCH (b:Block {height: $height})-[:CONTAINS]->(t:Transaction)
    WITH b, t ORDER BY t.txId LIMIT 1
    RETURN b, t AS coinbase
    v} *)
let block_with_coinbase store height =
  match get_block store height with
  | None -> None
  | Some block -> (
      let txs = block_transactions store height in
      match txs with
      | [] -> Some (block, None)
      | coinbase :: _ -> Some (block, Some coinbase))

(** Get full transaction details including inputs, outputs, addresses, and block.

    {v
    MATCH (b:Block)-[:CONTAINS]->(t:Transaction {txId: $tx_id})
    OPTIONAL MATCH (t)-[i:TX_INPUT]->(spent:Output)-[:TO_ADDRESS]->(inAddr:Address)
    OPTIONAL MATCH (t)-[:TX_OUTPUT]->(out:Output)-[:TO_ADDRESS]->(outAddr:Address)
    RETURN t, b,
           collect(DISTINCT {input: i, spent: spent, addr: inAddr}) AS inputs,
           collect(DISTINCT {output: out, addr: outAddr}) AS outputs
    v} *)
let tx_details store tx_id =
  match get_transaction store tx_id with
  | None -> None
  | Some tx ->
      let inputs = tx_inputs store tx_id in
      let outputs = tx_outputs store tx_id in
      let input_details =
        List.map
          (fun (inp : input) ->
            let spent_output =
              get_output store inp.in_spent_tx_id inp.in_spent_vout
            in
            let addr = output_address store inp.in_spent_tx_id inp.in_spent_vout in
            (inp, spent_output, addr))
          inputs
      in
      let output_details =
        List.map
          (fun (o : output) ->
            let addr = output_address store o.out_tx_id o.out_vout in
            (o, addr))
          outputs
      in
      let block = get_block store tx.tx_block_height in
      Some (tx, input_details, output_details, block)

(** Path finding algorithms for tracing value flow through the transaction graph. *)
module PathFinder = struct
  (** Element in a path through the transaction graph. *)
  type path_element =
    | OutputNode of output
    | TxNode of transaction
    | AddressNode of address

  (** Find a path between two outputs by following spending relationships.

      Uses BFS to find the shortest path from one output to another,
      following TX_INPUT relationships (spending) through transactions.

      {v
      MATCH path = (from:Output {txId: $from_tx, vout: $from_vout})
                   <-[:TX_INPUT*1..$max_depth]-
                   (to:Output {txId: $to_tx, vout: $to_vout})
      RETURN path
      LIMIT 1
      v} *)
  let find_path_between_outputs store ~from_tx ~from_vout ~to_tx ~to_vout
      ~max_depth =
    let visited = Hashtbl.create 100 in
    let rec bfs queue depth =
      if depth > max_depth || Queue.is_empty queue then None
      else
        let (current_tx, current_vout), path = Queue.pop queue in
        let key = (current_tx, current_vout) in
        if Hashtbl.mem visited key then bfs queue depth
        else begin
          Hashtbl.add visited key true;
          if current_tx = to_tx && current_vout = to_vout then
            Some (List.rev path)
          else begin
            (match output_spent_by store current_tx current_vout with
            | None -> ()
            | Some spending_tx_id -> (
                match get_transaction store spending_tx_id with
                | None -> ()
                | Some spending_tx ->
                    let outputs = tx_outputs store spending_tx_id in
                    List.iter
                      (fun (out : output) ->
                        let new_path =
                          OutputNode out :: TxNode spending_tx :: path
                        in
                        Queue.push
                          ((out.out_tx_id, out.out_vout), new_path)
                          queue)
                      outputs));
            bfs queue (depth + 1)
          end
        end
    in
    match get_output store from_tx from_vout with
    | None -> None
    | Some start_output ->
        let queue = Queue.create () in
        Queue.push ((from_tx, from_vout), [ OutputNode start_output ]) queue;
        bfs queue 0

  (** Find a path between two addresses through the transaction graph.

      Finds outputs belonging to each address, then searches for a path
      between any pair of outputs using {!find_path_between_outputs}.

      {v
      MATCH (fromAddr:Address {addressId: $from_addr})<-[:TO_ADDRESS]-(fromOut:Output),
            (toAddr:Address {addressId: $to_addr})<-[:TO_ADDRESS]-(toOut:Output),
            path = (fromOut)<-[:TX_INPUT*1..$max_depth]-(toOut)
      RETURN path
      LIMIT 1
      v} *)
  let find_path_between_addresses store ~from_addr ~to_addr ~max_depth =
    let from_outputs = address_outputs store from_addr in
    let to_outputs = address_outputs store to_addr in
    let to_set =
      List.fold_left
        (fun acc (o : output) ->
          Hashtbl.add acc (o.out_tx_id, o.out_vout) true;
          acc)
        (Hashtbl.create 10)
        to_outputs
    in
    let rec try_paths = function
      | [] -> None
      | (from_output : output) :: rest -> (
          let results =
            List.filter_map
              (fun (to_output : output) ->
                find_path_between_outputs store ~from_tx:from_output.out_tx_id
                  ~from_vout:from_output.out_vout ~to_tx:to_output.out_tx_id
                  ~to_vout:to_output.out_vout ~max_depth)
              to_outputs
          in
          match results with [] -> try_paths rest | path :: _ -> Some path)
    in
    ignore to_set;
    try_paths from_outputs
end

(** {1 Pretty printing} *)

(** Print block details to stdout. *)
let print_block (block : block) =
  Printf.printf "Block %d:\n" block.height;
  Printf.printf "  Hash: %s\n" block.hash;
  Printf.printf "  Timestamp: %Ld\n" block.timestamp;
  Printf.printf "  Nonce: %Ld\n" block.nonce;
  Printf.printf "  Bits: %Ld\n" block.bits;
  Printf.printf "  Version: %d\n" block.version

(** Print transaction details to stdout. *)
let print_transaction (tx : transaction) =
  Printf.printf "Transaction %d:\n" tx.tx_id;
  Printf.printf "  Hash: %s\n" tx.tx_hash;
  Printf.printf "  Block height: %d\n" tx.tx_block_height;
  Printf.printf "  Fee: %Ld satoshis\n" tx.tx_fee;
  Printf.printf "  Size: %d bytes\n" tx.tx_size;
  Printf.printf "  Weight: %d\n" tx.tx_weight;
  Printf.printf "  Locktime: %Ld\n" tx.tx_locktime;
  Printf.printf "  Version: %d\n" tx.tx_version

(** Print output details to stdout. *)
let print_output (o : output) =
  Printf.printf "  Output %d:%d - %Ld satoshis (%s)\n" o.out_tx_id o.out_vout
    o.out_value o.out_script_type

(** Print address details to stdout. *)
let print_address (addr : address) =
  Printf.printf "Address: %s (type: %s)\n" addr.addr_str addr.addr_type

(** {1 Utilities} *)

(** Convert satoshis to BTC (1 BTC = 100,000,000 satoshis). *)
let satoshis_to_btc satoshis = Int64.to_float satoshis /. 100_000_000.0
