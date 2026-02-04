(** Benchmark queries from BlockSci paper and queries.cypher.

    This application benchmarks typical blockchain analysis queries
    as described in the BlockSci paper, with their Cypher equivalents.

    Output format: CSV with columns Query,Time_ms,Result *)

open Blocksci

(** {1 Timing utilities} *)

let results = ref []

let time_it name f =
  let start = Unix.gettimeofday () in
  let result = f () in
  let elapsed = Unix.gettimeofday () -. start in
  let time_ms = elapsed *. 1000.0 in
  results := (name, time_ms, result) :: !results;
  result

(** {1 Basic Counts} *)

(** Block count.

    {v
    MATCH (b:Block)
    RETURN count(b) AS value;
    v} *)
let block_count store = Query.last_block_height store + 1

(** Tx count.

    {v
    MATCH (t:Transaction)
    RETURN count(t) AS value;
    v} *)
let tx_count store =
  let last_height = Query.last_block_height store in
  let count = ref 0 in
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    count := !count + List.length txs
  done;
  !count

(** Input count.

    {v
    MATCH ()-[r:TX_INPUT]->()
    RETURN count(r) AS value;
    v} *)
let input_count store =
  let last_height = Query.last_block_height store in
  let count = ref 0 in
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) ->
        let inputs = Query.tx_inputs store tx.tx_id in
        count := !count + List.length inputs)
      txs
  done;
  !count

(** Output count.

    {v
    MATCH (o:Output)
    RETURN count(o) AS value;
    v} *)
let output_count store =
  let last_height = Query.last_block_height store in
  let count = ref 0 in
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) ->
        let outputs = Query.tx_outputs store tx.tx_id in
        count := !count + List.length outputs)
      txs
  done;
  !count

(** Address count.

    {v
    MATCH (a:Address)
    RETURN count(a) AS value;
    v} *)
let address_count store =
  let keys = Store.list store [ "address" ] in
  List.length keys

(** {1 Paper Queries (Table 7)} *)

(** Tx locktime > 0.

    {v
    MATCH (t:Transaction)
    WHERE t.locktime > 0
    RETURN count(t) AS value;
    v} *)
let tx_locktime_gt_0 store =
  let last_height = Query.last_block_height store in
  let count = ref 0 in
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) ->
        if tx.tx_locktime > 0L then incr count)
      txs
  done;
  !count

(** Max output value.

    {v
    MATCH (o:Output)
    RETURN max(o.value) AS value;
    v} *)
let max_output_value store =
  let last_height = Query.last_block_height store in
  let max_val = ref 0L in
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) ->
        let outputs = Query.tx_outputs store tx.tx_id in
        List.iter
          (fun (o : Types.output) ->
            if Int64.compare o.out_value !max_val > 0 then
              max_val := o.out_value)
          outputs)
      txs
  done;
  !max_val

(** Calculate fee (max fee).

    {v
    MATCH (t:Transaction)
    RETURN max(t.fee) AS value;
    v} *)
let calculate_fee store =
  let last_height = Query.last_block_height store in
  let max_fee = ref 0L in
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) ->
        if Int64.compare tx.tx_fee !max_fee > 0 then max_fee := tx.tx_fee)
      txs
  done;
  !max_fee

(** {1 Additional Queries} *)

(** Total output value.

    {v
    MATCH (o:Output)
    RETURN sum(o.value) AS value;
    v} *)
let total_output_value store =
  let last_height = Query.last_block_height store in
  let total = ref 0L in
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) ->
        let outputs = Query.tx_outputs store tx.tx_id in
        List.iter
          (fun (o : Types.output) -> total := Int64.add !total o.out_value)
          outputs)
      txs
  done;
  !total

(** Total fees.

    {v
    MATCH (t:Transaction)
    RETURN sum(t.fee) AS value;
    v} *)
let total_fees store =
  let last_height = Query.last_block_height store in
  let total = ref 0L in
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) -> total := Int64.add !total tx.tx_fee)
      txs
  done;
  !total

(** Max input value.

    {v
    MATCH ()-[:TX_INPUT]->(o:Output)
    RETURN max(o.value) AS value;
    v} *)
let max_input_value store =
  let last_height = Query.last_block_height store in
  let max_val = ref 0L in
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) ->
        let inputs = Query.tx_inputs store tx.tx_id in
        List.iter
          (fun (inp : Types.input) ->
            match Query.get_output store inp.in_spent_tx_id inp.in_spent_vout with
            | Some o ->
                if Int64.compare o.out_value !max_val > 0 then
                  max_val := o.out_value
            | None -> ())
          inputs)
      txs
  done;
  !max_val

(** Tx version > 1.

    {v
    MATCH (t:Transaction)
    WHERE t.version > 1
    RETURN count(t) AS value;
    v} *)
let tx_version_gt_1 store =
  let last_height = Query.last_block_height store in
  let count = ref 0 in
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) ->
        if tx.tx_version > 1 then incr count)
      txs
  done;
  !count

(** {1 Block Statistics} *)

(** Avg tx per block.

    {v
    MATCH (b:Block)-[:CONTAINS]->(t:Transaction)
    WITH b, count(t) AS txCount
    RETURN avg(txCount) AS value;
    v} *)
let avg_tx_per_block store =
  let last_height = Query.last_block_height store in
  let total_tx = ref 0 in
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    total_tx := !total_tx + List.length txs
  done;
  if last_height < 0 then 0.0
  else float_of_int !total_tx /. float_of_int (last_height + 1)

(** Max tx per block.

    {v
    MATCH (b:Block)-[:CONTAINS]->(t:Transaction)
    WITH b, count(t) AS txCount
    RETURN max(txCount) AS value;
    v} *)
let max_tx_per_block store =
  let last_height = Query.last_block_height store in
  let max_tx = ref 0 in
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    let n = List.length txs in
    if n > !max_tx then max_tx := n
  done;
  !max_tx

(** {1 UTXO Queries} *)

(** Spent outputs.

    {v
    MATCH (o:Output)<-[:TX_INPUT]-()
    RETURN count(o) AS value;
    v} *)
let spent_outputs store =
  let last_height = Query.last_block_height store in
  let count = ref 0 in
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) ->
        let outputs = Query.tx_outputs store tx.tx_id in
        List.iter
          (fun (o : Types.output) ->
            if Query.is_output_spent store o.out_tx_id o.out_vout then incr count)
          outputs)
      txs
  done;
  !count

(** Unspent outputs (UTXOs).

    {v
    MATCH (o:Output)
    WHERE NOT (o)<-[:TX_INPUT]-()
    RETURN count(o) AS value;
    v} *)
let unspent_outputs store =
  let last_height = Query.last_block_height store in
  let count = ref 0 in
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) ->
        let outputs = Query.tx_outputs store tx.tx_id in
        List.iter
          (fun (o : Types.output) ->
            if not (Query.is_output_spent store o.out_tx_id o.out_vout) then
              incr count)
          outputs)
      txs
  done;
  !count

(** {1 Advanced Queries} *)

(** High value tx (fee > 10 BTC).

    {v
    MATCH (t:Transaction)
    WHERE t.fee > 1000000000
    RETURN count(t) AS value;
    v} *)
let high_value_tx store =
  let last_height = Query.last_block_height store in
  let count = ref 0 in
  let threshold = 1_000_000_000L in (* 10 BTC in satoshis *)
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) ->
        if Int64.compare tx.tx_fee threshold > 0 then incr count)
      txs
  done;
  !count

(** Multi-input tx (> 10 inputs).

    {v
    MATCH (t:Transaction)-[:TX_INPUT]->()
    WITH t, count( * ) AS inputs
    WHERE inputs > 10
    RETURN count(t) AS value;
    v} *)
let multi_input_tx store =
  let last_height = Query.last_block_height store in
  let count = ref 0 in
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) ->
        let inputs = Query.tx_inputs store tx.tx_id in
        if List.length inputs > 10 then incr count)
      txs
  done;
  !count

(** {1 Main benchmark runner} *)

let run_benchmarks store =
  results := [];

  (* Basic Counts *)
  let _ = time_it "Block count" (fun () -> Int64.of_int (block_count store)) in
  let _ = time_it "Tx count" (fun () -> Int64.of_int (tx_count store)) in
  let _ = time_it "Input count" (fun () -> Int64.of_int (input_count store)) in
  let _ = time_it "Output count" (fun () -> Int64.of_int (output_count store)) in
  let _ = time_it "Address count" (fun () -> Int64.of_int (address_count store)) in

  (* Paper Queries (Table 7) *)
  let _ = time_it "Tx locktime > 0" (fun () -> Int64.of_int (tx_locktime_gt_0 store)) in
  let _ = time_it "Max output value" (fun () -> max_output_value store) in
  let _ = time_it "Calculate fee" (fun () -> calculate_fee store) in

  (* Additional Queries *)
  let _ = time_it "Total output value" (fun () -> total_output_value store) in
  let _ = time_it "Total fees" (fun () -> total_fees store) in
  let _ = time_it "Max input value" (fun () -> max_input_value store) in
  let _ = time_it "Tx version > 1" (fun () -> Int64.of_int (tx_version_gt_1 store)) in

  (* Block Statistics *)
  let _ = time_it "Avg tx per block" (fun () -> Int64.of_float (avg_tx_per_block store *. 1000.0)) in
  let _ = time_it "Max tx per block" (fun () -> Int64.of_int (max_tx_per_block store)) in

  (* UTXO Queries *)
  let _ = time_it "Spent outputs" (fun () -> Int64.of_int (spent_outputs store)) in
  let _ = time_it "Unspent outputs" (fun () -> Int64.of_int (unspent_outputs store)) in

  (* Advanced Queries *)
  let _ = time_it "High value tx" (fun () -> Int64.of_int (high_value_tx store)) in
  let _ = time_it "Multi-input tx" (fun () -> Int64.of_int (multi_input_tx store)) in

  ()

let output_csv () =
  Printf.printf "Query,Time_ms,Result\n";
  List.iter
    (fun (name, time_ms, result) ->
      Printf.printf "%s,%.3f,%Ld\n" name time_ms result)
    (List.rev !results)

(** {1 CLI} *)

let default_store = "/tmp/irmin-blocksci-store"

let run_with_store ~sw ~fs store_path =
  let root = Eio.Path.(fs / store_path) in
  let repo = Store.init ~sw ~fs root in
  let main = Store.main repo in
  Fun.protect
    ~finally:(fun () -> Store.Store.Repo.close repo)
    (fun () ->
      run_benchmarks main;
      output_csv ())

let () =
  let store_path =
    if Array.length Sys.argv > 1 then Sys.argv.(1) else default_store
  in
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let fs = Eio.Stdenv.fs env in
  run_with_store ~sw ~fs store_path
