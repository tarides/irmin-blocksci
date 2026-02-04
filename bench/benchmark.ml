(** Benchmark queries from BlockSci paper Table 7.

    This application benchmarks typical blockchain analysis queries
    as described in the BlockSci paper, with their Cypher equivalents. *)

open Blocksci

(** {1 Timing utilities} *)

let time_it name f =
  let start = Unix.gettimeofday () in
  let result = f () in
  let elapsed = Unix.gettimeofday () -. start in
  Printf.printf "  %s: %.3f ms\n%!" name (elapsed *. 1000.0);
  result

(** {1 Benchmark queries from Table 7} *)

(** Q1: Count transactions with locktime > 0.

    {v
    MATCH (t:Transaction)
    WHERE t.locktime > 0
    RETURN count(t) AS value;
    v} *)
let q1_locktime_nonzero store =
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

(** Q2: Find maximum output value.

    {v
    MATCH (o:Output)
    RETURN max(o.value) AS value;
    v} *)
let q2_max_output_value store =
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

(** Q3: Find maximum transaction fee.

    {v
    MATCH (t:Transaction)
    RETURN max(t.fee) AS value;
    v} *)
let q3_max_fee store =
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

(** Q4: Sum of outputs to a specific address (e.g., Satoshi Dice).

    {v
    MATCH (a:Address)<-[:TO_ADDRESS]-(o:Output)
    WHERE id(a) = ADDRESS_ID
    RETURN sum(o.value) AS value;
    v} *)
let q4_address_total_received store address_id =
  let outputs = Query.address_outputs store address_id in
  List.fold_left
    (fun acc (o : Types.output) -> Int64.add acc o.out_value)
    0L outputs

(** Q5: Count zero-conf outputs (spent in same block as created).

    {v
    MATCH (t1:Transaction)-[:TX_OUTPUT]->(o:Output)<-[:TX_INPUT]-(t2:Transaction)
    WHERE t1.blockHeight = t2.blockHeight
    RETURN count(o) AS value;
    v} *)
let q5_zero_conf_outputs store =
  let last_height = Query.last_block_height store in
  let count = ref 0 in
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) ->
        let inputs = Query.tx_inputs store tx.tx_id in
        List.iter
          (fun (inp : Types.input) ->
            (* Check if the output being spent was created in the same block *)
            match Query.get_transaction store inp.in_spent_tx_id with
            | Some prev_tx when prev_tx.tx_block_height = tx.tx_block_height ->
                incr count
            | _ -> ())
          inputs)
      txs
  done;
  !count

(** Q6: Locktime change heuristic - count transactions where locktime
    behavior is preserved across exactly one input.

    {v
    MATCH (t1:Transaction)-[:TX_OUTPUT]->(o:Output)<-[:TX_INPUT]-(t2:Transaction)
    WHERE (t1.locktime > 0) = (t2.locktime > 0)
    WITH t1, count(o) AS matching_count
    WHERE matching_count = 1
    RETURN count(t1) AS value;
    v}

    This identifies transactions that maintain locktime consistency
    with exactly one of their funding transactions. *)
let q6_locktime_change store =
  let last_height = Query.last_block_height store in
  (* Track transactions and their matching input count *)
  let tx_matching_counts = Hashtbl.create 1000 in
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) ->
        let tx_has_locktime = tx.tx_locktime > 0L in
        let inputs = Query.tx_inputs store tx.tx_id in
        List.iter
          (fun (inp : Types.input) ->
            match Query.get_transaction store inp.in_spent_tx_id with
            | Some prev_tx ->
                let prev_has_locktime = prev_tx.tx_locktime > 0L in
                if tx_has_locktime = prev_has_locktime then begin
                  let current =
                    match Hashtbl.find_opt tx_matching_counts prev_tx.tx_id with
                    | Some n -> n
                    | None -> 0
                  in
                  Hashtbl.replace tx_matching_counts prev_tx.tx_id (current + 1)
                end
            | None -> ())
          inputs)
      txs
  done;
  (* Count transactions with exactly 1 matching output *)
  Hashtbl.fold
    (fun _ count acc -> if count = 1 then acc + 1 else acc)
    tx_matching_counts 0

(** {1 Main benchmark runner} *)

let run_benchmarks store =
  let last_height = Query.last_block_height store in
  Printf.printf "Store has %d blocks\n\n" (last_height + 1);

  Printf.printf "=== BlockSci Paper Table 7 Benchmarks ===\n\n";

  let count =
    time_it "Q1: Tx locktime > 0" (fun () -> q1_locktime_nonzero store)
  in
  Printf.printf "     Result: %d transactions\n\n" count;

  let max_output =
    time_it "Q2: Max output value" (fun () -> q2_max_output_value store)
  in
  Printf.printf "     Result: %Ld satoshis (%.2f BTC)\n\n" max_output
    (Query.satoshis_to_btc max_output);

  let max_fee = time_it "Q3: Max fee" (fun () -> q3_max_fee store) in
  Printf.printf "     Result: %Ld satoshis (%.4f BTC)\n\n" max_fee
    (Query.satoshis_to_btc max_fee);

  (* Q4: Address query - use address "0" as example if it exists *)
  let addr_total =
    time_it "Q4: Address total received (addr 0)" (fun () ->
        q4_address_total_received store "0")
  in
  Printf.printf "     Result: %Ld satoshis (%.2f BTC)\n\n" addr_total
    (Query.satoshis_to_btc addr_total);

  let zero_conf =
    time_it "Q5: Zero-conf outputs" (fun () -> q5_zero_conf_outputs store)
  in
  Printf.printf "     Result: %d outputs\n\n" zero_conf;

  let locktime_change =
    time_it "Q6: Locktime change heuristic" (fun () -> q6_locktime_change store)
  in
  Printf.printf "     Result: %d transactions\n\n" locktime_change;

  Printf.printf "Benchmark complete.\n"

(** {1 CLI} *)

let default_store = "/tmp/irmin-blocksci-store"

let run_with_store ~sw ~fs store_path =
  let root = Eio.Path.(fs / store_path) in
  let repo = Store.init ~sw ~fs root in
  let main = Store.main repo in
  Fun.protect
    ~finally:(fun () -> Store.Store.Repo.close repo)
    (fun () -> run_benchmarks main)

let () =
  let store_path =
    if Array.length Sys.argv > 1 then Sys.argv.(1) else default_store
  in
  Printf.printf "BlockSci Benchmark Suite (Table 7)\n";
  Printf.printf "==================================\n";
  Printf.printf "Store: %s\n\n" store_path;
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let fs = Eio.Stdenv.fs env in
  run_with_store ~sw ~fs store_path
