(** Benchmark queries from BlockSci paper and queries.cypher.

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

(** {1 Paper Queries (Table 7)} *)

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
            match Query.get_transaction store inp.in_spent_tx_id with
            | Some prev_tx when prev_tx.tx_block_height = tx.tx_block_height ->
                incr count
            | _ -> ())
          inputs)
      txs
  done;
  !count

(** Q6: Locktime change heuristic.

    {v
    MATCH (t1:Transaction)-[:TX_OUTPUT]->(o:Output)<-[:TX_INPUT]-(t2:Transaction)
    WHERE (t1.locktime > 0) = (t2.locktime > 0)
    WITH t1, count(o) AS matching_count
    WHERE matching_count = 1
    RETURN count(t1) AS value;
    v} *)
let q6_locktime_change store =
  let last_height = Query.last_block_height store in
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
  Hashtbl.fold
    (fun _ count acc -> if count = 1 then acc + 1 else acc)
    tx_matching_counts 0

(** {1 Additional Queries (not in Table 7)} *)

(** Sum of all output values.

    {v
    MATCH (o:Output)
    RETURN sum(o.value) AS value;
    v} *)
let sum_output_value store =
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

(** Sum of all transaction fees.

    {v
    MATCH (t:Transaction)
    RETURN sum(t.fee) AS value;
    v} *)
let sum_fees store =
  let last_height = Query.last_block_height store in
  let total = ref 0L in
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) -> total := Int64.add !total tx.tx_fee)
      txs
  done;
  !total

(** Find maximum value of any spent output.

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

(** Count transactions with version > 1.

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

(** Count total number of inputs.

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

(** Count total number of outputs.

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

(** {1 Basic Counts} *)

(** Count blocks.

    {v
    MATCH (b:Block)
    RETURN count(b) AS value;
    v} *)
let block_count store = Query.last_block_height store + 1

(** Count transactions.

    {v
    MATCH (t:Transaction)
    RETURN count(t) AS value;
    v} *)
let transaction_count store =
  let last_height = Query.last_block_height store in
  let count = ref 0 in
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    count := !count + List.length txs
  done;
  !count

(** Count spent outputs.

    {v
    MATCH (o:Output)<-[:TX_INPUT]-()
    RETURN count(o) AS value;
    v} *)
let spent_output_count store =
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

(** Count unspent outputs (UTXOs).

    {v
    MATCH (o:Output)
    WHERE NOT (o)<-[:TX_INPUT]-()
    RETURN count(o) AS value;
    v} *)
let utxo_count store =
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

(** {1 Block Statistics} *)

(** Average transactions per block.

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

(** Maximum transactions in a single block.

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

(** {1 Advanced Queries} *)

(** Count transactions with fee > 10 BTC.

    {v
    MATCH (t:Transaction)
    WHERE t.fee > 1000000000
    RETURN count(t) AS value;
    v} *)
let high_fee_tx_count store =
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

(** Count transactions with more than 10 inputs.

    {v
    MATCH (t:Transaction)-[:TX_INPUT]->()
    WITH t, count( * ) AS inputs
    WHERE inputs > 10
    RETURN count(t) AS value;
    v} *)
let multi_input_tx_count store =
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

(** {1 Verification Queries} *)

(** Get genesis block info.

    {v
    MATCH (b:Block {height: 0})
    RETURN b.hash AS genesis_hash, b.timestamp AS genesis_timestamp;
    v} *)
let genesis_block store = Query.get_block store 0

(** Count transactions in block 170 (first real transaction).

    {v
    MATCH (b:Block {height: 170})-[:CONTAINS]->(t:Transaction)
    RETURN count(t) AS tx_count;
    v} *)
let block_170_tx_count store =
  let txs = Query.block_transactions store 170 in
  List.length txs

(** Script type distribution.

    {v
    MATCH (o:Output)
    RETURN o.scriptType AS script_type, count(o) AS count
    ORDER BY count DESC;
    v} *)
let script_type_distribution store =
  let last_height = Query.last_block_height store in
  let counts = Hashtbl.create 10 in
  for height = 0 to last_height do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) ->
        let outputs = Query.tx_outputs store tx.tx_id in
        List.iter
          (fun (o : Types.output) ->
            let current =
              match Hashtbl.find_opt counts o.out_script_type with
              | Some n -> n
              | None -> 0
            in
            Hashtbl.replace counts o.out_script_type (current + 1))
          outputs)
      txs
  done;
  let items = Hashtbl.fold (fun k v acc -> (k, v) :: acc) counts [] in
  List.sort (fun (_, a) (_, b) -> compare b a) items

(** {1 Main benchmark runner} *)

let run_benchmarks store =
  let last_height = Query.last_block_height store in
  Printf.printf "Store has %d blocks\n\n" (last_height + 1);

  (* Paper Queries (Table 7) *)
  Printf.printf "=== Paper Queries (Table 7) ===\n\n";

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

  (* Additional Queries *)
  Printf.printf "=== Additional Queries ===\n\n";

  let sum_out =
    time_it "Sum output value" (fun () -> sum_output_value store)
  in
  Printf.printf "     Result: %Ld satoshis (%.2f BTC)\n\n" sum_out
    (Query.satoshis_to_btc sum_out);

  let sum_f = time_it "Sum fees" (fun () -> sum_fees store) in
  Printf.printf "     Result: %Ld satoshis (%.4f BTC)\n\n" sum_f
    (Query.satoshis_to_btc sum_f);

  let max_inp =
    time_it "Max input value" (fun () -> max_input_value store)
  in
  Printf.printf "     Result: %Ld satoshis (%.2f BTC)\n\n" max_inp
    (Query.satoshis_to_btc max_inp);

  let ver_gt_1 =
    time_it "Tx version > 1" (fun () -> tx_version_gt_1 store)
  in
  Printf.printf "     Result: %d transactions\n\n" ver_gt_1;

  let inp_cnt = time_it "Input count" (fun () -> input_count store) in
  Printf.printf "     Result: %d inputs\n\n" inp_cnt;

  let out_cnt = time_it "Output count" (fun () -> output_count store) in
  Printf.printf "     Result: %d outputs\n\n" out_cnt;

  (* Basic Counts *)
  Printf.printf "=== Basic Counts ===\n\n";

  let blk_cnt = time_it "Block count" (fun () -> block_count store) in
  Printf.printf "     Result: %d blocks\n\n" blk_cnt;

  let tx_cnt = time_it "Transaction count" (fun () -> transaction_count store) in
  Printf.printf "     Result: %d transactions\n\n" tx_cnt;

  let spent_cnt =
    time_it "Spent output count" (fun () -> spent_output_count store)
  in
  Printf.printf "     Result: %d spent outputs\n\n" spent_cnt;

  let utxo_cnt = time_it "UTXO count" (fun () -> utxo_count store) in
  Printf.printf "     Result: %d UTXOs\n\n" utxo_cnt;

  (* Block Statistics *)
  Printf.printf "=== Block Statistics ===\n\n";

  let avg_tx = time_it "Avg tx per block" (fun () -> avg_tx_per_block store) in
  Printf.printf "     Result: %.2f transactions\n\n" avg_tx;

  let max_tx = time_it "Max tx per block" (fun () -> max_tx_per_block store) in
  Printf.printf "     Result: %d transactions\n\n" max_tx;

  (* Advanced Queries *)
  Printf.printf "=== Advanced Queries ===\n\n";

  let high_fee =
    time_it "High-fee tx (>10 BTC)" (fun () -> high_fee_tx_count store)
  in
  Printf.printf "     Result: %d transactions\n\n" high_fee;

  let multi_inp =
    time_it "Multi-input tx (>10 inputs)" (fun () -> multi_input_tx_count store)
  in
  Printf.printf "     Result: %d transactions\n\n" multi_inp;

  (* Verification Queries *)
  Printf.printf "=== Verification Queries ===\n\n";

  let genesis =
    time_it "Genesis block" (fun () -> genesis_block store)
  in
  (match genesis with
   | Some b ->
       Printf.printf "     Hash: %s\n" b.Types.hash;
       Printf.printf "     Timestamp: %Ld\n\n" b.Types.timestamp
   | None -> Printf.printf "     Not found\n\n");

  let b170_tx =
    time_it "Block 170 tx count" (fun () -> block_170_tx_count store)
  in
  Printf.printf "     Result: %d transactions\n\n" b170_tx;

  let script_dist =
    time_it "Script type distribution" (fun () -> script_type_distribution store)
  in
  Printf.printf "     Result:\n";
  List.iter
    (fun (script_type, count) ->
      Printf.printf "       %s: %d\n" script_type count)
    script_dist;

  Printf.printf "\nBenchmark complete.\n"

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
  Printf.printf "BlockSci Benchmark Suite\n";
  Printf.printf "========================\n";
  Printf.printf "Store: %s\n\n" store_path;
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let fs = Eio.Stdenv.fs env in
  run_with_store ~sw ~fs store_path
