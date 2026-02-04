(** Benchmark queries from BlockSci paper.

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

(** {1 Benchmark queries} *)

(** Q1: Count all transactions in a block range.

    {v
    MATCH (b:Block)-[:CONTAINS]->(t:Transaction)
    WHERE b.height >= $start AND b.height < $end
    RETURN count(t)
    v} *)
let q1_count_transactions store ~start_height ~end_height =
  let count = ref 0 in
  for height = start_height to end_height - 1 do
    let txs = Query.block_transactions store height in
    count := !count + List.length txs
  done;
  !count

(** Q2: Sum of all output values in a block range.

    {v
    MATCH (b:Block)-[:CONTAINS]->(t:Transaction)-[:TX_OUTPUT]->(o:Output)
    WHERE b.height >= $start AND b.height < $end
    RETURN sum(o.value)
    v} *)
let q2_total_output_value store ~start_height ~end_height =
  let total = ref 0L in
  for height = start_height to end_height - 1 do
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

(** Q3: Count transactions with more than N inputs (potential clustering candidates).

    {v
    MATCH (t:Transaction)-[i:TX_INPUT]->(:Output)
    WITH t, count(i) AS input_count
    WHERE input_count > $threshold
    RETURN count(t)
    v}

    Multi-input transactions are used for address clustering heuristics. *)
let q3_multi_input_transactions store ~start_height ~end_height ~threshold =
  let count = ref 0 in
  for height = start_height to end_height - 1 do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) ->
        let inputs = Query.tx_inputs store tx.tx_id in
        if List.length inputs > threshold then incr count)
      txs
  done;
  !count

(** Q4: Total fees collected in a block range.

    {v
    MATCH (b:Block)-[:CONTAINS]->(t:Transaction)
    WHERE b.height >= $start AND b.height < $end
    RETURN sum(t.fee)
    v} *)
let q4_total_fees store ~start_height ~end_height =
  let total = ref 0L in
  for height = start_height to end_height - 1 do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) -> total := Int64.add !total tx.tx_fee)
      txs
  done;
  !total

(** Q5: Find all unspent outputs (UTXOs) in a block range.

    {v
    MATCH (b:Block)-[:CONTAINS]->(t:Transaction)-[:TX_OUTPUT]->(o:Output)
    WHERE b.height >= $start AND b.height < $end
      AND NOT EXISTS { (o)<-[:TX_INPUT]-(:Transaction) }
    RETURN count(o), sum(o.value)
    v} *)
let q5_utxo_stats store ~start_height ~end_height =
  let count = ref 0 in
  let total_value = ref 0L in
  for height = start_height to end_height - 1 do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) ->
        let outputs = Query.tx_outputs store tx.tx_id in
        List.iter
          (fun (o : Types.output) ->
            if not (Query.is_output_spent store o.out_tx_id o.out_vout) then begin
              incr count;
              total_value := Int64.add !total_value o.out_value
            end)
          outputs)
      txs
  done;
  (!count, !total_value)

(** Q6: Average transaction size in a block range.

    {v
    MATCH (b:Block)-[:CONTAINS]->(t:Transaction)
    WHERE b.height >= $start AND b.height < $end
    RETURN avg(t.size)
    v} *)
let q6_avg_tx_size store ~start_height ~end_height =
  let total_size = ref 0 in
  let count = ref 0 in
  for height = start_height to end_height - 1 do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) ->
        total_size := !total_size + tx.tx_size;
        incr count)
      txs
  done;
  if !count = 0 then 0.0 else float_of_int !total_size /. float_of_int !count

(** Q7: Find coinbase rewards in a block range.

    {v
    MATCH (b:Block)-[:CONTAINS]->(t:Transaction)
    WHERE NOT EXISTS { (t)-[:TX_INPUT]->(:Output) }
    WITH b, t
    MATCH (t)-[:TX_OUTPUT]->(o:Output)
    RETURN b.height, sum(o.value) AS coinbase_reward
    ORDER BY b.height
    v} *)
let q7_coinbase_rewards store ~start_height ~end_height =
  let rewards = ref [] in
  for height = start_height to end_height - 1 do
    let txs = Query.block_transactions store height in
    (* Coinbase is typically the first transaction with no inputs *)
    match txs with
    | [] -> ()
    | coinbase :: _ ->
        let inputs = Query.tx_inputs store coinbase.tx_id in
        if List.length inputs = 0 then begin
          let outputs = Query.tx_outputs store coinbase.tx_id in
          let reward =
            List.fold_left
              (fun acc (o : Types.output) -> Int64.add acc o.out_value)
              0L outputs
          in
          rewards := (height, reward) :: !rewards
        end
  done;
  List.rev !rewards

(** Q8: Distribution of output script types.

    {v
    MATCH (b:Block)-[:CONTAINS]->(t:Transaction)-[:TX_OUTPUT]->(o:Output)
    WHERE b.height >= $start AND b.height < $end
    RETURN o.scriptType, count(o)
    ORDER BY count(o) DESC
    v} *)
let q8_script_type_distribution store ~start_height ~end_height =
  let counts = Hashtbl.create 10 in
  for height = start_height to end_height - 1 do
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

(** Q9: Find high-value transactions (> threshold BTC).

    {v
    MATCH (t:Transaction)-[:TX_OUTPUT]->(o:Output)
    WITH t, sum(o.value) AS total_output
    WHERE total_output > $threshold
    RETURN t.txId, total_output
    ORDER BY total_output DESC
    LIMIT $limit
    v} *)
let q9_high_value_transactions store ~start_height ~end_height ~threshold_satoshis
    ~limit =
  let results = ref [] in
  for height = start_height to end_height - 1 do
    let txs = Query.block_transactions store height in
    List.iter
      (fun (tx : Types.transaction) ->
        let outputs = Query.tx_outputs store tx.tx_id in
        let total =
          List.fold_left
            (fun acc (o : Types.output) -> Int64.add acc o.out_value)
            0L outputs
        in
        if Int64.compare total threshold_satoshis > 0 then
          results := (tx.tx_id, total) :: !results)
      txs
  done;
  let sorted =
    List.sort (fun (_, a) (_, b) -> Int64.compare b a) !results
  in
  if List.length sorted > limit then
    List.filteri (fun i _ -> i < limit) sorted
  else sorted

(** Q10: Block statistics summary.

    {v
    MATCH (b:Block {height: $height})-[:CONTAINS]->(t:Transaction)
    OPTIONAL MATCH (t)-[:TX_OUTPUT]->(o:Output)
    OPTIONAL MATCH (t)-[i:TX_INPUT]->(:Output)
    RETURN b.height, b.hash, count(DISTINCT t) AS tx_count,
           count(DISTINCT o) AS output_count, count(DISTINCT i) AS input_count
    v} *)
let q10_block_stats store height =
  match Query.get_block store height with
  | None -> None
  | Some block ->
      let txs = Query.block_transactions store height in
      let tx_count = List.length txs in
      let output_count = ref 0 in
      let input_count = ref 0 in
      List.iter
        (fun (tx : Types.transaction) ->
          let outputs = Query.tx_outputs store tx.tx_id in
          let inputs = Query.tx_inputs store tx.tx_id in
          output_count := !output_count + List.length outputs;
          input_count := !input_count + List.length inputs)
        txs;
      Some (block, tx_count, !output_count, !input_count)

(** {1 Main benchmark runner} *)

let run_benchmarks store =
  let last_height = Query.last_block_height store in
  Printf.printf "Store has %d blocks\n\n" (last_height + 1);

  (* Benchmark parameters - adjust based on store size *)
  let small_range = min 100 (last_height + 1) in
  let medium_range = min 1000 (last_height + 1) in
  let start = 0 in

  Printf.printf "=== Benchmark: Small range (%d blocks) ===\n" small_range;

  let _ =
    time_it "Q1: Count transactions" (fun () ->
        q1_count_transactions store ~start_height:start
          ~end_height:(start + small_range))
  in

  let _ =
    time_it "Q2: Total output value" (fun () ->
        q2_total_output_value store ~start_height:start
          ~end_height:(start + small_range))
  in

  let _ =
    time_it "Q3: Multi-input txs (>2 inputs)" (fun () ->
        q3_multi_input_transactions store ~start_height:start
          ~end_height:(start + small_range) ~threshold:2)
  in

  let _ =
    time_it "Q4: Total fees" (fun () ->
        q4_total_fees store ~start_height:start
          ~end_height:(start + small_range))
  in

  let _ =
    time_it "Q5: UTXO stats" (fun () ->
        q5_utxo_stats store ~start_height:start
          ~end_height:(start + small_range))
  in

  let _ =
    time_it "Q6: Avg tx size" (fun () ->
        q6_avg_tx_size store ~start_height:start
          ~end_height:(start + small_range))
  in

  let _ =
    time_it "Q7: Coinbase rewards" (fun () ->
        q7_coinbase_rewards store ~start_height:start
          ~end_height:(start + small_range))
  in

  let _ =
    time_it "Q8: Script type distribution" (fun () ->
        q8_script_type_distribution store ~start_height:start
          ~end_height:(start + small_range))
  in

  let _ =
    time_it "Q9: High-value txs (>1 BTC)" (fun () ->
        q9_high_value_transactions store ~start_height:start
          ~end_height:(start + small_range)
          ~threshold_satoshis:100_000_000L ~limit:10)
  in

  let _ =
    time_it "Q10: Block stats (single block)" (fun () ->
        q10_block_stats store (start + small_range / 2))
  in

  Printf.printf "\n=== Benchmark: Medium range (%d blocks) ===\n" medium_range;

  let tx_count =
    time_it "Q1: Count transactions" (fun () ->
        q1_count_transactions store ~start_height:start
          ~end_height:(start + medium_range))
  in
  Printf.printf "     Result: %d transactions\n" tx_count;

  let total_value =
    time_it "Q2: Total output value" (fun () ->
        q2_total_output_value store ~start_height:start
          ~end_height:(start + medium_range))
  in
  Printf.printf "     Result: %Ld satoshis (%.2f BTC)\n" total_value
    (Query.satoshis_to_btc total_value);

  let multi_input =
    time_it "Q3: Multi-input txs (>2 inputs)" (fun () ->
        q3_multi_input_transactions store ~start_height:start
          ~end_height:(start + medium_range) ~threshold:2)
  in
  Printf.printf "     Result: %d transactions\n" multi_input;

  let total_fees =
    time_it "Q4: Total fees" (fun () ->
        q4_total_fees store ~start_height:start
          ~end_height:(start + medium_range))
  in
  Printf.printf "     Result: %Ld satoshis (%.4f BTC)\n" total_fees
    (Query.satoshis_to_btc total_fees);

  let utxo_count, utxo_value =
    time_it "Q5: UTXO stats" (fun () ->
        q5_utxo_stats store ~start_height:start
          ~end_height:(start + medium_range))
  in
  Printf.printf "     Result: %d UTXOs, %Ld satoshis (%.2f BTC)\n" utxo_count
    utxo_value (Query.satoshis_to_btc utxo_value);

  let avg_size =
    time_it "Q6: Avg tx size" (fun () ->
        q6_avg_tx_size store ~start_height:start
          ~end_height:(start + medium_range))
  in
  Printf.printf "     Result: %.1f bytes\n" avg_size;

  let script_dist =
    time_it "Q8: Script type distribution" (fun () ->
        q8_script_type_distribution store ~start_height:start
          ~end_height:(start + medium_range))
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
