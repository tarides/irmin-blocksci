open Types

let parse_output_id s =
  match String.split_on_char ':' s with
  | [ tx_id; vout ] -> (int_of_string tx_id, int_of_string vout)
  | _ -> failwith ("Invalid output ID: " ^ s)

(* Progress reporting *)
let report_progress name total new_count =
  Printf.printf "Imported %d %s (%d new)\n%!" total name new_count

let report_progress_inline count interval name =
  if count mod interval = 0 then
    Printf.printf "\r  %s: %d...%!" name count

(* Streaming CSV import helper *)
let with_csv_stream path f =
  let ic = open_in (Eio.Path.native_exn path) in
  Fun.protect
    ~finally:(fun () -> close_in ic)
    (fun () ->
      let csv = Csv.of_channel ic in
      (* Skip header *)
      let _ = Csv.next csv in
      f csv)

let import_blocks batch dir =
  let path = Eio.Path.(dir / "nodes" / "blocks.csv") in
  let total = ref 0 in
  let new_count = ref 0 in
  with_csv_stream path (fun csv ->
    Csv.iter
      ~f:(fun row ->
        incr total;
        report_progress_inline !total 10000 "blocks";
        match row with
        | [ _block_id; height; hash; timestamp; nonce; bits; version; _label ] ->
            let height = int_of_string height in
            if not (Store.Batch.mem batch (Store.block_path height)) then begin
              let block : block =
                {
                  height;
                  hash;
                  timestamp = Int64.of_string timestamp;
                  nonce = Int64.of_string nonce;
                  bits = Int64.of_string bits;
                  version = int_of_string version;
                }
              in
              Store.Batch.set batch (Store.block_path block.height) (Block block);
              incr new_count
            end
        | _ -> failwith "Invalid blocks.csv row")
      csv);
  Store.Batch.flush batch;
  Printf.printf "\r";
  report_progress "blocks" !total !new_count

let import_transactions batch dir =
  let path = Eio.Path.(dir / "nodes" / "transactions.csv") in
  let total = ref 0 in
  let new_count = ref 0 in
  with_csv_stream path (fun csv ->
    Csv.iter
      ~f:(fun row ->
        incr total;
        report_progress_inline !total 100000 "transactions";
        match row with
        | [ tx_id; hash; locktime; version; fee; size; weight; block_height; _label ] ->
            let tx_id = int_of_string tx_id in
            if not (Store.Batch.mem batch (Store.tx_path tx_id)) then begin
              let tx : transaction =
                {
                  tx_id;
                  tx_hash = hash;
                  tx_locktime = Int64.of_string locktime;
                  tx_version = int_of_string version;
                  tx_fee = Int64.of_string fee;
                  tx_size = int_of_string size;
                  tx_weight = int_of_string weight;
                  tx_block_height = int_of_string block_height;
                }
              in
              Store.Batch.set batch (Store.tx_path tx.tx_id) (Transaction tx);
              incr new_count
            end
        | _ -> failwith "Invalid transactions.csv row")
      csv);
  Store.Batch.flush batch;
  Printf.printf "\r";
  report_progress "transactions" !total !new_count

let import_outputs batch dir =
  let path = Eio.Path.(dir / "nodes" / "outputs.csv") in
  let total = ref 0 in
  let new_count = ref 0 in
  with_csv_stream path (fun csv ->
    Csv.iter
      ~f:(fun row ->
        incr total;
        report_progress_inline !total 100000 "outputs";
        match row with
        | [ output_id; value; script_type; _label ] ->
            let tx_id, vout = parse_output_id output_id in
            if not (Store.Batch.mem batch (Store.output_path tx_id vout)) then begin
              let output : output =
                {
                  out_value = Int64.of_string value;
                  out_script_type = script_type;
                  out_tx_id = tx_id;
                  out_vout = vout;
                }
              in
              Store.Batch.set batch (Store.output_path tx_id vout) (Output output);
              incr new_count
            end
        | _ -> failwith "Invalid outputs.csv row")
      csv);
  Store.Batch.flush batch;
  Printf.printf "\r";
  report_progress "outputs" !total !new_count

let import_addresses batch dir =
  let path = Eio.Path.(dir / "nodes" / "addresses.csv") in
  let total = ref 0 in
  let new_count = ref 0 in
  with_csv_stream path (fun csv ->
    Csv.iter
      ~f:(fun row ->
        incr total;
        report_progress_inline !total 100000 "addresses";
        match row with
        | [ address_id; address; addr_type; _label ] ->
            if not (Store.Batch.mem batch (Store.address_path address_id)) then begin
              let addr : address = { addr_str = address; addr_type } in
              Store.Batch.set batch (Store.address_path address_id) (Address addr);
              incr new_count
            end
        | _ -> failwith "Invalid addresses.csv row")
      csv);
  Store.Batch.flush batch;
  Printf.printf "\r";
  report_progress "addresses" !total !new_count

let import_contains batch dir =
  let path = Eio.Path.(dir / "relationships" / "contains.csv") in
  (* Track tx counts per block *)
  let block_txs = Hashtbl.create 1000 in
  let total = ref 0 in
  let new_count = ref 0 in
  with_csv_stream path (fun csv ->
    Csv.iter
      ~f:(fun row ->
        incr total;
        report_progress_inline !total 100000 "block->tx";
        match row with
        | [ block_id; tx_id; _rel_type ] ->
            let block_id = int_of_string block_id in
            let tx_id = int_of_string tx_id in
            let current_count =
              match Hashtbl.find_opt block_txs block_id with
              | Some n -> n
              | None -> 0
            in
            let idx = current_count in
            Hashtbl.replace block_txs block_id (current_count + 1);
            Store.Batch.set batch (Store.block_tx_path block_id idx) (TxRef tx_id);
            incr new_count
        | _ -> failwith "Invalid contains.csv row")
      csv);
  Store.Batch.flush batch;
  Printf.printf "\r";
  report_progress "block->tx relationships" !total !new_count

let import_to_address batch dir =
  let path = Eio.Path.(dir / "relationships" / "to_address.csv") in
  let total = ref 0 in
  let new_count = ref 0 in
  with_csv_stream path (fun csv ->
    Csv.iter
      ~f:(fun row ->
        incr total;
        report_progress_inline !total 100000 "output->addr";
        match row with
        | [ output_id; address_id; _rel_type ] ->
            let tx_id, vout = parse_output_id output_id in
            let oref : output_ref = { ref_tx_id = tx_id; ref_vout = vout } in
            Store.Batch.set batch
              (Store.addr_output_path address_id tx_id vout)
              (OutputRef oref);
            Store.Batch.set batch (Store.output_addr_path tx_id vout) (AddrRef address_id);
            incr new_count
        | _ -> failwith "Invalid to_address.csv row")
      csv);
  Store.Batch.flush batch;
  Printf.printf "\r";
  report_progress "output->address relationships" !total !new_count

let import_tx_input batch dir =
  let path = Eio.Path.(dir / "relationships" / "tx_input.csv") in
  let total = ref 0 in
  let new_count = ref 0 in
  with_csv_stream path (fun csv ->
    Csv.iter
      ~f:(fun row ->
        incr total;
        report_progress_inline !total 100000 "tx_input";
        match row with
        | [ tx_id; output_id; index; sequence; _rel_type ] ->
            let tx_id = int_of_string tx_id in
            let spent_tx_id, spent_vout = parse_output_id output_id in
            let index = int_of_string index in
            let input : input =
              {
                in_spent_tx_id = spent_tx_id;
                in_spent_vout = spent_vout;
                in_index = index;
                in_sequence = Int64.of_string sequence;
              }
            in
            Store.Batch.set batch (Store.tx_input_path tx_id index) (Input input);
            Store.Batch.set batch
              (Store.spent_by_path spent_tx_id spent_vout)
              (TxRef tx_id);
            incr new_count
        | _ -> failwith "Invalid tx_input.csv row")
      csv);
  Store.Batch.flush batch;
  Printf.printf "\r";
  report_progress "tx_input relationships" !total !new_count

let import_tx_output batch dir =
  let path = Eio.Path.(dir / "relationships" / "tx_output.csv") in
  let total = ref 0 in
  let new_count = ref 0 in
  with_csv_stream path (fun csv ->
    Csv.iter
      ~f:(fun row ->
        incr total;
        report_progress_inline !total 100000 "tx_output";
        match row with
        | [ tx_id; output_id; index; _rel_type ] ->
            let tx_id = int_of_string tx_id in
            let out_tx_id, vout = parse_output_id output_id in
            let _ = int_of_string index in
            let oref : output_ref = { ref_tx_id = out_tx_id; ref_vout = vout } in
            Store.Batch.set batch (Store.tx_output_path tx_id vout) (OutputRef oref);
            incr new_count
        | _ -> failwith "Invalid tx_output.csv row")
      csv);
  Store.Batch.flush batch;
  Printf.printf "\r";
  report_progress "tx_output relationships" !total !new_count

let import_all store dir =
  Printf.printf "Importing from %s...\n%!" (Eio.Path.native_exn dir);
  let batch = Store.Batch.create ~batch_size:50000 store in
  import_blocks batch dir;
  import_transactions batch dir;
  import_outputs batch dir;
  import_addresses batch dir;
  import_contains batch dir;
  import_to_address batch dir;
  import_tx_input batch dir;
  import_tx_output batch dir;
  Printf.printf "Import complete!\n%!"
