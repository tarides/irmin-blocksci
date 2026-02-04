open Types

let parse_output_id s =
  match String.split_on_char ':' s with
  | [ tx_id; vout ] -> (int_of_string tx_id, int_of_string vout)
  | _ -> failwith ("Invalid output ID: " ^ s)

(* Check if a path exists in the store *)
let exists store path = Option.is_some (Store.get store path)

(* Path for storing block tx count metadata *)
let block_tx_count_path height = [ "meta"; "block_tx_count"; string_of_int height ]

(* Get current tx count for a block (for incremental import) *)
let get_block_tx_count store height =
  match Store.get store (block_tx_count_path height) with
  | Some (Types.Meta json) -> (
      match int_of_string_opt json with Some n -> n | None -> 0)
  | _ -> 0

(* Set tx count for a block *)
let set_block_tx_count store height count =
  Store.set store (block_tx_count_path height) (Types.Meta (string_of_int count))

let import_blocks store dir =
  let path = Eio.Path.(dir / "nodes" / "blocks.csv") in
  let csv = Csv.load (Eio.Path.native_exn path) in
  let rows = List.tl csv in
  let new_count = ref 0 in
  List.iter
    (fun row ->
      match row with
      | [ _block_id; height; hash; timestamp; nonce; bits; version; _label ] ->
          let height = int_of_string height in
          if not (exists store (Store.block_path height)) then begin
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
            Store.set store (Store.block_path block.height) (Block block);
            incr new_count
          end
      | _ -> failwith "Invalid blocks.csv row")
    rows;
  Printf.printf "Imported %d blocks (%d new)\n%!" (List.length rows) !new_count

let import_transactions store dir =
  let path = Eio.Path.(dir / "nodes" / "transactions.csv") in
  let csv = Csv.load (Eio.Path.native_exn path) in
  let rows = List.tl csv in
  let new_count = ref 0 in
  List.iter
    (fun row ->
      match row with
      | [ tx_id; hash; locktime; version; fee; size; weight; block_height; _label
        ] ->
          let tx_id = int_of_string tx_id in
          if not (exists store (Store.tx_path tx_id)) then begin
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
            Store.set store (Store.tx_path tx.tx_id) (Transaction tx);
            incr new_count
          end
      | _ -> failwith "Invalid transactions.csv row")
    rows;
  Printf.printf "Imported %d transactions (%d new)\n%!" (List.length rows) !new_count

let import_outputs store dir =
  let path = Eio.Path.(dir / "nodes" / "outputs.csv") in
  let csv = Csv.load (Eio.Path.native_exn path) in
  let rows = List.tl csv in
  let new_count = ref 0 in
  List.iter
    (fun row ->
      match row with
      | [ output_id; value; script_type; _label ] ->
          let tx_id, vout = parse_output_id output_id in
          if not (exists store (Store.output_path tx_id vout)) then begin
            let output : output =
              {
                out_value = Int64.of_string value;
                out_script_type = script_type;
                out_tx_id = tx_id;
                out_vout = vout;
              }
            in
            Store.set store (Store.output_path tx_id vout) (Output output);
            incr new_count
          end
      | _ -> failwith "Invalid outputs.csv row")
    rows;
  Printf.printf "Imported %d outputs (%d new)\n%!" (List.length rows) !new_count

let import_addresses store dir =
  let path = Eio.Path.(dir / "nodes" / "addresses.csv") in
  let csv = Csv.load (Eio.Path.native_exn path) in
  let rows = List.tl csv in
  let new_count = ref 0 in
  List.iter
    (fun row ->
      match row with
      | [ address_id; address; addr_type; _label ] ->
          if not (exists store (Store.address_path address_id)) then begin
            let addr : address = { addr_str = address; addr_type } in
            Store.set store (Store.address_path address_id) (Address addr);
            incr new_count
          end
      | _ -> failwith "Invalid addresses.csv row")
    rows;
  Printf.printf "Imported %d addresses (%d new)\n%!" (List.length rows) !new_count

let import_contains store dir =
  let path = Eio.Path.(dir / "relationships" / "contains.csv") in
  let csv = Csv.load (Eio.Path.native_exn path) in
  let rows = List.tl csv in
  (* Track tx counts per block for incremental import *)
  let block_txs = Hashtbl.create 1000 in
  (* Cache of existing tx_ids per block (loaded from store on first access) *)
  let block_existing_txs = Hashtbl.create 1000 in
  let new_count = ref 0 in
  (* Get or load existing tx_ids for a block *)
  let get_existing_txs block_id =
    match Hashtbl.find_opt block_existing_txs block_id with
    | Some set -> set
    | None ->
        let set = Hashtbl.create 10 in
        let count = get_block_tx_count store block_id in
        for i = 0 to count - 1 do
          match Store.get store (Store.block_tx_path block_id i) with
          | Some (TxRef tx_id) -> Hashtbl.add set tx_id true
          | _ -> ()
        done;
        Hashtbl.add block_existing_txs block_id set;
        Hashtbl.add block_txs block_id count;
        set
  in
  List.iter
    (fun row ->
      match row with
      | [ block_id; tx_id; _rel_type ] ->
          let block_id = int_of_string block_id in
          let tx_id = int_of_string tx_id in
          let existing_set = get_existing_txs block_id in
          if not (Hashtbl.mem existing_set tx_id) then begin
            Hashtbl.add existing_set tx_id true;
            let current_count =
              match Hashtbl.find_opt block_txs block_id with
              | Some n -> n
              | None -> 0
            in
            let idx = current_count in
            Hashtbl.replace block_txs block_id (current_count + 1);
            Store.set store (Store.block_tx_path block_id idx) (TxRef tx_id);
            incr new_count
          end
      | _ -> failwith "Invalid contains.csv row")
    rows;
  (* Update stored counts *)
  Hashtbl.iter (fun block_id count -> set_block_tx_count store block_id count) block_txs;
  Printf.printf "Imported %d block->tx relationships (%d new)\n%!" (List.length rows) !new_count

let import_to_address store dir =
  let path = Eio.Path.(dir / "relationships" / "to_address.csv") in
  let csv = Csv.load (Eio.Path.native_exn path) in
  let rows = List.tl csv in
  let new_count = ref 0 in
  List.iter
    (fun row ->
      match row with
      | [ output_id; address_id; _rel_type ] ->
          let tx_id, vout = parse_output_id output_id in
          if not (exists store (Store.output_addr_path tx_id vout)) then begin
            let oref : output_ref = { ref_tx_id = tx_id; ref_vout = vout } in
            Store.set store
              (Store.addr_output_path address_id tx_id vout)
              (OutputRef oref);
            Store.set store (Store.output_addr_path tx_id vout) (AddrRef address_id);
            incr new_count
          end
      | _ -> failwith "Invalid to_address.csv row")
    rows;
  Printf.printf "Imported %d output->address relationships (%d new)\n%!" (List.length rows) !new_count

let import_tx_input store dir =
  let path = Eio.Path.(dir / "relationships" / "tx_input.csv") in
  let csv = Csv.load (Eio.Path.native_exn path) in
  let rows = List.tl csv in
  let new_count = ref 0 in
  List.iter
    (fun row ->
      match row with
      | [ tx_id; output_id; index; sequence; _rel_type ] ->
          let tx_id = int_of_string tx_id in
          let spent_tx_id, spent_vout = parse_output_id output_id in
          let index = int_of_string index in
          if not (exists store (Store.tx_input_path tx_id index)) then begin
            let input : input =
              {
                in_spent_tx_id = spent_tx_id;
                in_spent_vout = spent_vout;
                in_index = index;
                in_sequence = Int64.of_string sequence;
              }
            in
            Store.set store (Store.tx_input_path tx_id index) (Input input);
            Store.set store
              (Store.spent_by_path spent_tx_id spent_vout)
              (TxRef tx_id);
            incr new_count
          end
      | _ -> failwith "Invalid tx_input.csv row")
    rows;
  Printf.printf "Imported %d tx_input relationships (%d new)\n%!" (List.length rows) !new_count

let import_tx_output store dir =
  let path = Eio.Path.(dir / "relationships" / "tx_output.csv") in
  let csv = Csv.load (Eio.Path.native_exn path) in
  let rows = List.tl csv in
  let new_count = ref 0 in
  List.iter
    (fun row ->
      match row with
      | [ tx_id; output_id; index; _rel_type ] ->
          let tx_id = int_of_string tx_id in
          let out_tx_id, vout = parse_output_id output_id in
          let _ = int_of_string index in
          if not (exists store (Store.tx_output_path tx_id vout)) then begin
            let oref : output_ref = { ref_tx_id = out_tx_id; ref_vout = vout } in
            Store.set store (Store.tx_output_path tx_id vout) (OutputRef oref);
            incr new_count
          end
      | _ -> failwith "Invalid tx_output.csv row")
    rows;
  Printf.printf "Imported %d tx_output relationships (%d new)\n%!" (List.length rows) !new_count

let import_all store dir =
  Printf.printf "Importing from %s...\n%!" (Eio.Path.native_exn dir);
  import_blocks store dir;
  import_transactions store dir;
  import_outputs store dir;
  import_addresses store dir;
  import_contains store dir;
  import_to_address store dir;
  import_tx_input store dir;
  import_tx_output store dir;
  Printf.printf "Import complete!\n%!"
