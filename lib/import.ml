open Types

let parse_output_id s =
  match String.split_on_char ':' s with
  | [ tx_id; vout ] -> (int_of_string tx_id, int_of_string vout)
  | _ -> failwith ("Invalid output ID: " ^ s)

let parse_address_id s =
  match String.split_on_char ':' s with
  | [ _; id ] -> id
  | _ -> failwith ("Invalid address ID: " ^ s)

let import_blocks store dir =
  let path = Eio.Path.(dir / "nodes" / "blocks.csv") in
  let csv = Csv.load (Eio.Path.native_exn path) in
  let rows = List.tl csv in
  List.iter
    (fun row ->
      match row with
      | [ _block_id; height; hash; timestamp; nonce; bits; version; _label ] ->
          let block : block =
            {
              height = int_of_string height;
              hash;
              timestamp = Int64.of_string timestamp;
              nonce = Int64.of_string nonce;
              bits = Int64.of_string bits;
              version = int_of_string version;
            }
          in
          Store.set store (Store.block_path block.height) (Block block)
      | _ -> failwith "Invalid blocks.csv row")
    rows;
  Printf.printf "Imported %d blocks\n%!" (List.length rows)

let import_transactions store dir =
  let path = Eio.Path.(dir / "nodes" / "transactions.csv") in
  let csv = Csv.load (Eio.Path.native_exn path) in
  let rows = List.tl csv in
  List.iter
    (fun row ->
      match row with
      | [ tx_id; hash; locktime; version; fee; size; weight; block_height; _label
        ] ->
          let tx : transaction =
            {
              tx_id = int_of_string tx_id;
              tx_hash = hash;
              tx_locktime = Int64.of_string locktime;
              tx_version = int_of_string version;
              tx_fee = Int64.of_string fee;
              tx_size = int_of_string size;
              tx_weight = int_of_string weight;
              tx_block_height = int_of_string block_height;
            }
          in
          Store.set store (Store.tx_path tx.tx_id) (Transaction tx)
      | _ -> failwith "Invalid transactions.csv row")
    rows;
  Printf.printf "Imported %d transactions\n%!" (List.length rows)

let import_outputs store dir =
  let path = Eio.Path.(dir / "nodes" / "outputs.csv") in
  let csv = Csv.load (Eio.Path.native_exn path) in
  let rows = List.tl csv in
  List.iter
    (fun row ->
      match row with
      | [ output_id; value; script_type; _label ] ->
          let tx_id, vout = parse_output_id output_id in
          let output : output =
            {
              out_value = Int64.of_string value;
              out_script_type = script_type;
              out_tx_id = tx_id;
              out_vout = vout;
            }
          in
          Store.set store (Store.output_path tx_id vout) (Output output)
      | _ -> failwith "Invalid outputs.csv row")
    rows;
  Printf.printf "Imported %d outputs\n%!" (List.length rows)

let import_addresses store dir =
  let path = Eio.Path.(dir / "nodes" / "addresses.csv") in
  let csv = Csv.load (Eio.Path.native_exn path) in
  let rows = List.tl csv in
  List.iter
    (fun row ->
      match row with
      | [ address_id; address; addr_type; _label ] ->
          let addr : address = { addr_str = address; addr_type } in
          Store.set store (Store.address_path address_id) (Address addr)
      | _ -> failwith "Invalid addresses.csv row")
    rows;
  Printf.printf "Imported %d addresses\n%!" (List.length rows)

let import_contains store dir =
  let path = Eio.Path.(dir / "relationships" / "contains.csv") in
  let csv = Csv.load (Eio.Path.native_exn path) in
  let rows = List.tl csv in
  let block_txs = Hashtbl.create 1000 in
  List.iter
    (fun row ->
      match row with
      | [ block_id; tx_id; _rel_type ] ->
          let block_id = int_of_string block_id in
          let tx_id = int_of_string tx_id in
          let idx =
            match Hashtbl.find_opt block_txs block_id with
            | None ->
                Hashtbl.add block_txs block_id 1;
                0
            | Some n ->
                Hashtbl.replace block_txs block_id (n + 1);
                n
          in
          Store.set store (Store.block_tx_path block_id idx) (TxRef tx_id)
      | _ -> failwith "Invalid contains.csv row")
    rows;
  Printf.printf "Imported %d block->tx relationships\n%!" (List.length rows)

let import_to_address store dir =
  let path = Eio.Path.(dir / "relationships" / "to_address.csv") in
  let csv = Csv.load (Eio.Path.native_exn path) in
  let rows = List.tl csv in
  List.iter
    (fun row ->
      match row with
      | [ output_id; address_id; _rel_type ] ->
          let tx_id, vout = parse_output_id output_id in
          let oref : output_ref = { ref_tx_id = tx_id; ref_vout = vout } in
          Store.set store
            (Store.addr_output_path address_id tx_id vout)
            (OutputRef oref);
          Store.set store (Store.output_addr_path tx_id vout) (AddrRef address_id)
      | _ -> failwith "Invalid to_address.csv row")
    rows;
  Printf.printf "Imported %d output->address relationships\n%!" (List.length rows)

let import_tx_input store dir =
  let path = Eio.Path.(dir / "relationships" / "tx_input.csv") in
  let csv = Csv.load (Eio.Path.native_exn path) in
  let rows = List.tl csv in
  List.iter
    (fun row ->
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
          Store.set store (Store.tx_input_path tx_id index) (Input input);
          Store.set store
            (Store.spent_by_path spent_tx_id spent_vout)
            (TxRef tx_id)
      | _ -> failwith "Invalid tx_input.csv row")
    rows;
  Printf.printf "Imported %d tx_input relationships\n%!" (List.length rows)

let import_tx_output store dir =
  let path = Eio.Path.(dir / "relationships" / "tx_output.csv") in
  let csv = Csv.load (Eio.Path.native_exn path) in
  let rows = List.tl csv in
  List.iter
    (fun row ->
      match row with
      | [ tx_id; output_id; index; _rel_type ] ->
          let tx_id = int_of_string tx_id in
          let out_tx_id, vout = parse_output_id output_id in
          let _ = int_of_string index in
          let oref : output_ref = { ref_tx_id = out_tx_id; ref_vout = vout } in
          Store.set store (Store.tx_output_path tx_id vout) (OutputRef oref)
      | _ -> failwith "Invalid tx_output.csv row")
    rows;
  Printf.printf "Imported %d tx_output relationships\n%!" (List.length rows)

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
