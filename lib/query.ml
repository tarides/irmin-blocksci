open Types

let get_block store height =
  match Store.get store (Store.block_path height) with
  | Some (Block b) -> Some b
  | _ -> None

let get_transaction store tx_id =
  match Store.get store (Store.tx_path tx_id) with
  | Some (Transaction tx) -> Some tx
  | _ -> None

let get_output store tx_id vout =
  match Store.get store (Store.output_path tx_id vout) with
  | Some (Output o) -> Some o
  | _ -> None

let get_address store addr =
  match Store.get store (Store.address_path addr) with
  | Some (Address a) -> Some a
  | _ -> None

let block_transactions store height =
  let keys = Store.list store (Store.block_txs_path height) in
  List.filter_map
    (fun key ->
      match Store.get store (Store.block_txs_path height @ [ key ]) with
      | Some (TxRef tx_id) -> get_transaction store tx_id
      | _ -> None)
    keys

let tx_inputs store tx_id =
  let keys = Store.list store (Store.tx_inputs_path tx_id) in
  List.filter_map
    (fun key ->
      match Store.get store (Store.tx_inputs_path tx_id @ [ key ]) with
      | Some (Input i) -> Some i
      | _ -> None)
    keys

let tx_outputs store tx_id =
  let keys = Store.list store (Store.tx_outputs_path tx_id) in
  List.filter_map
    (fun key ->
      match Store.get store (Store.tx_outputs_path tx_id @ [ key ]) with
      | Some (OutputRef r) -> get_output store r.ref_tx_id r.ref_vout
      | _ -> None)
    keys

let output_address store tx_id vout =
  match Store.get store (Store.output_addr_path tx_id vout) with
  | Some (AddrRef addr) -> Some addr
  | _ -> None

let address_outputs store addr =
  let keys = Store.list store (Store.addr_outputs_path addr) in
  List.filter_map
    (fun key ->
      match Store.get store (Store.addr_outputs_path addr @ [ key ]) with
      | Some (OutputRef r) -> get_output store r.ref_tx_id r.ref_vout
      | _ -> None)
    keys

let output_spent_by store tx_id vout =
  match Store.get store (Store.spent_by_path tx_id vout) with
  | Some (TxRef spending_tx_id) -> Some spending_tx_id
  | _ -> None

let is_output_spent store tx_id vout =
  Option.is_some (output_spent_by store tx_id vout)

let address_balance store addr =
  let outputs = address_outputs store addr in
  List.fold_left
    (fun acc (o : output) ->
      if is_output_spent store o.out_tx_id o.out_vout then acc
      else Int64.add acc o.out_value)
    0L outputs

let block_chain store start_height count =
  let rec collect acc height remaining =
    if remaining <= 0 then List.rev acc
    else
      match get_block store height with
      | Some block -> collect (block :: acc) (height + 1) (remaining - 1)
      | None -> List.rev acc
  in
  collect [] start_height count

let block_with_coinbase store height =
  match get_block store height with
  | None -> None
  | Some block -> (
      let txs = block_transactions store height in
      match txs with
      | [] -> Some (block, None)
      | coinbase :: _ -> Some (block, Some coinbase))

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

module PathFinder = struct
  type path_element =
    | OutputNode of output
    | TxNode of transaction
    | AddressNode of address

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

let print_block (block : block) =
  Printf.printf "Block %d:\n" block.height;
  Printf.printf "  Hash: %s\n" block.hash;
  Printf.printf "  Timestamp: %Ld\n" block.timestamp;
  Printf.printf "  Nonce: %Ld\n" block.nonce;
  Printf.printf "  Bits: %Ld\n" block.bits;
  Printf.printf "  Version: %d\n" block.version

let print_transaction (tx : transaction) =
  Printf.printf "Transaction %d:\n" tx.tx_id;
  Printf.printf "  Hash: %s\n" tx.tx_hash;
  Printf.printf "  Block height: %d\n" tx.tx_block_height;
  Printf.printf "  Fee: %Ld satoshis\n" tx.tx_fee;
  Printf.printf "  Size: %d bytes\n" tx.tx_size;
  Printf.printf "  Weight: %d\n" tx.tx_weight;
  Printf.printf "  Locktime: %Ld\n" tx.tx_locktime;
  Printf.printf "  Version: %d\n" tx.tx_version

let print_output (o : output) =
  Printf.printf "  Output %d:%d - %Ld satoshis (%s)\n" o.out_tx_id o.out_vout
    o.out_value o.out_script_type

let print_address (addr : address) =
  Printf.printf "Address: %s (type: %s)\n" addr.addr_str addr.addr_type

let satoshis_to_btc satoshis = Int64.to_float satoshis /. 100_000_000.0
