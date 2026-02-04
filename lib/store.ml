module Conf = struct
  let entries = 32
  let stable_hash = 256
  let contents_length_header = Some `Varint
  let inode_child_order = `Seeded_hash
  let forbid_empty_dir_persistence = false
end

module KV = Irmin_pack_unix.KV (Conf)
module Store = KV.Make (Irmin.Contents.String)

let block_path height = [ "block"; string_of_int height ]
let tx_path tx_id = [ "tx"; string_of_int tx_id ]

let output_path tx_id vout =
  [ "output"; string_of_int tx_id; string_of_int vout ]

let address_path addr = [ "address"; addr ]
let block_txs_path height = [ "index"; "block_txs"; string_of_int height ]
let block_tx_path height idx = block_txs_path height @ [ string_of_int idx ]
let tx_inputs_path tx_id = [ "index"; "tx_inputs"; string_of_int tx_id ]
let tx_input_path tx_id idx = tx_inputs_path tx_id @ [ string_of_int idx ]
let tx_outputs_path tx_id = [ "index"; "tx_outputs"; string_of_int tx_id ]
let tx_output_path tx_id idx = tx_outputs_path tx_id @ [ string_of_int idx ]
let addr_outputs_path addr = [ "index"; "addr_outputs"; addr ]

let addr_output_path addr tx_id vout =
  addr_outputs_path addr @ [ Printf.sprintf "%d:%d" tx_id vout ]

let output_addr_path tx_id vout =
  [ "index"; "output_addr"; string_of_int tx_id; string_of_int vout ]

let spent_by_path tx_id vout =
  [ "index"; "spent_by"; string_of_int tx_id; string_of_int vout ]

let init ~sw ~fs root =
  let config = Irmin_pack.Conf.init ~sw ~fs root in
  Store.Repo.v config

let main repo = Store.main repo

let info msg =
  Store.Info.v ~author:"irmin-blocksci" ~message:msg
    (Int64.of_float (Unix.time ()))

let set store path entity =
  let json = Types.entity_to_json entity in
  Store.set_exn ~info:(fun () -> info "import") store path json

(* Batch operations for efficient bulk imports *)
module Batch = struct
  type t = {
    store : Store.t;
    mutable tree : Store.tree;
    mutable count : int;
    batch_size : int;
  }

  let create ?(batch_size = 10000) store =
    let tree =
      match Store.Head.find store with
      | Some commit -> Store.Commit.tree commit
      | None -> Store.Tree.empty ()
    in
    { store; tree; count = 0; batch_size }

  let set batch path entity =
    let json = Types.entity_to_json entity in
    batch.tree <- Store.Tree.add batch.tree path json;
    batch.count <- batch.count + 1;
    if batch.count >= batch.batch_size then begin
      Store.set_tree_exn ~info:(fun () -> info "batch import") batch.store [] batch.tree;
      batch.count <- 0
    end

  let flush batch =
    if batch.count > 0 then begin
      Store.set_tree_exn ~info:(fun () -> info "batch import") batch.store [] batch.tree;
      batch.count <- 0
    end

  let mem batch path =
    Store.Tree.mem batch.tree path
end

let get store path =
  match Store.find store path with
  | None -> None
  | Some json -> Types.json_to_entity json

let list store path =
  try Store.list store path |> List.map fst
  with e ->
    Printf.printf "Error listing path %s: %s\n%!" (String.concat "/" path)
      (Printexc.to_string e);
    []
