type block = {
  height : int;
  hash : string;
  timestamp : int64;
  nonce : int64;
  bits : int64;
  version : int;
}

type output = {
  out_value : int64;
  out_script_type : string;
  out_tx_id : int;
  out_vout : int;
}

type input = {
  in_spent_tx_id : int;
  in_spent_vout : int;
  in_index : int;
  in_sequence : int64;
}

type transaction = {
  tx_id : int;
  tx_hash : string;
  tx_locktime : int64;
  tx_version : int;
  tx_fee : int64;
  tx_size : int;
  tx_weight : int;
  tx_block_height : int;
}

type address = {
  addr_str : string;
  addr_type : string;
}

type output_ref = {
  ref_tx_id : int;
  ref_vout : int;
}

type entity =
  | Block of block
  | Transaction of transaction
  | Output of output
  | Input of input
  | Address of address
  | OutputRef of output_ref
  | TxRef of int
  | AddrRef of string
  | Meta of string

let block_to_json b =
  Printf.sprintf
    {|{"type":"block","height":%d,"hash":"%s","timestamp":%Ld,"nonce":%Ld,"bits":%Ld,"version":%d}|}
    b.height b.hash b.timestamp b.nonce b.bits b.version

let transaction_to_json t =
  Printf.sprintf
    {|{"type":"tx","id":%d,"hash":"%s","locktime":%Ld,"version":%d,"fee":%Ld,"size":%d,"weight":%d,"block":%d}|}
    t.tx_id t.tx_hash t.tx_locktime t.tx_version t.tx_fee t.tx_size t.tx_weight
    t.tx_block_height

let output_to_json o =
  Printf.sprintf {|{"type":"out","value":%Ld,"script":"%s","tx":%d,"vout":%d}|}
    o.out_value o.out_script_type o.out_tx_id o.out_vout

let input_to_json i =
  Printf.sprintf
    {|{"type":"in","spent_tx":%d,"spent_vout":%d,"idx":%d,"seq":%Ld}|}
    i.in_spent_tx_id i.in_spent_vout i.in_index i.in_sequence

let address_to_json a =
  Printf.sprintf {|{"type":"addr","str":"%s","typ":"%s"|} a.addr_str a.addr_type

let output_ref_to_json r =
  Printf.sprintf {|{"type":"oref","tx":%d,"vout":%d}|} r.ref_tx_id r.ref_vout

let entity_to_json = function
  | Block b -> block_to_json b
  | Transaction t -> transaction_to_json t
  | Output o -> output_to_json o
  | Input i -> input_to_json i
  | Address a -> address_to_json a
  | OutputRef r -> output_ref_to_json r
  | TxRef tx_id -> Printf.sprintf {|{"type":"txref","id":%d}|} tx_id
  | AddrRef addr -> Printf.sprintf {|{"type":"addrref","addr":"%s"}|} addr
  | Meta data -> Printf.sprintf {|{"type":"meta","data":"%s"}|} data

let parse_int64 s = Int64.of_string s
let parse_int s = int_of_string s

let parse_string s =
  let len = String.length s in
  if len >= 2 && s.[0] = '"' && s.[len - 1] = '"' then
    String.sub s 1 (len - 2)
  else s

let find_field json key =
  let pattern = Printf.sprintf "\"%s\":" key in
  match String.index_opt json '"' with
  | None -> None
  | Some _ -> (
      try
        let start = String.index (json) (pattern.[0]) in
        let rec find_pattern pos =
          if pos + String.length pattern > String.length json then None
          else if String.sub json pos (String.length pattern) = pattern then
            Some (pos + String.length pattern)
          else find_pattern (pos + 1)
        in
        match find_pattern start with
        | None -> None
        | Some value_start ->
            let value_end =
              let rec find_end pos depth in_string =
                if pos >= String.length json then pos
                else
                  match json.[pos] with
                  | '"' when not in_string -> find_end (pos + 1) depth true
                  | '"' when json.[pos - 1] <> '\\' ->
                      find_end (pos + 1) depth false
                  | '{' | '[' when not in_string ->
                      find_end (pos + 1) (depth + 1) false
                  | '}' | ']' when not in_string && depth > 0 ->
                      find_end (pos + 1) (depth - 1) false
                  | ',' | '}' | ']' when not in_string && depth = 0 -> pos
                  | _ -> find_end (pos + 1) depth in_string
              in
              find_end value_start 0 false
            in
            Some (String.sub json value_start (value_end - value_start))
      with _ -> None)

let json_to_entity json =
  match find_field json "type" with
  | None -> None
  | Some type_str -> (
      let typ = parse_string type_str in
      match typ with
      | "block" -> (
          match
            ( find_field json "height",
              find_field json "hash",
              find_field json "timestamp",
              find_field json "nonce",
              find_field json "bits",
              find_field json "version" )
          with
          | Some h, Some hash, Some ts, Some n, Some b, Some v ->
              Some
                (Block
                   {
                     height = parse_int h;
                     hash = parse_string hash;
                     timestamp = parse_int64 ts;
                     nonce = parse_int64 n;
                     bits = parse_int64 b;
                     version = parse_int v;
                   })
          | _ -> None)
      | "tx" -> (
          match
            ( find_field json "id",
              find_field json "hash",
              find_field json "locktime",
              find_field json "version",
              find_field json "fee",
              find_field json "size",
              find_field json "weight",
              find_field json "block" )
          with
          | Some id, Some hash, Some lt, Some v, Some fee, Some sz, Some w, Some
              bh ->
              Some
                (Transaction
                   {
                     tx_id = parse_int id;
                     tx_hash = parse_string hash;
                     tx_locktime = parse_int64 lt;
                     tx_version = parse_int v;
                     tx_fee = parse_int64 fee;
                     tx_size = parse_int sz;
                     tx_weight = parse_int w;
                     tx_block_height = parse_int bh;
                   })
          | _ -> None)
      | "out" -> (
          match
            ( find_field json "value",
              find_field json "script",
              find_field json "tx",
              find_field json "vout" )
          with
          | Some v, Some s, Some t, Some vo ->
              Some
                (Output
                   {
                     out_value = parse_int64 v;
                     out_script_type = parse_string s;
                     out_tx_id = parse_int t;
                     out_vout = parse_int vo;
                   })
          | _ -> None)
      | "in" -> (
          match
            ( find_field json "spent_tx",
              find_field json "spent_vout",
              find_field json "idx",
              find_field json "seq" )
          with
          | Some st, Some sv, Some i, Some sq ->
              Some
                (Input
                   {
                     in_spent_tx_id = parse_int st;
                     in_spent_vout = parse_int sv;
                     in_index = parse_int i;
                     in_sequence = parse_int64 sq;
                   })
          | _ -> None)
      | "addr" -> (
          match (find_field json "str", find_field json "typ") with
          | Some s, Some t ->
              Some (Address { addr_str = parse_string s; addr_type = parse_string t })
          | _ -> None)
      | "oref" -> (
          match (find_field json "tx", find_field json "vout") with
          | Some t, Some v ->
              Some (OutputRef { ref_tx_id = parse_int t; ref_vout = parse_int v })
          | _ -> None)
      | "txref" -> (
          match find_field json "id" with
          | Some id -> Some (TxRef (parse_int id))
          | None -> None)
      | "addrref" -> (
          match find_field json "addr" with
          | Some a -> Some (AddrRef (parse_string a))
          | None -> None)
      | "meta" -> (
          match find_field json "data" with
          | Some d -> Some (Meta (parse_string d))
          | None -> None)
      | _ -> None)
