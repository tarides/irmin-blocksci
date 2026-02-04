(** GraphQL server for querying blockchain data. *)

open Graphql_lwt

(** GraphQL schema for blockchain queries. *)
module Schema = struct
  (** Block type in GraphQL *)
  let block =
    Schema.(
      obj "Block"
        ~fields:
          [
            field "height" ~typ:(non_null int) ~args:Arg.[]
              ~resolve:(fun _ (b : Types.block) -> b.height);
            field "hash" ~typ:(non_null string) ~args:Arg.[]
              ~resolve:(fun _ (b : Types.block) -> b.hash);
            field "timestamp" ~typ:(non_null string) ~args:Arg.[]
              ~resolve:(fun _ (b : Types.block) -> Int64.to_string b.timestamp);
            field "nonce" ~typ:(non_null string) ~args:Arg.[]
              ~resolve:(fun _ (b : Types.block) -> Int64.to_string b.nonce);
            field "bits" ~typ:(non_null string) ~args:Arg.[]
              ~resolve:(fun _ (b : Types.block) -> Int64.to_string b.bits);
            field "version" ~typ:(non_null int) ~args:Arg.[]
              ~resolve:(fun _ (b : Types.block) -> b.version);
          ])

  (** Transaction type in GraphQL *)
  let transaction =
    Schema.(
      obj "Transaction"
        ~fields:
          [
            field "txId" ~typ:(non_null int) ~args:Arg.[]
              ~resolve:(fun _ (tx : Types.transaction) -> tx.tx_id);
            field "hash" ~typ:(non_null string) ~args:Arg.[]
              ~resolve:(fun _ (tx : Types.transaction) -> tx.tx_hash);
            field "blockHeight" ~typ:(non_null int) ~args:Arg.[]
              ~resolve:(fun _ (tx : Types.transaction) -> tx.tx_block_height);
            field "fee" ~typ:(non_null string) ~args:Arg.[]
              ~resolve:(fun _ (tx : Types.transaction) ->
                Int64.to_string tx.tx_fee);
            field "size" ~typ:(non_null int) ~args:Arg.[]
              ~resolve:(fun _ (tx : Types.transaction) -> tx.tx_size);
            field "weight" ~typ:(non_null int) ~args:Arg.[]
              ~resolve:(fun _ (tx : Types.transaction) -> tx.tx_weight);
            field "locktime" ~typ:(non_null string) ~args:Arg.[]
              ~resolve:(fun _ (tx : Types.transaction) ->
                Int64.to_string tx.tx_locktime);
            field "version" ~typ:(non_null int) ~args:Arg.[]
              ~resolve:(fun _ (tx : Types.transaction) -> tx.tx_version);
          ])

  (** Output type in GraphQL *)
  let output =
    Schema.(
      obj "Output"
        ~fields:
          [
            field "txId" ~typ:(non_null int) ~args:Arg.[]
              ~resolve:(fun _ (o : Types.output) -> o.out_tx_id);
            field "vout" ~typ:(non_null int) ~args:Arg.[]
              ~resolve:(fun _ (o : Types.output) -> o.out_vout);
            field "value" ~typ:(non_null string) ~args:Arg.[]
              ~resolve:(fun _ (o : Types.output) -> Int64.to_string o.out_value);
            field "valueBtc" ~typ:(non_null float) ~args:Arg.[]
              ~resolve:(fun _ (o : Types.output) ->
                Query.satoshis_to_btc o.out_value);
            field "scriptType" ~typ:(non_null string) ~args:Arg.[]
              ~resolve:(fun _ (o : Types.output) -> o.out_script_type);
          ])

  (** Address type in GraphQL *)
  let address =
    Schema.(
      obj "Address"
        ~fields:
          [
            field "address" ~typ:(non_null string) ~args:Arg.[]
              ~resolve:(fun _ (a : Types.address) -> a.addr_str);
            field "type" ~typ:(non_null string) ~args:Arg.[]
              ~resolve:(fun _ (a : Types.address) -> a.addr_type);
          ])

  (** Store info type in GraphQL *)
  let store_info =
    Schema.(
      obj "StoreInfo"
        ~fields:
          [
            field "lastBlockHeight" ~typ:(non_null int) ~args:Arg.[]
              ~resolve:(fun _ height -> height);
          ])

  (** Create the query schema with a store reference *)
  let make_schema store =
    Schema.(
      schema
        [
          field "block" ~typ:block
            ~args:Arg.[ arg "height" ~typ:(non_null int) ]
            ~resolve:(fun _ () height -> Query.get_block store height);
          field "transaction" ~typ:transaction
            ~args:Arg.[ arg "txId" ~typ:(non_null int) ]
            ~resolve:(fun _ () tx_id -> Query.get_transaction store tx_id);
          field "output" ~typ:output
            ~args:
              Arg.[ arg "txId" ~typ:(non_null int); arg "vout" ~typ:(non_null int) ]
            ~resolve:(fun _ () tx_id vout -> Query.get_output store tx_id vout);
          field "address" ~typ:address
            ~args:Arg.[ arg "addressId" ~typ:(non_null string) ]
            ~resolve:(fun _ () addr_id -> Query.get_address store addr_id);
          field "blockTransactions"
            ~typ:(non_null (list (non_null transaction)))
            ~args:Arg.[ arg "height" ~typ:(non_null int) ]
            ~resolve:(fun _ () height -> Query.block_transactions store height);
          field "addressBalance" ~typ:(non_null string)
            ~args:Arg.[ arg "addressId" ~typ:(non_null string) ]
            ~resolve:(fun _ () addr_id ->
              Int64.to_string (Query.address_balance store addr_id));
          field "addressOutputs" ~typ:(non_null (list (non_null output)))
            ~args:Arg.[ arg "addressId" ~typ:(non_null string) ]
            ~resolve:(fun _ () addr_id -> Query.address_outputs store addr_id);
          field "blockChain" ~typ:(non_null (list (non_null block)))
            ~args:
              Arg.
                [
                  arg "startHeight" ~typ:(non_null int);
                  arg "count" ~typ:(non_null int);
                ]
            ~resolve:(fun _ () start count -> Query.block_chain store start count);
          field "storeInfo" ~typ:(non_null store_info) ~args:Arg.[]
            ~resolve:(fun _ () -> Query.last_block_height store);
        ])
end

(** Convert Yojson.Basic.t to Graphql_parser.const_value *)
let rec json_to_const_value : Yojson.Basic.t -> Graphql_parser.const_value =
  function
  | `Null -> `Null
  | `Bool b -> `Bool b
  | `Int i -> `Int i
  | `Float f -> `Float f
  | `String s -> `String s
  | `List l -> `List (List.map json_to_const_value l)
  | `Assoc l -> `Assoc (List.map (fun (k, v) -> (k, json_to_const_value v)) l)

(** GraphiQL HTML interface *)
let graphiql_html =
  {|<!DOCTYPE html>
<html>
<head>
  <title>BlockSci GraphQL Explorer</title>
  <link href="https://unpkg.com/graphiql/graphiql.min.css" rel="stylesheet" />
</head>
<body style="margin: 0;">
  <div id="graphiql" style="height: 100vh;"></div>
  <script crossorigin src="https://unpkg.com/react/umd/react.production.min.js"></script>
  <script crossorigin src="https://unpkg.com/react-dom/umd/react-dom.production.min.js"></script>
  <script crossorigin src="https://unpkg.com/graphiql/graphiql.min.js"></script>
  <script>
    const fetcher = GraphiQL.createFetcher({ url: '/graphql' });
    ReactDOM.render(
      React.createElement(GraphiQL, { fetcher: fetcher }),
      document.getElementById('graphiql'),
    );
  </script>
</body>
</html>|}

(** Start the GraphQL server *)
let start_server ~port store =
  let schema = Schema.make_schema store in
  let callback _conn req body =
    let open Lwt.Syntax in
    let uri = Cohttp.Request.uri req in
    match Uri.path uri with
    | "/graphql" -> (
        let* body_str = Cohttp_lwt.Body.to_string body in
        match Yojson.Basic.from_string body_str with
        | json -> (
            let query =
              Yojson.Basic.Util.(json |> member "query" |> to_string_option)
            in
            let variables =
              Yojson.Basic.Util.(json |> member "variables" |> to_option Fun.id)
            in
            match query with
            | None ->
                Cohttp_lwt_unix.Server.respond_string ~status:`Bad_request
                  ~body:"Missing query" ()
            | Some query -> (
                let variables =
                  Option.map
                    (fun v ->
                      Yojson.Basic.Util.to_assoc v
                      |> List.map (fun (k, v) -> (k, json_to_const_value v)))
                    variables
                in
                match Graphql_parser.parse query with
                | Error err ->
                    Cohttp_lwt_unix.Server.respond_string ~status:`Bad_request
                      ~body:("Parse error: " ^ err) ()
                | Ok doc ->
                    let* result =
                      Graphql_lwt.Schema.execute schema () ?variables doc
                    in
                    let body =
                      match result with
                      | Ok (`Response data) -> Yojson.Basic.to_string data
                      | Ok (`Stream _) ->
                          {|{"errors":[{"message":"Streaming not supported"}]}|}
                      | Error err -> Yojson.Basic.to_string err
                    in
                    Cohttp_lwt_unix.Server.respond_string ~status:`OK
                      ~headers:
                        (Cohttp.Header.of_list
                           [ ("Content-Type", "application/json") ])
                      ~body ()))
        | exception Yojson.Json_error msg ->
            Cohttp_lwt_unix.Server.respond_string ~status:`Bad_request
              ~body:("Invalid JSON: " ^ msg) ())
    | "/" | "/graphiql" ->
        Cohttp_lwt_unix.Server.respond_string ~status:`OK
          ~headers:(Cohttp.Header.of_list [ ("Content-Type", "text/html") ])
          ~body:graphiql_html ()
    | _ ->
        Cohttp_lwt_unix.Server.respond_string ~status:`Not_found
          ~body:"Not found" ()
  in
  let server = Cohttp_lwt_unix.Server.make ~callback () in
  Printf.printf "GraphQL server running at http://localhost:%d/graphql\n%!" port;
  Printf.printf "GraphiQL interface at http://localhost:%d/graphiql\n%!" port;
  Cohttp_lwt_unix.Server.create ~mode:(`TCP (`Port port)) server
