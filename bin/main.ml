open Cmdliner
open Blocksci

let default_store = "/tmp/irmin-blocksci-store"

let run_with_store ~sw ~fs store_path f =
  let root = Eio.Path.(fs / store_path) in
  let repo = Store.init ~sw ~fs root in
  let main = Store.main repo in
  Fun.protect ~finally:(fun () -> Store.Store.Repo.close repo) (fun () -> f main)

let import_cmd env =
  let doc = "Import BlockSci CSV export into Irmin store" in
  let export_dir =
    Arg.(
      required
      & pos 0 (some string) None
      & info [] ~docv:"DIR" ~doc:"Path to the CSV export directory")
  in
  let store_path =
    Arg.(
      value
      & opt string default_store
      & info [ "s"; "store" ] ~docv:"PATH"
          ~doc:"Path to the Irmin store (default: /tmp/irmin-blocksci-store)")
  in
  let run export_dir store_path =
    Eio.Switch.run @@ fun sw ->
    let fs = Eio.Stdenv.fs env in
    run_with_store ~sw ~fs store_path (fun main ->
        let dir = Eio.Path.(fs / export_dir) in
        Import.import_all main dir)
  in
  let info = Cmd.info "import" ~doc in
  Cmd.v info Term.(const run $ export_dir $ store_path)

let query_block_cmd env =
  let doc = "Query a block by height" in
  let height =
    Arg.(
      required & pos 0 (some int) None & info [] ~docv:"HEIGHT" ~doc:"Block height")
  in
  let store_path =
    Arg.(
      value
      & opt string default_store
      & info [ "s"; "store" ] ~docv:"PATH" ~doc:"Path to the Irmin store")
  in
  let run height store_path =
    Eio.Switch.run @@ fun sw ->
    let fs = Eio.Stdenv.fs env in
    run_with_store ~sw ~fs store_path (fun main ->
        match Query.get_block main height with
        | None -> Printf.printf "Block %d not found\n" height
        | Some block ->
            Query.print_block block;
            let txs = Query.block_transactions main height in
            Printf.printf "  Transactions: %d\n" (List.length txs);
            List.iter
              (fun (tx : Types.transaction) ->
                Printf.printf "    TX %d: %s\n" tx.tx_id tx.tx_hash)
              txs)
  in
  let info = Cmd.info "block" ~doc in
  Cmd.v info Term.(const run $ height $ store_path)

let query_tx_cmd env =
  let doc = "Query a transaction by ID" in
  let tx_id =
    Arg.(
      required & pos 0 (some int) None & info [] ~docv:"TX_ID" ~doc:"Transaction ID")
  in
  let store_path =
    Arg.(
      value
      & opt string default_store
      & info [ "s"; "store" ] ~docv:"PATH" ~doc:"Path to the Irmin store")
  in
  let run tx_id store_path =
    Eio.Switch.run @@ fun sw ->
    let fs = Eio.Stdenv.fs env in
    run_with_store ~sw ~fs store_path (fun main ->
        match Query.tx_details main tx_id with
        | None -> Printf.printf "Transaction %d not found\n" tx_id
        | Some (tx, inputs, outputs, block) ->
            Query.print_transaction tx;
            (match block with
            | Some (b : Types.block) ->
                Printf.printf "  In block: %d (%s)\n" b.height b.hash
            | None -> ());
            Printf.printf "  Inputs (%d):\n" (List.length inputs);
            List.iter
              (fun ((inp : Types.input), spent_output, addr) ->
                Printf.printf "    [%d] spends %d:%d" inp.in_index
                  inp.in_spent_tx_id inp.in_spent_vout;
                (match spent_output with
                | Some (o : Types.output) ->
                    Printf.printf " (%Ld satoshis = %.8f BTC)" o.out_value
                      (Query.satoshis_to_btc o.out_value)
                | None -> ());
                (match addr with
                | Some a -> Printf.printf " from %s" a
                | None -> ());
                Printf.printf "\n")
              inputs;
            Printf.printf "  Outputs (%d):\n" (List.length outputs);
            List.iter
              (fun ((o : Types.output), addr) ->
                Printf.printf "    [%d] %Ld satoshis = %.8f BTC (%s)" o.out_vout
                  o.out_value (Query.satoshis_to_btc o.out_value) o.out_script_type;
                (match addr with Some a -> Printf.printf " to %s" a | None -> ());
                let spent =
                  if Query.is_output_spent main o.out_tx_id o.out_vout then
                    " [SPENT]"
                  else " [UNSPENT]"
                in
                Printf.printf "%s\n" spent)
              outputs)
  in
  let info = Cmd.info "tx" ~doc in
  Cmd.v info Term.(const run $ tx_id $ store_path)

let query_balance_cmd env =
  let doc = "Query balance for an address" in
  let address =
    Arg.(
      required & pos 0 (some string) None & info [] ~docv:"ADDRESS" ~doc:"Address")
  in
  let store_path =
    Arg.(
      value
      & opt string default_store
      & info [ "s"; "store" ] ~docv:"PATH" ~doc:"Path to the Irmin store")
  in
  let run address store_path =
    Eio.Switch.run @@ fun sw ->
    let fs = Eio.Stdenv.fs env in
    run_with_store ~sw ~fs store_path (fun main ->
        match Query.get_address main address with
        | None -> Printf.printf "Address %s not found\n" address
        | Some addr ->
            Query.print_address addr;
            let outputs = Query.address_outputs main address in
            Printf.printf "Total outputs: %d\n" (List.length outputs);
            let unspent =
              List.filter
                (fun (o : Types.output) ->
                  not (Query.is_output_spent main o.out_tx_id o.out_vout))
                outputs
            in
            Printf.printf "Unspent outputs: %d\n" (List.length unspent);
            let balance = Query.address_balance main address in
            Printf.printf "Balance: %Ld satoshis = %.8f BTC\n" balance
              (Query.satoshis_to_btc balance))
  in
  let info = Cmd.info "balance" ~doc in
  Cmd.v info Term.(const run $ address $ store_path)

let query_chain_cmd env =
  let doc = "Query a range of blocks" in
  let start_height =
    Arg.(
      required
      & pos 0 (some int) None
      & info [] ~docv:"START" ~doc:"Starting block height")
  in
  let count =
    Arg.(
      value & opt int 10 & info [ "n"; "count" ] ~doc:"Number of blocks to query")
  in
  let store_path =
    Arg.(
      value
      & opt string default_store
      & info [ "s"; "store" ] ~docv:"PATH" ~doc:"Path to the Irmin store")
  in
  let run start_height count store_path =
    Eio.Switch.run @@ fun sw ->
    let fs = Eio.Stdenv.fs env in
    run_with_store ~sw ~fs store_path (fun main ->
        let blocks = Query.block_chain main start_height count in
        Printf.printf "Blocks %d to %d:\n" start_height
          (start_height + List.length blocks - 1);
        List.iter
          (fun (block : Types.block) ->
            Printf.printf "  %d: %s (txs: %d)\n" block.height block.hash
              (List.length (Query.block_transactions main block.height)))
          blocks)
  in
  let info = Cmd.info "chain" ~doc in
  Cmd.v info Term.(const run $ start_height $ count $ store_path)

let query_info_cmd env =
  let doc = "Show store information (last block height, etc.)" in
  let store_path =
    Arg.(
      value
      & opt string default_store
      & info [ "s"; "store" ] ~docv:"PATH" ~doc:"Path to the Irmin store")
  in
  let run store_path =
    Eio.Switch.run @@ fun sw ->
    let fs = Eio.Stdenv.fs env in
    run_with_store ~sw ~fs store_path (fun main ->
        let last_height = Query.last_block_height main in
        if last_height < 0 then Printf.printf "Store is empty (no blocks)\n"
        else begin
          Printf.printf "Last block height: %d\n" last_height;
          match Query.get_block main last_height with
          | Some block ->
              Printf.printf "Last block hash: %s\n" block.hash;
              Printf.printf "Last block timestamp: %Ld\n" block.timestamp
          | None -> ()
        end)
  in
  let info = Cmd.info "info" ~doc in
  Cmd.v info Term.(const run $ store_path)

let query_output_cmd env =
  let doc = "Query an output by tx_id:vout" in
  let output_ref =
    Arg.(
      required
      & pos 0 (some string) None
      & info [] ~docv:"TX_ID:VOUT" ~doc:"Output reference (tx_id:vout)")
  in
  let store_path =
    Arg.(
      value
      & opt string default_store
      & info [ "s"; "store" ] ~docv:"PATH" ~doc:"Path to the Irmin store")
  in
  let run output_ref store_path =
    Eio.Switch.run @@ fun sw ->
    let fs = Eio.Stdenv.fs env in
    run_with_store ~sw ~fs store_path (fun main ->
        match String.split_on_char ':' output_ref with
        | [ tx_id_s; vout_s ] -> (
            let tx_id = int_of_string tx_id_s in
            let vout = int_of_string vout_s in
            match Query.get_output main tx_id vout with
            | None -> Printf.printf "Output %d:%d not found\n" tx_id vout
            | Some (o : Types.output) ->
                Printf.printf "Output %d:%d:\n" tx_id vout;
                Printf.printf "  Value: %Ld satoshis = %.8f BTC\n" o.out_value
                  (Query.satoshis_to_btc o.out_value);
                Printf.printf "  Script type: %s\n" o.out_script_type;
                (match Query.output_address main tx_id vout with
                | Some addr -> Printf.printf "  Address: %s\n" addr
                | None -> ());
                (match Query.output_spent_by main tx_id vout with
                | Some spending_tx ->
                    Printf.printf "  Spent by tx: %d\n" spending_tx
                | None -> Printf.printf "  Status: UNSPENT\n"))
        | _ -> Printf.printf "Invalid output reference. Use format: tx_id:vout\n")
  in
  let info = Cmd.info "output" ~doc in
  Cmd.v info Term.(const run $ output_ref $ store_path)

let query_cmd env =
  let doc = "Query the blockchain data" in
  let info = Cmd.info "query" ~doc in
  Cmd.group info
    [
      query_block_cmd env;
      query_tx_cmd env;
      query_balance_cmd env;
      query_chain_cmd env;
      query_output_cmd env;
      query_info_cmd env;
    ]

let main_cmd env =
  let doc = "BlockSci data stored in Irmin" in
  let info =
    Cmd.info "irmin-blocksci" ~version:"0.1" ~doc
      ~man:
        [
          `S Manpage.s_description;
          `P
            "Store and query Bitcoin blockchain data from BlockSci CSV exports \
             using Irmin.";
          `S Manpage.s_commands;
          `P "import DIR - Import CSV data from DIR";
          `P "query block HEIGHT - Query block at HEIGHT";
          `P "query tx TX_ID - Query transaction TX_ID";
          `P "query balance ADDRESS - Query balance for ADDRESS";
          `P "query chain START [-n COUNT] - Query blocks from START";
          `P "query output TX_ID:VOUT - Query output";
          `P "query info - Show store information (last block height)";
        ]
  in
  Cmd.group info ~default:Term.(ret (const (`Help (`Pager, None))))
    [ import_cmd env; query_cmd env ]

let () =
  Eio_main.run @@ fun env ->
  exit (Cmd.eval (main_cmd env))
