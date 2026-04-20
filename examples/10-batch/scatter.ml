(* Heterogeneous batch across cluster slots.

   Builds a batch of mixed commands (GET, SET, INCR, HSET, HGET)
   and runs it non-atomic. Shows the [batch_entry_result] sum and
   how to decode each entry. *)

module B = Valkey.Batch
module C = Valkey.Client
module CR = Valkey.Cluster_router

let seeds = [ "valkey-c1", 7000; "valkey-c2", 7001; "valkey-c3", 7002 ]

let () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config =
    { (CR.Config.default ~seeds) with prefer_hostname = true }
  in
  match CR.create ~sw ~net ~clock ~config () with
  | Error m -> failwith m
  | Ok router ->
      let client = C.from_router ~config:C.Config.default router in
      Fun.protect ~finally:(fun () -> C.close client) @@ fun () ->

      let keys = [ "scatter:a"; "scatter:b"; "scatter:cnt"; "scatter:h" ] in
      let _ = B.del_cluster client keys in

      let b = B.create () in
      let enqueue args =
        match B.queue b args with
        | Ok () -> ()
        | Error _ ->
            (* non-atomic mode never rejects; silence the compiler *)
            ()
      in
      enqueue [| "SET"; "scatter:a"; "one" |];
      enqueue [| "SET"; "scatter:b"; "two" |];
      enqueue [| "INCR"; "scatter:cnt" |];
      enqueue [| "INCR"; "scatter:cnt" |];
      enqueue [| "HSET"; "scatter:h"; "name"; "ada"; "city"; "cambridge" |];
      enqueue [| "GET"; "scatter:a" |];
      enqueue [| "GET"; "scatter:b" |];
      enqueue [| "HGET"; "scatter:h"; "name" |];

      (match B.run ~timeout:2.0 client b with
       | Error e ->
           Format.eprintf "batch: %a@." Valkey.Connection.Error.pp e
       | Ok None ->
           (* non-atomic always returns Some *)
           print_endline "impossible"
       | Ok (Some results) ->
           Array.iteri
             (fun i -> function
                | B.One (Ok v) ->
                    Format.printf "  [%d] %a\n" i Valkey.Resp3.pp v
                | B.One (Error e) ->
                    Format.printf "  [%d] err: %a\n"
                      i Valkey.Connection.Error.pp e
                | B.Many nodes ->
                    Format.printf "  [%d] fan-out: %d replies\n"
                      i (List.length nodes))
             results);

      let _ = B.del_cluster client keys in
      ()
