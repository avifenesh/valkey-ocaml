(* Atomic batch: commit path.

   Queues SET NX + INCR + INCR + GET against keys co-located via
   a hashtag, runs it as MULTI/EXEC on one primary.

   Concurrent atomic batches on the same router are safe today —
   the router serialises them via a per-primary mutex inside
   [Router.atomic_lock_for_slot]. See [test/test_batch.ml]'s
   [test_atomic_concurrent] for a functional exercise.

   WATCH isn't shown here because this path doesn't need CAS.
   For read-modify-write see [cas_with_watch.ml] in this same
   directory, which uses [Batch.with_watch] to open a real guard
   across the read + EXEC window. *)

module B = Valkey.Batch
module C = Valkey.Client
module CR = Valkey.Cluster_router

let seeds = [ "valkey-c1", 7000; "valkey-c2", 7001; "valkey-c3", 7002 ]

let key = "ex:atomic:{demo}"

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

      let _ = C.del client [ key ] in

      (* Build an atomic batch that (1) seeds the counter if
         missing, (2) increments it twice, (3) reads it back.
         Five queued commands, all pinned to the same slot via
         the hashtag in the key. *)
      let b = B.create ~atomic:true ~hint_key:key () in
      let q args = let _ = B.queue b args in () in
      q [| "SET"; key; "10"; "NX" |];
      q [| "INCR"; key |];
      q [| "INCR"; key |];
      q [| "GET"; key |];

      (match B.run client b with
       | Error e ->
           Format.eprintf "run: %a@." Valkey.Connection.Error.pp e
       | Ok None ->
           (* Only possible if WATCH was supplied and tripped. *)
           print_endline "unexpected WATCH abort"
       | Ok (Some rs) ->
           Array.iteri
             (fun i -> function
                | B.One (Ok v) ->
                    Format.printf "  [%d] %a\n"
                      i Valkey.Resp3.pp v
                | B.One (Error e) ->
                    Format.printf "  [%d] err: %a\n"
                      i Valkey.Connection.Error.pp e
                | B.Many _ -> ())
             rs);

      let _ = C.del client [ key ] in
      ()
