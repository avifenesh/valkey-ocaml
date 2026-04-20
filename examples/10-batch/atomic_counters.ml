(* Atomic batch: commit path.

   Queues a read-modify-write triple and runs it as MULTI/EXEC on
   one slot. No WATCH in the demo — see the note below for why.

   {1 WATCH in buffered atomic Batch}

   The [~watch] parameter exists on [Batch.create], but in the
   current buffered model WATCH is sent at [run] time alongside
   MULTI, so it only protects the submillisecond window between
   [WATCH] and [EXEC]. For the classic "read value, decide what
   to write, WATCH-guarded commit" pattern, use [Transaction]
   today (where WATCH is sent at [begin_], giving you a real
   window to read + compute before [exec]).

   A future version will add a [Batch.watch] call that sends
   WATCH immediately, restoring the familiar semantics to the
   buffered API. *)

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
