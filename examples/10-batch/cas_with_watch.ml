(* Read-modify-write CAS with [Batch.with_watch].

   Two fibers race to increment the same counter. Each fiber:
     1. opens a WATCH guard on the counter key
     2. reads the current value
     3. queues [SET key (v + 1)] in an atomic batch
     4. commits via [run_with_guard]

   If a rival mutated the key between the guard's WATCH and our
   EXEC, the server returns Null, [run_with_guard] returns
   [Ok None], and we retry from the top. Both fibers eventually
   succeed; the final value is exactly [start + 2 * rounds]. *)

module B = Valkey.Batch
module C = Valkey.Client
module CR = Valkey.Cluster_router

let seeds = [ "valkey-c1", 7000; "valkey-c2", 7001; "valkey-c3", 7002 ]

let key = "ex:cas:{wg}"

let bump_once client =
  (* Loop until the server accepts our EXEC. Each iteration opens
     a fresh guard; old state is always cleaned up by with_watch. *)
  let rec go attempts =
    let outcome =
      B.with_watch client [ key ] (fun guard ->
        let cur =
          match C.get client key with
          | Ok (Some s) -> int_of_string s
          | Ok None -> 0
          | Error _ -> 0
        in
        let b = B.create ~atomic:true ~hint_key:key () in
        let _ = B.queue b [| "SET"; key; string_of_int (cur + 1) |] in
        B.run_with_guard b guard)
    in
    match outcome with
    | Ok (Ok (Some _))   -> attempts + 1
    | Ok (Ok None)       -> go (attempts + 1)   (* rival won the race *)
    | Ok (Error _) | Error _ -> attempts + 1
  in
  go 0

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

      let _ = C.set client key "0" in

      let rounds = 50 in
      let total_a = ref 0 and total_b = ref 0 in
      Eio.Fiber.both
        (fun () ->
          for _ = 1 to rounds do
            total_a := !total_a + bump_once client
          done)
        (fun () ->
          for _ = 1 to rounds do
            total_b := !total_b + bump_once client
          done);

      let final =
        match C.get client key with
        | Ok (Some s) -> s
        | _ -> "?"
      in
      Printf.printf
        "fiber A: %d commit attempts (incl. retries)\n\
         fiber B: %d commit attempts (incl. retries)\n\
         final counter value: %s (expected %d)\n%!"
        !total_a !total_b final (2 * rounds);

      let _ = C.del client [ key ] in
      ()
