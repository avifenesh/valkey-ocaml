(* Tests for Valkey.Batch: scatter-gather batch primitive and the
   typed cluster-aware helpers built on top.

   Runs against the docker-compose cluster. *)

module C = Valkey.Client
module CR = Valkey.Cluster_router
module B = Valkey.Batch
module E = Valkey.Connection.Error
module Conn = Valkey.Connection

let seeds = [ "valkey-c1", 7000; "valkey-c2", 7001; "valkey-c3", 7002 ]

let force_skip () =
  try Sys.getenv "VALKEY_CLUSTER" = "skip" with Not_found -> false

let cluster_reachable () =
  if force_skip () then false
  else
    try
      Eio_main.run @@ fun env ->
      Eio.Switch.run @@ fun sw ->
      let (host, port) = List.hd seeds in
      let conn =
        Conn.connect ~sw
          ~net:(Eio.Stdenv.net env) ~clock:(Eio.Stdenv.clock env)
          ~config:Conn.Config.default ~host ~port ()
      in
      Conn.close conn;
      true
    with _ -> false

let with_cluster_client f =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config =
    { (CR.Config.default ~seeds) with prefer_hostname = true }
  in
  match CR.create ~sw ~net ~clock ~config () with
  | Error m -> Alcotest.failf "Cluster_router.create: %s" m
  | Ok router ->
      let client = C.from_router ~config:C.Config.default router in
      Fun.protect ~finally:(fun () -> C.close client) @@ fun () -> f client

let err_pp = E.pp

(* 500 spread keys: mset_cluster populates, mget_cluster fetches
   them back, del_cluster wipes. No hashtags — each key lands on
   a random slot, so this exercises the per-slot bucketing +
   parallel dispatch across every primary. *)
let test_mset_mget_del_roundtrip () =
  with_cluster_client @@ fun client ->
  let n = 500 in
  let kvs =
    List.init n
      (fun i ->
        Printf.sprintf "batch:demo:%d" i,
        Printf.sprintf "v-%d" i)
  in
  let keys = List.map fst kvs in

  (* Pre-clean so partial state from earlier runs doesn't skew us. *)
  let _ = B.del_cluster client keys in

  (match B.mset_cluster client kvs with
   | Ok () -> ()
   | Error e -> Alcotest.failf "mset_cluster: %a" err_pp e);

  (match B.mget_cluster client keys with
   | Error e -> Alcotest.failf "mget_cluster: %a" err_pp e
   | Ok pairs ->
       Alcotest.(check int) "length" n (List.length pairs);
       List.iter2
         (fun (exp_k, exp_v) (got_k, got_v) ->
           Alcotest.(check string) "key order" exp_k got_k;
           match got_v with
           | Some v when v = exp_v -> ()
           | Some other ->
               Alcotest.failf "mget %s: expected %s, got %s"
                 exp_k exp_v other
           | None ->
               Alcotest.failf "mget %s: missing" exp_k)
         kvs pairs);

  match B.del_cluster client keys with
  | Ok n_removed ->
      Alcotest.(check int) "del_cluster count" n n_removed
  | Error e ->
      Alcotest.failf "del_cluster: %a" err_pp e

(* Heterogeneous batch: mix GET / SET / INCR / HSET across slots.
   Each queued command keeps its own result; we unwrap the
   variant and check decoded values. *)
let test_heterogeneous_batch () =
  with_cluster_client @@ fun client ->
  let _ = B.del_cluster client [ "het:a"; "het:b"; "het:c"; "het:h" ] in

  let b = B.create () in
  let q args =
    match B.queue b args with
    | Ok () -> ()
    | Error _ ->
        Alcotest.fail "queue rejected in non-atomic mode (unexpected)"
  in
  q [| "SET"; "het:a"; "one" |];
  q [| "SET"; "het:b"; "two" |];
  q [| "INCR"; "het:c" |];
  q [| "HSET"; "het:h"; "name"; "ada" |];
  q [| "GET"; "het:a" |];
  q [| "GET"; "het:c" |];

  (match B.run client b with
   | Error e -> Alcotest.failf "run: %a" err_pp e
   | Ok None -> Alcotest.fail "unexpected atomic abort"
   | Ok (Some rs) ->
       Alcotest.(check int) "result count" 6 (Array.length rs);
       (* SET should return +OK (Simple_string). *)
       (match rs.(0) with
        | B.One (Ok (Valkey.Resp3.Simple_string "OK")) -> ()
        | _ -> Alcotest.fail "rs.0: expected SET OK");
       (* INCR returns an integer. *)
       (match rs.(2) with
        | B.One (Ok (Valkey.Resp3.Integer 1L)) -> ()
        | _ -> Alcotest.fail "rs.2: expected INCR 1");
       (* HSET returns count of new fields. *)
       (match rs.(3) with
        | B.One (Ok (Valkey.Resp3.Integer 1L)) -> ()
        | _ -> Alcotest.fail "rs.3: expected HSET 1");
       (* GET het:a -> "one" *)
       (match rs.(4) with
        | B.One (Ok (Valkey.Resp3.Bulk_string "one")) -> ()
        | _ -> Alcotest.fail "rs.4: expected GET one");
       (* GET het:c -> "1" *)
       (match rs.(5) with
        | B.One (Ok (Valkey.Resp3.Bulk_string "1")) -> ()
        | _ -> Alcotest.fail "rs.5: expected GET 1"));

  let _ = B.del_cluster client [ "het:a"; "het:b"; "het:c"; "het:h" ] in
  ()

(* Atomic mode rejects fan-out commands at queue time with a
   structured error naming the offending command. *)
let test_atomic_rejects_fan_out () =
  (* Pure logic — no cluster needed; the rejection happens in
     Batch.queue before any dispatch. *)
  let b = B.create ~atomic:true ~hint_key:"x" () in
  match B.queue b [| "SCRIPT"; "LOAD"; "return 1" |] with
  | Error (B.Fan_out_in_atomic_batch name) ->
      Alcotest.(check string) "command name" "SCRIPT" name
  | Ok () ->
      Alcotest.fail "atomic batch accepted a fan-out command (SCRIPT LOAD)"

(* Atomic batch: commit path. Hashtag pins everything to the same
   slot; EXEC returns the per-command array. *)
let test_atomic_commits () =
  with_cluster_client @@ fun client ->
  let k = "cart:{atomic-demo}" in
  let ctr = "ctr:{atomic-demo}" in
  let _ = C.del client [ k; ctr ] in

  let b = B.create ~atomic:true ~hint_key:k () in
  let _ = B.queue b [| "SET"; k; "hello" |] in
  let _ = B.queue b [| "INCR"; ctr |] in
  let _ = B.queue b [| "GET"; k |] in

  (match B.run client b with
   | Error e -> Alcotest.failf "atomic run: %a" err_pp e
   | Ok None -> Alcotest.fail "unexpected WATCH abort (no WATCH set)"
   | Ok (Some rs) ->
       Alcotest.(check int) "length" 3 (Array.length rs);
       (match rs.(0) with
        | B.One (Ok (Valkey.Resp3.Simple_string "OK")) -> ()
        | _ -> Alcotest.fail "atomic.0: expected SET OK");
       (match rs.(1) with
        | B.One (Ok (Valkey.Resp3.Integer 1L)) -> ()
        | _ -> Alcotest.fail "atomic.1: expected INCR 1");
       (match rs.(2) with
        | B.One (Ok (Valkey.Resp3.Bulk_string "hello")) -> ()
        | _ -> Alcotest.fail "atomic.2: expected GET hello"));

  let _ = C.del client [ k; ctr ] in
  ()

(* Atomic batch: cross-slot rejection at run time via client-side
   validation against the pinned slot. *)
let test_atomic_crossslot_detected () =
  with_cluster_client @@ fun client ->
  let b = B.create ~atomic:true ~hint_key:"{slot-a}x" () in
  let _ = B.queue b [| "GET"; "{slot-a}x" |] in
  let _ = B.queue b [| "GET"; "{slot-b}other" |] in
  match B.run client b with
  | Error (E.Server_error { code = "CROSSSLOT"; _ }) -> ()
  | r ->
      Alcotest.failf
        "expected Server_error CROSSSLOT; got %s"
        (match r with
         | Ok _ -> "Ok"
         | Error e -> Format.asprintf "Error %a" err_pp e)

(* The buffered-atomic [Batch.create ~watch:] cannot reliably
   demonstrate a WATCH abort: WATCH, MULTI, and EXEC are sent
   back-to-back on one connection with no user code in between,
   so there's no window for a rival fiber to dirty the watched
   key. That's the known limitation — real CAS goes through the
   guard API, exercised by [test_watch_guard_aborts_on_rival]. *)

(* Before the per-primary atomic-lock: two fibers running atomic
   batches through the same router would step on each other's
   MULTI ("Command 'multi' not allowed inside a transaction" +
   EXECABORT). With the lock in place, they queue behind each
   other and both commit. *)
let test_atomic_concurrent () =
  with_cluster_client @@ fun client ->
  let a_key = "atomic:race:{a}" in
  let b_key = "atomic:race:{b}" in
  let _ = C.del client [ a_key; b_key ] in

  let run_one ~key =
    let b = B.create ~atomic:true ~hint_key:key () in
    let _ = B.queue b [| "SET"; key; "one" |] in
    let _ = B.queue b [| "INCR"; key ^ ":ctr" |] in
    B.run client b
  in

  let r1 = ref (Ok None) in
  let r2 = ref (Ok None) in
  Eio.Fiber.both
    (fun () -> r1 := run_one ~key:a_key)
    (fun () -> r2 := run_one ~key:b_key);

  (match !r1, !r2 with
   | Ok (Some _), Ok (Some _) -> ()
   | Ok None, _ | _, Ok None ->
       Alcotest.fail "unexpected WATCH abort (no WATCH set)"
   | Error e, _ | _, Error e ->
       Alcotest.failf "concurrent atomic batch failed: %a" err_pp e);

  let _ = C.del client [ a_key; b_key; a_key ^ ":ctr"; b_key ^ ":ctr" ] in
  ()

(* WATCH guard happy path: with_watch opens WATCH, the closure
   reads, builds an atomic batch with the new value, and
   run_with_guard commits. *)
let test_watch_guard_commits () =
  with_cluster_client @@ fun client ->
  let k = "wg:counter:{wg}" in
  let _ = C.set client k "10" in

  let outcome =
    B.with_watch client [ k ] (fun guard ->
      let cur =
        match C.get client k with
        | Ok (Some s) -> int_of_string s
        | _ -> Alcotest.fail "GET counter under guard"
      in
      let b = B.create ~atomic:true ~hint_key:k () in
      let _ = B.queue b [| "SET"; k; string_of_int (cur + 1) |] in
      B.run_with_guard b guard)
  in
  (match outcome with
   | Ok (Ok (Some _)) -> ()
   | Ok (Ok None) -> Alcotest.fail "unexpected WATCH abort (no rival)"
   | Ok (Error e) -> Alcotest.failf "run_with_guard: %a" err_pp e
   | Error e -> Alcotest.failf "with_watch setup: %a" err_pp e);

  (match C.get client k with
   | Ok (Some "11") -> ()
   | Ok v ->
       Alcotest.failf "expected 11; got %s"
         (match v with Some s -> s | None -> "<nil>")
   | Error e -> Alcotest.failf "post-commit GET: %a" err_pp e);

  let _ = C.del client [ k ] in
  ()

(* WATCH guard abort path: rival mutation between watch() and EXEC
   makes EXEC return Null → run_with_guard returns Ok None. The
   caller can then retry the read-modify-write loop. *)
let test_watch_guard_aborts_on_rival () =
  with_cluster_client @@ fun client ->
  let k = "wg:race:{wg}" in
  let _ = C.set client k "v0" in

  let outcome =
    B.with_watch client [ k ] (fun guard ->
      (* Read inside guard so we'd notice the rival *)
      let _ = C.get client k in
      (* Rival mutates the key while we hold the guard.
         The mutex allows non-atomic traffic, so this SET goes
         through on the same primary connection and bumps the
         server-side WATCH dirty flag. *)
      let _ = C.set client k "v-rival" in
      let b = B.create ~atomic:true ~hint_key:k () in
      let _ = B.queue b [| "SET"; k; "v-attempted" |] in
      B.run_with_guard b guard)
  in
  (match outcome with
   | Ok (Ok None) -> ()
   | Ok (Ok (Some _)) ->
       Alcotest.fail "expected WATCH abort but batch committed"
   | Ok (Error e) -> Alcotest.failf "run_with_guard: %a" err_pp e
   | Error e -> Alcotest.failf "with_watch setup: %a" err_pp e);

  (* Verify rival's value won. *)
  (match C.get client k with
   | Ok (Some "v-rival") -> ()
   | Ok v ->
       Alcotest.failf "expected v-rival; got %s"
         (match v with Some s -> s | None -> "<nil>")
   | Error e -> Alcotest.failf "post-abort GET: %a" err_pp e);

  let _ = C.del client [ k ] in
  ()

(* with_watch must release the guard even if the closure raises.
   After the exception bubbles out, a new with_watch on the same
   primary must be able to acquire the mutex (no deadlock) and
   issue commands cleanly. *)
let test_watch_guard_released_on_exception () =
  with_cluster_client @@ fun client ->
  let k = "wg:exn:{wg}" in
  let _ = C.set client k "before" in

  let raised =
    try
      let _ =
        B.with_watch client [ k ] (fun _g ->
          failwith "user-code crashed")
      in
      false
    with Failure _ -> true
  in
  Alcotest.(check bool) "exception propagated" true raised;

  (* If the mutex leaked, this second with_watch would deadlock or
     time out; if WATCH state leaked, the caller's atomic batch
     might fail at EXEC time. Both negative paths are covered by
     the second cycle completing successfully. *)
  let outcome =
    B.with_watch client [ k ] (fun guard ->
      let b = B.create ~atomic:true ~hint_key:k () in
      let _ = B.queue b [| "SET"; k; "after" |] in
      B.run_with_guard b guard)
  in
  (match outcome with
   | Ok (Ok (Some _)) -> ()
   | _ -> Alcotest.fail "second with_watch failed; mutex leaked?");

  (match C.get client k with
   | Ok (Some "after") -> ()
   | _ -> Alcotest.fail "expected 'after' after recovery commit");

  let _ = C.del client [ k ] in
  ()

(* CROSSSLOT validation happens in [watch] before any wire I/O,
   keeping a misuse from acquiring a mutex that would never be
   released. *)
let test_watch_crossslot_rejected () =
  with_cluster_client @@ fun client ->
  let outcome =
    B.with_watch client [ "{slot-a}x"; "{slot-b}y" ] (fun _g ->
      Alcotest.fail "closure should not run on CROSSSLOT keys")
  in
  match outcome with
  | Error (E.Server_error { code = "CROSSSLOT"; _ }) -> ()
  | r ->
      Alcotest.failf "expected CROSSSLOT; got %s"
        (match r with
         | Ok _ -> "Ok"
         | Error e -> Format.asprintf "Error %a" err_pp e)

(* Empty batch under guard is treated as "no write needed":
   UNWATCH is sent and Ok (Some [||]) returned. *)
let test_watch_empty_batch_is_noop () =
  with_cluster_client @@ fun client ->
  let k = "wg:empty:{wg}" in
  let _ = C.set client k "untouched" in
  let outcome =
    B.with_watch client [ k ] (fun guard ->
      let b = B.create ~atomic:true ~hint_key:k () in
      B.run_with_guard b guard)
  in
  (match outcome with
   | Ok (Ok (Some [||])) -> ()
   | Ok (Ok (Some arr)) ->
       Alcotest.failf "expected empty array; got %d entries"
         (Array.length arr)
   | Ok (Ok None) -> Alcotest.fail "unexpected WATCH abort"
   | Ok (Error e) -> Alcotest.failf "run_with_guard: %a" err_pp e
   | Error e -> Alcotest.failf "with_watch setup: %a" err_pp e);

  (* And the key is unchanged. *)
  (match C.get client k with
   | Ok (Some "untouched") -> ()
   | _ -> Alcotest.fail "key was modified by an empty batch?!");

  let _ = C.del client [ k ] in
  ()

let skip_placeholder name () =
  Printf.printf
    "[skipped] %s (cluster unreachable; docker-compose.cluster.yml)\n%!"
    name

let tests =
  let reachable = cluster_reachable () in
  let tc name f =
    if reachable then Alcotest.test_case name `Quick f
    else Alcotest.test_case name `Quick (skip_placeholder name)
  in
  (* The fan-out rejection test is cluster-independent; always run. *)
  [ Alcotest.test_case "atomic batch rejects fan-out at queue time"
      `Quick test_atomic_rejects_fan_out;
    tc "mset / mget / del cluster round-trip (500 keys)"
      test_mset_mget_del_roundtrip;
    tc "heterogeneous batch (GET / SET / INCR / HSET)"
      test_heterogeneous_batch;
    tc "atomic batch commits (SET / INCR / GET)" test_atomic_commits;
    tc "atomic batch CROSSSLOT detected at run time"
      test_atomic_crossslot_detected;
    tc "concurrent atomic batches on same router both commit"
      test_atomic_concurrent;
    tc "watch guard: read-modify-write commits"
      test_watch_guard_commits;
    tc "watch guard: rival mutation aborts run_with_guard"
      test_watch_guard_aborts_on_rival;
    tc "with_watch releases guard on closure exception"
      test_watch_guard_released_on_exception;
    tc "watch rejects cross-slot key set"
      test_watch_crossslot_rejected;
    tc "watch guard: empty batch sends UNWATCH and returns []"
      test_watch_empty_batch_is_noop;
  ]
