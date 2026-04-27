(* Tests for Valkey.Batch: scatter-gather batch primitive and the
   typed cluster-aware helpers built on top.

   Runs against the docker-compose cluster. *)

module C = Valkey.Client
module CR = Valkey.Cluster_router
module B = Valkey.Batch
module E = Valkey.Connection.Error
module Conn = Valkey.Connection
module R = Valkey.Resp3

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

let create_router_retry ~sw ~net ~clock ~config =
  let deadline = Eio.Time.now clock +. 5.0 in
  let rec loop last_error =
    match CR.create ~sw ~net ~clock ~config () with
    | Ok router -> Ok router
    | Error m ->
        if Eio.Time.now clock >= deadline then Error last_error
        else (
          Eio.Time.sleep clock 0.2;
          loop m)
  in
  loop "Cluster_router.create did not run"

let with_cluster_client f =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config =
    { (CR.Config.default ~seeds) with prefer_hostname = true }
  in
  match create_router_retry ~sw ~net ~clock ~config with
  | Error m -> Alcotest.failf "Cluster_router.create: %s" m
  | Ok router ->
      let client = C.from_router ~config:C.Config.default router in
      Fun.protect ~finally:(fun () -> C.close client) @@ fun () -> f client

let with_cluster_client_and_admin ~admin_host:_ ~admin_port:_ f =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config =
    { (CR.Config.default ~seeds) with prefer_hostname = true }
  in
  match create_router_retry ~sw ~net ~clock ~config with
  | Error m -> Alcotest.failf "Cluster_router.create: %s" m
  | Ok router ->
      let client = C.from_router ~config:C.Config.default router in
      (* Build admin as a cluster-aware client so [client_pause]
         fans to every primary instead of pausing a single
         hard-coded node. The pre-failover ~admin_host /
         ~admin_port arguments named one specific primary
         (valkey-c1), which works as long as that node is still
         a primary. After any earlier-running test fires a
         CLUSTER FAILOVER (e.g. csc lifecycle), c1 may be a
         replica — pausing it then pauses the wrong server and
         the timeout assertions don't fire. The arguments are
         kept on the signature for callers' clarity but
         intentionally ignored here. *)
      (match
         create_router_retry ~sw ~net ~clock
           ~config:(CR.Config.default ~seeds)
       with
       | Error m -> Alcotest.failf "admin Cluster_router.create: %s" m
       | Ok admin_router ->
           let admin = C.from_router ~config:C.Config.default admin_router in
           Fun.protect
             ~finally:(fun () -> C.close admin; C.close client)
           @@ fun () ->
           f ~clock client admin)

let err_pp = E.pp

let contains s needle =
  let ls = String.length s
  and ln = String.length needle in
  let rec loop i =
    if i + ln > ls then false
    else if String.sub s i ln = needle then true
    else loop (i + 1)
  in
  if ln = 0 then true else loop 0

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

(* Different slots on the same primary must still be rejected
   client-side in cluster mode. Before the fix, this slipped
   through to the server because the validation compared pooled
   connections instead of hash slots. *)
let test_atomic_same_primary_crossslot_detected_client_side () =
  with_cluster_client @@ fun client ->
  let key_a = "alpha" in
  let key_b = "gamma" in
  let slot_a = Valkey.Slot.of_key key_a in
  let slot_b = Valkey.Slot.of_key key_b in
  Alcotest.(check bool) "different slots" true (slot_a <> slot_b);
  Alcotest.(check bool) "same c1 shard"
    true (slot_a <= 5460 && slot_b <= 5460);
  let b = B.create ~atomic:true ~hint_key:key_a () in
  let _ = B.queue b [| "GET"; key_a |] in
  let _ = B.queue b [| "GET"; key_b |] in
  match B.run client b with
  | Error (E.Server_error { code = "CROSSSLOT"; message }) ->
      if not (contains message "batch is pinned to slot") then
        Alcotest.failf
          "expected client-side CROSSSLOT message, got %S" message
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

let test_atomic_timeout_respected () =
  with_cluster_client_and_admin
    ~admin_host:"valkey-c1" ~admin_port:7000
  @@ fun ~clock client admin ->
  let key = "alpha" in
  let _ = C.set client key "before-timeout" in
  let b = B.create ~atomic:true ~hint_key:key () in
  let _ = B.queue b [| "GET"; key |] in
  (match C.client_pause admin ~timeout_ms:200 with
   | Ok () -> ()
   | Error e -> Alcotest.failf "CLIENT PAUSE: %a" err_pp e);
  Eio.Time.sleep clock 0.01;
  (match B.run ~timeout:0.05 client b with
   | Error E.Timeout -> ()
   | Ok _ -> Alcotest.fail "expected atomic batch Timeout"
   | Error e -> Alcotest.failf "expected Timeout, got %a" err_pp e);
  Eio.Time.sleep clock 0.25;
  (match C.get client key with
   | Ok (Some "before-timeout") -> ()
   | Ok v ->
       Alcotest.failf "expected before-timeout, got %s"
         (match v with Some s -> s | None -> "<nil>")
   | Error e -> Alcotest.failf "GET after timeout: %a" err_pp e);
  ignore (C.del client [ key ])

let test_watch_guard_timeout_respected () =
  with_cluster_client_and_admin
    ~admin_host:"valkey-c1" ~admin_port:7000
  @@ fun ~clock client admin ->
  let key = "alpha" in
  let _ = C.set client key "before-guard-timeout" in
  let outcome =
    B.with_watch client [ key ] (fun guard ->
      let b = B.create ~atomic:true ~hint_key:key () in
      let _ = B.queue b [| "SET"; key; "after-guard-timeout" |] in
      (match C.client_pause admin ~timeout_ms:200 with
       | Ok () -> ()
       | Error e -> Alcotest.failf "CLIENT PAUSE: %a" err_pp e);
      Eio.Time.sleep clock 0.01;
      B.run_with_guard ~timeout:0.05 b guard)
  in
  (match outcome with
   | Ok (Error E.Timeout) -> ()
   | Ok (Ok _) ->
       Alcotest.fail "expected guard commit path to time out"
   | Ok (Error e) ->
       Alcotest.failf "expected Timeout, got %a" err_pp e
   | Error e ->
       Alcotest.failf "with_watch setup failed: %a" err_pp e);
  Eio.Time.sleep clock 0.25;
  (match C.get client key with
   | Ok (Some "before-guard-timeout") -> ()
   | Ok v ->
       Alcotest.failf "expected before-guard-timeout, got %s"
         (match v with Some s -> s | None -> "<nil>")
   | Error e -> Alcotest.failf "GET after guard timeout: %a" err_pp e);
  ignore (C.del client [ key ])

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

(* pfcount_cluster: union cardinality across HLLs spread across
   multiple slots. With overlapping membership we'd expect
   ~3000 uniques despite the per-slot sum being 4500. *)
let test_pfcount_cluster_crossslot () =
  with_cluster_client @@ fun client ->
  let k1 = "pfc:a" in
  let k2 = "pfc:b" in
  let k3 = "pfc:c" in
  let _ = C.del client [ k1; k2; k3 ] in
  (* Seed three HLLs with overlap — 3000 distinct items total
     across the three, each HLL carrying 1500 items with 1000
     shared with its neighbour. *)
  let add_range k lo hi =
    let rec loop i =
      if i > hi then ()
      else begin
        let _ =
          C.pfadd client k
            ~elements:[ Printf.sprintf "item-%d" i ]
        in
        loop (i + 1)
      end
    in
    loop lo
  in
  add_range k1 0 1499;       (* 0..1499 *)
  add_range k2 500 1999;     (* 500..1999 (overlaps k1 by 1000) *)
  add_range k3 1000 2499;    (* 1000..2499 (overlaps k2 by 1000) *)
  (* Make sure at least two keys live on different slots — if
     they happened to collide we'd be testing the single-slot
     fast path, which is fine but misses the point. *)
  let slots =
    List.sort_uniq compare
      (List.map (fun k -> Valkey.Slot.of_key k) [ k1; k2; k3 ])
  in
  Alcotest.(check bool)
    "test keys hit multiple slots" true
    (List.length slots > 1);

  (match B.pfcount_cluster client [ k1; k2; k3 ] with
   | Error e -> Alcotest.failf "pfcount_cluster: %a" err_pp e
   | Ok n ->
       (* True union is 2500 unique items. HLL has ~0.81% relative
          error so we allow ±5% wiggle. *)
       if n < 2375 || n > 2625 then
         Alcotest.failf
           "expected ~2500 (true union), got %d" n);

  let _ = C.del client [ k1; k2; k3 ] in
  ()

(* Missing inputs contribute nothing, matching PFCOUNT's
   nonexistent-key-is-empty-HLL semantics. *)
let test_pfcount_cluster_missing_keys_ok () =
  with_cluster_client @@ fun client ->
  let _ =
    C.del client
      [ "pfc:missing:a"; "pfc:missing:b"; "pfc:missing:c" ]
  in
  (match
     B.pfcount_cluster client
       [ "pfc:missing:a"; "pfc:missing:b"; "pfc:missing:c" ]
   with
   | Ok 0 -> ()
   | Ok n ->
       Alcotest.failf "expected 0 for all-missing; got %d" n
   | Error e -> Alcotest.failf "pfcount_cluster: %a" err_pp e)

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

(* Minimal CLUSTER NODES parser: extract (host, port) of the first
   replica. Format: "<id> <host>:<port>@<bus>[,hostname] <flags>
   ...". Used to find a replica we can force-promote. *)
let pick_a_replica nodes_text =
  let lines = String.split_on_char '\n' nodes_text in
  List.find_map
    (fun line ->
      if String.length line = 0 then None
      else
        match String.split_on_char ' ' line with
        | _id :: addr :: flags :: _ ->
            let is_replica =
              List.exists (fun f -> f = "slave" || f = "replica")
                (String.split_on_char ',' flags)
            in
            if not is_replica then None
            else
              let addr = List.hd (String.split_on_char ',' addr) in
              (match String.index_opt addr ':' with
               | None -> None
               | Some i ->
                   let host = String.sub addr 0 i in
                   let rest =
                     String.sub addr (i + 1) (String.length addr - i - 1)
                   in
                   let port =
                     let at =
                       try String.index rest '@'
                       with Not_found -> String.length rest
                     in
                     int_of_string (String.sub rest 0 at)
                   in
                   Some (host, port))
        | _ -> None)
    lines

(* WATCH guard + topology change mid-flight.
   Opens a WATCH guard on a key, force-promotes a replica to flip
   the cluster's primary set, then attempts run_with_guard. The
   connection holding the guard is now talking to a former primary
   (now a replica), so EXEC must fail in some bounded way: either
   [Ok None] (treated as a WATCH abort, caller retries) or
   [Ok (Some _)] (transparent retry on the new primary succeeded).
   The tested wire-level invariant: a slot ownership change between
   WATCH and EXEC must NOT leak as [Error (Server_error MOVED)] to
   the caller — that's a foot-gun the user can't realistically
   distinguish from a "real" server error.
   STATUS.md previously deferred this scenario as "slot-migration
   under load"; it's exercised here. *)
(* Snapshot of (node-id, role) tuples from CLUSTER NODES, used to
   verify the failover actually flipped a primary↔replica. *)
let role_set nodes_text =
  String.split_on_char '\n' nodes_text
  |> List.filter_map (fun line ->
       if String.length line = 0 then None
       else match String.split_on_char ' ' line with
         | id :: _addr :: flags :: _ ->
             let role =
               if List.exists (fun f -> f = "master")
                    (String.split_on_char ',' flags)
               then "master"
               else if List.exists
                         (fun f -> f = "slave" || f = "replica")
                         (String.split_on_char ',' flags)
               then "replica"
               else "other"
             in
             Some (id, role)
         | _ -> None)
  |> List.sort compare

let test_watch_guard_under_failover () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config =
    { (CR.Config.default ~seeds) with prefer_hostname = true }
  in
  match create_router_retry ~sw ~net ~clock ~config with
  | Error m -> Alcotest.failf "Cluster_router.create: %s" m
  | Ok router ->
      let client = C.from_router ~config:C.Config.default router in
      Fun.protect ~finally:(fun () -> C.close client) @@ fun () ->
      let cluster_nodes_text () =
        let (h, p) = List.hd seeds in
        let conn =
          Conn.connect ~sw ~net ~clock ~config:Conn.Config.default
            ~host:h ~port:p ()
        in
        let r = Conn.request conn [| "CLUSTER"; "NODES" |] in
        Conn.close conn;
        match r with
        | Ok (R.Bulk_string s | R.Simple_string s
              | R.Verbatim_string { data = s; _ }) -> s
        | _ -> Alcotest.fail "CLUSTER NODES failed"
      in
      let roles_before = role_set (cluster_nodes_text ()) in
      let k = "wg:failover:{wgf}" in
      let _ = C.set client k "v0" in
      let outcome =
        B.with_watch client [ k ] (fun guard ->
          (* Force topology change mid-guard. Issue CLUSTER FAILOVER
             FORCE on a replica (only replicas accept it). *)
          (match pick_a_replica (cluster_nodes_text ()) with
           | None -> Alcotest.fail "no replica found in CLUSTER NODES"
           | Some (host, port) ->
               let replica_conn =
                 Conn.connect ~sw ~net ~clock
                   ~config:Conn.Config.default ~host ~port ()
               in
               let _ =
                 Conn.request replica_conn
                   [| "CLUSTER"; "FAILOVER"; "FORCE" |]
               in
               Conn.close replica_conn);
          (* Failover takes ~50–500ms; sleep so the EXEC inside
             run_with_guard hits the post-failover state. *)
          Eio.Time.sleep clock 0.5;
          let b = B.create ~atomic:true ~hint_key:k () in
          let _ = B.queue b [| "SET"; k; "v-after-failover" |] in
          B.run_with_guard b guard)
      in
      (* Verify the failover actually happened — without this, a
         silent FAILOVER no-op (e.g. replica already primary, or
         cluster busy) would let the test pass on stale topology
         and prove nothing about WATCH+EXEC under flip. *)
      let roles_after = role_set (cluster_nodes_text ()) in
      if roles_before = roles_after then
        Alcotest.fail
          "CLUSTER FAILOVER FORCE produced no role change; test ran \
           on unchanged topology and proves nothing — investigate \
           cluster state, not the test failure";
      (match outcome with
       | Ok (Ok None) -> ()                       (* WATCH abort: clean *)
       | Ok (Ok (Some _)) -> ()                   (* transparent retry *)
       | Ok (Error e) ->
           Alcotest.failf
             "WATCH+EXEC under failover leaked transport error: %a"
             err_pp e
       | Error e ->
           Alcotest.failf
             "with_watch under failover failed at setup: %a"
             err_pp e);
      let _ = C.del client [ k ] in
      ()

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
    tc "atomic batch same-primary CROSSSLOT detected client-side"
      test_atomic_same_primary_crossslot_detected_client_side;
    tc "concurrent atomic batches on same router both commit"
      test_atomic_concurrent;
    tc "atomic batch timeout is respected"
      test_atomic_timeout_respected;
    tc "watch guard: read-modify-write commits"
      test_watch_guard_commits;
    tc "watch guard: rival mutation aborts run_with_guard"
      test_watch_guard_aborts_on_rival;
    tc "watch guard: timeout is respected"
      test_watch_guard_timeout_respected;
    tc "with_watch releases guard on closure exception"
      test_watch_guard_released_on_exception;
    tc "watch rejects cross-slot key set"
      test_watch_crossslot_rejected;
    tc "watch guard: empty batch sends UNWATCH and returns []"
      test_watch_empty_batch_is_noop;
    tc "pfcount_cluster union across slots within error bounds"
      test_pfcount_cluster_crossslot;
    tc "pfcount_cluster all-missing returns 0"
      test_pfcount_cluster_missing_keys_ok;
    (* Forces a CLUSTER FAILOVER mid-WATCH; runs last among cluster
       tests so its topology change doesn't perturb sibling tests. *)
    tc "watch guard: topology change between WATCH and EXEC"
      test_watch_guard_under_failover;
  ]
