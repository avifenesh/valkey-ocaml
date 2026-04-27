(** B8: cache lifecycle under connection/topology events.

    Tests that the B7 flush invariants hold end-to-end against
    a live server:
      - Reconnect-flush: CLIENT KILL forces reconnect; cache clears.
      - Topology-refresh flush: cluster topology change → clear.
      - Cluster failover: primary fails, replica promotes, our
        connections reconnect → cache clears across all shards.

    Standalone tests use :6379. Cluster tests use the compose
    cluster on :7000-7005. Tests skip gracefully when the
    required server isn't up. *)

module C = Valkey.Client
module Cfg = Valkey.Client.Config
module Cache = Valkey.Cache
module Conn = Valkey.Connection
module CR = Valkey.Cluster_router
module R = Valkey.Resp3
module E = Valkey.Connection.Error

let host = "localhost"
let port = 6379

let sleep_ms env ms =
  Eio.Time.sleep (Eio.Stdenv.clock env) (ms /. 1000.0)

(* --- Standalone: reconnect flushes the cache ------------------- *)

(* B7.2 promises: on every successful reconnect the CSC cache is
   cleared, because the server forgot our tracking context while
   we were disconnected. Prove it end-to-end: populate, kill,
   reconnect, assert empty. *)
let test_standalone_reconnect_flushes_cache () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let cache = Cache.create ~byte_budget:(1024 * 1024) in
  let ccfg = Valkey.Client_cache.make ~cache () in
  let client =
    C.connect ~sw ~net ~clock
      ~config:{ Cfg.default with client_cache = Some ccfg }
      ~host ~port ()
  in
  let aux = C.connect ~sw ~net ~clock ~host ~port () in
  let k = "ocaml:csc:lc:reconnect:k" in
  let _ = C.del aux [k] in
  let _ = C.exec aux [| "SET"; k; "v" |] in
  (* Populate our cache. *)
  (match C.get client k with
   | Ok (Some "v") -> ()
   | _ -> Alcotest.fail "initial GET");
  Alcotest.(check bool) "cached before kill" true
    (Option.is_some (Cache.get cache k));
  (* CLIENT KILL self via aux. *)
  let self_id =
    match C.exec client [| "CLIENT"; "ID" |] with
    | Ok (R.Integer id) -> id
    | _ -> Alcotest.fail "CLIENT ID"
  in
  let _ =
    C.exec aux [| "CLIENT"; "KILL"; "ID"; Int64.to_string self_id |]
  in
  sleep_ms env 100.0;
  (* Wake the supervisor so it notices the dead socket and runs
     recovery_loop; PING kicks the cmd path. *)
  let _ = C.exec client [| "PING" |] in
  sleep_ms env 300.0;
  Alcotest.(check int) "cache empty after reconnect" 0 (Cache.count cache);
  (* Sanity: tracking still works — a fresh GET populates again. *)
  (match C.get client k with
   | Ok (Some "v") -> ()
   | _ -> Alcotest.fail "post-reconnect GET");
  Alcotest.(check bool) "cache re-populated post-reconnect" true
    (Option.is_some (Cache.get cache k));
  let _ = C.del aux [k] in
  C.close client;
  C.close aux

(* --- Cluster: failover clears cache --------------------------- *)

(* Expensive test; marked Slow. We populate the CSC cache, then
   trigger a failover from a primary to its replica using
   [CLUSTER FAILOVER FORCE] on the replica. Both our connections
   (old primary + new primary, same wire identity from the client's
   POV if the router re-routes to the promoted replica) bounce;
   the topology-refresh flush (B7.1) OR the per-connection
   reconnect flush (B7.2) clears the cache.

   Falls back to a soft assertion if the test cannot orchestrate
   the failover (e.g. cluster not quorate after promote): we
   assert the cache cleared, but allow either the topology-
   refresh path or the reconnect path to have done it. *)

let seeds = [ "valkey-c1", 7000; "valkey-c2", 7001; "valkey-c3", 7002 ]

let force_skip () =
  try Sys.getenv "VALKEY_CLUSTER" = "skip" with Not_found -> false

let cluster_reachable () =
  if force_skip () then false
  else
    try
      Eio_main.run @@ fun env ->
      Eio.Switch.run @@ fun sw ->
      let net = Eio.Stdenv.net env in
      let clock = Eio.Stdenv.clock env in
      let (host, port) = List.hd seeds in
      let conn =
        Conn.connect ~sw ~net ~clock
          ~config:Conn.Config.default ~host ~port ()
      in
      Conn.close conn;
      true
    with _ -> false

(* Block until the cluster is fully bootstrapped. "Reachable" (a
   single seed accepts a TCP connection) doesn't imply ready —
   we've seen tests fail with "no topology agreement across
   seeds" because the test ran during the gossip-settle window
   right after `docker compose up`. Waits for: cluster_state:ok
   on the first seed, all 16384 slots assigned and ok, AND
   Discovery agreement across seeds (by calling
   Discovery.discover_from_seeds). 30s deadline is generous;
   typical settle is sub-second on loopback. *)
let wait_cluster_ready env =
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let deadline = Eio.Time.now clock +. 30.0 in
  let info_ok host port =
    try
      let conn =
        Conn.connect ~sw ~net ~clock ~config:Conn.Config.default
          ~host ~port ()
      in
      let r = Conn.request conn [| "CLUSTER"; "INFO" |] in
      Conn.close conn;
      match r with
      | Ok (R.Bulk_string s | R.Verbatim_string { data = s; _ }
            | R.Simple_string s) ->
          let has line = String.length s >= String.length line
                         && (let rec scan i =
                               if i + String.length line > String.length s
                               then false
                               else if String.sub s i (String.length line) = line
                               then true
                               else scan (i + 1)
                             in scan 0)
          in
          has "cluster_state:ok\r\n"
          && has "cluster_slots_assigned:16384\r\n"
          && has "cluster_slots_ok:16384\r\n"
      | _ -> false
    with _ -> false
  in
  let discovery_agrees () =
    try
      match
        Valkey.Discovery.discover_from_seeds
          ~sw ~net ~clock
          ~connection_config:Conn.Config.default
          ~agreement_ratio:1.0
          ~min_nodes_for_quorum:(List.length seeds)
          ~seeds ()
      with
      | Ok _ -> true
      | Error _ -> false
    with _ -> false
  in
  let rec wait () =
    if List.for_all (fun (h, p) -> info_ok h p) seeds
       && discovery_agrees ()
    then ()
    else if Eio.Time.now clock >= deadline then
      Alcotest.fail
        "cluster did not reach steady state within 30s — \
         check `docker compose -f docker-compose.cluster.yml ps`"
    else (Eio.Time.sleep clock 0.2; wait ())
  in
  wait ()

let skipped name =
  Printf.printf "    [SKIP] %s (cluster not reachable)\n" name

(* Parsing helpers for CLUSTER NODES output.
   Each line: <id> <host:port@bus[,hostname]> <flags>
              <master-id-or--> <ping> <pong> <epoch> <link> [<slot-range> ...]
   Replica lines have flags containing "slave" / "replica" and a
   non-"-" master-id field. Slot ranges (e.g. "0-5460") only
   appear on master lines. *)

let parse_addr addr_field =
  (* "host:port@bus[,hostname]" -> (host, port) using the first
     comma-separated chunk only (the announced address). *)
  let primary_chunk = List.hd (String.split_on_char ',' addr_field) in
  match String.index_opt primary_chunk ':' with
  | None -> None
  | Some i ->
      let host = String.sub primary_chunk 0 i in
      let rest =
        String.sub primary_chunk (i + 1) (String.length primary_chunk - i - 1)
      in
      let port =
        let at = try String.index rest '@' with Not_found -> String.length rest in
        int_of_string (String.sub rest 0 at)
      in
      Some (host, port)

let line_is_replica flags =
  List.exists (fun f -> f = "slave" || f = "replica")
    (String.split_on_char ',' flags)

let line_owns_slot fields slot =
  (* Master lines list slot ranges as the trailing fields, e.g.
     "0-5460" or "12345" (single). Skip first 8 fixed fields then
     scan. *)
  match fields with
  | _id :: _addr :: _flags :: _master :: _ping :: _pong :: _epoch :: _link
    :: ranges ->
      List.exists
        (fun r ->
          match String.split_on_char '-' r with
          | [ a ] ->
              (try int_of_string a = slot with _ -> false)
          | [ a; b ] ->
              (try
                 let lo = int_of_string a and hi = int_of_string b in
                 lo <= slot && slot <= hi
               with _ -> false)
          | _ -> false)
        ranges
  | _ -> false

let cluster_nodes_text env =
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let h, p = List.hd seeds in
  let conn = Conn.connect ~sw ~net ~clock ~host:h ~port:p () in
  let r = Conn.request conn [| "CLUSTER"; "NODES" |] in
  Conn.close conn;
  match r with
  | Ok (R.Bulk_string s | R.Simple_string s
        | R.Verbatim_string { data = s; _ }) -> s
  | Ok v -> Alcotest.failf "CLUSTER NODES unexpected: %a" R.pp v
  | Error e -> Alcotest.failf "CLUSTER NODES: %a" E.pp e

(* Trigger a topology change by forcing a failover on the shard
   that owns [slot]. Picks the first replica of that slot's
   primary and sends CLUSTER FAILOVER FORCE to it. Returns once
   the cluster has accepted the promotion; gossip + topology
   refresh on the test client are async, so the caller still
   needs a poll loop / sleep before assertions.
   Targeting a SPECIFIC shard (rather than "any first replica")
   is what makes the post-failover MOVED probe deterministic —
   otherwise force_failover may hit a shard that owns neither
   test key and the probes never see MOVED. *)
let force_failover_for_slot env ~slot =
  let nodes = cluster_nodes_text env in
  let lines =
    List.filter (fun l -> String.length l > 0)
      (String.split_on_char '\n' nodes)
  in
  let parsed =
    List.map (fun l -> l, String.split_on_char ' ' l) lines
  in
  let owner_id =
    List.find_map
      (fun (_l, fields) ->
        match fields with
        | id :: _addr :: flags :: _ when not (line_is_replica flags)
                                          && line_owns_slot fields slot ->
            Some id
        | _ -> None)
      parsed
  in
  let owner_id =
    match owner_id with
    | Some id -> id
    | None -> Alcotest.failf "no master owns slot %d" slot
  in
  let replica_ep =
    List.find_map
      (fun (_l, fields) ->
        match fields with
        | _id :: addr :: flags :: master :: _
          when line_is_replica flags && master = owner_id ->
            parse_addr addr
        | _ -> None)
      parsed
  in
  match replica_ep with
  | None ->
      Alcotest.failf
        "no replica found for shard owning slot %d (master id %s)"
        slot owner_id
  | Some (host, port) ->
      Eio.Switch.run @@ fun sw ->
      let net = Eio.Stdenv.net env in
      let clock = Eio.Stdenv.clock env in
      let conn =
        Conn.connect ~sw ~net ~clock ~config:Conn.Config.default
          ~host ~port ()
      in
      let _ =
        Conn.request conn [| "CLUSTER"; "FAILOVER"; "FORCE" |]
      in
      Conn.close conn

let test_cluster_failover_flushes_cache () =
  if not (cluster_reachable ()) then
    skipped "cluster failover flushes cache"
  else
    Eio_main.run @@ fun env ->
    wait_cluster_ready env;
    Eio.Switch.run @@ fun sw ->
    let net = Eio.Stdenv.net env in
    let clock = Eio.Stdenv.clock env in
    let cache = Cache.create ~byte_budget:(1024 * 1024) in
    let ccfg = Valkey.Client_cache.make ~cache () in
    let cluster_cfg =
      let d = CR.Config.default ~seeds in
      { d with connection =
                 { d.connection with client_cache = Some ccfg } }
    in
    let router =
      match CR.create ~sw ~net ~clock ~config:cluster_cfg () with
      | Ok r -> r
      | Error e -> Alcotest.failf "cluster router: %s" e
    in
    let client =
      C.from_router
        ~config:{ Cfg.default with connection = cluster_cfg.connection }
        router
    in
    let aux_router =
      match CR.create ~sw ~net ~clock ~config:(CR.Config.default ~seeds) ()
      with
      | Ok r -> r
      | Error e -> Alcotest.failf "aux router: %s" e
    in
    let aux = C.from_router ~config:Cfg.default aux_router in
    let k_a = "{sharda}:ocaml:csc:lc:failover:a" in
    let k_b = "{shardb}:ocaml:csc:lc:failover:b" in
    let cleanup () =
      List.iter (fun k -> let _ = C.del aux [k] in ()) [k_a; k_b]
    in
    cleanup ();
    let finally () = cleanup (); C.close client; C.close aux in
    Fun.protect ~finally (fun () ->
        let _ = C.exec aux [| "SET"; k_a; "va" |] in
        let _ = C.exec aux [| "SET"; k_b; "vb" |] in
        let _ = C.get client k_a in
        let _ = C.get client k_b in
        Alcotest.(check int) "cache populated before failover" 2
          (Cache.count cache);
        (* Target the failover at k_a's shard so the post-failover
           probe is guaranteed to land on a now-demoted node and
           surface MOVED. *)
        force_failover_for_slot env ~slot:(Valkey.Slot.of_key k_a);
        (* CLUSTER FAILOVER FORCE returns OK immediately, but the
           actual promotion + slot-ownership gossip is async and
           empirically takes 100 ms to several seconds on Linux +
           Docker Engine. Empirically (Valkey 9), the demoted
           primary does NOT drop existing client connections, so
           the per-connection reconnect-flush path doesn't fire
           and the periodic topology-refresh fiber is on a 15 s
           interval. We elicit the topology-change detection the
           way a real workload does: poke each key with a raw
           [exec] (so cached_read can't short-circuit on the hot
           cache); when one of those reads lands on the
           failed-over shard, the OCaml client receives MOVED,
           [trigger_refresh] fires, and the CSC cache is cleared
           eagerly. Loop with a deadline because failover timing
           varies — a single fixed sleep is flaky. *)
        let deadline = Eio.Time.now (Eio.Stdenv.clock env) +. 5.0 in
        let rec poll () =
          (* Per-probe timeout: post-failover the OCaml client may
             still be talking to a node whose reply path stalled
             during the demotion handshake; without an explicit
             timeout, [C.exec] would block indefinitely. *)
          let _ = C.exec ~timeout:0.5 client [| "GET"; k_a |] in
          let _ = C.exec ~timeout:0.5 client [| "GET"; k_b |] in
          if Cache.count cache = 0 then ()
          else if Eio.Time.now (Eio.Stdenv.clock env) >= deadline then ()
          else (sleep_ms env 100.0; poll ())
        in
        poll ();
        Alcotest.(check int) "cache cleared after failover"
          0 (Cache.count cache);
        (* Failback: promote a replica back? Leave cluster in its
           new state; the next test will adapt. Undoing the
           failover cleanly requires more orchestration than is
           worth it for a single test. *)
        ())

(* --- TTL safety net (B9) --------------------------------------- *)

(* With entry_ttl_ms set, a cached entry expires locally even if
   no server invalidation arrives. Proves the safety-net path:
   populate, sleep past TTL without touching the server (no
   external writes, no FLUSHDB), assert the next GET sees a
   miss-then-populate instead of a stale cached hit. *)
let test_ttl_safety_net_expires_without_invalidation () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let cache = Cache.create ~byte_budget:(1024 * 1024) in
  let ccfg =
    Valkey.Client_cache.make ~cache ~entry_ttl_ms:100 ()
  in
  let client =
    C.connect ~sw ~net ~clock
      ~config:{ Cfg.default with client_cache = Some ccfg }
      ~host ~port ()
  in
  let aux = C.connect ~sw ~net ~clock ~host ~port () in
  let k = "ocaml:csc:lc:ttl:k" in
  let _ = C.del aux [k] in
  let _ = C.exec aux [| "SET"; k; "v" |] in
  (match C.get client k with
   | Ok (Some "v") -> ()
   | _ -> Alcotest.fail "initial GET");
  Alcotest.(check bool) "cached" true
    (Option.is_some (Cache.get cache k));
  (* Wait past TTL without any server-side change. *)
  sleep_ms env 200.0;
  Alcotest.(check (option reject)) "cache miss after TTL expiry"
    None (Cache.get cache k);
  let _ = C.del aux [k] in
  C.close client;
  C.close aux

let tests =
  [ Alcotest.test_case "standalone: reconnect flushes cache" `Slow
      test_standalone_reconnect_flushes_cache;
    Alcotest.test_case "cluster: failover flushes cache" `Slow
      test_cluster_failover_flushes_cache;
    Alcotest.test_case "ttl: expires entry without invalidation" `Slow
      test_ttl_safety_net_expires_without_invalidation;
  ]
