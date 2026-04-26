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

let skipped name =
  Printf.printf "    [SKIP] %s (cluster not reachable)\n" name

(* Trigger a topology change by forcing a failover on any one
   shard. Returns once the cluster has accepted the promotion;
   the CSC client's refresh fiber will pick up the new topology
   asynchronously. *)
let force_failover env =
  (* Use a short-lived aux cluster client to orchestrate. *)
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let aux_cfg = CR.Config.default ~seeds in
  let aux_router =
    match CR.create ~sw ~net ~clock ~config:aux_cfg () with
    | Ok r -> r
    | Error e -> Alcotest.failf "aux router: %s" e
  in
  let aux = C.from_router ~config:Cfg.default aux_router in
  (* Find a replica and tell it to take over its shard. *)
  let reply = C.exec aux [| "CLUSTER"; "NODES" |] in
  C.close aux;
  match reply with
  | Error e -> Alcotest.failf "CLUSTER NODES: %a" E.pp e
  | Ok (R.Bulk_string s | R.Simple_string s
        | R.Verbatim_string { data = s; _ }) ->
      (* Parse for a "replica" line's address. Format varies;
         grab the first field with "slave" or "replica" role
         and extract its host:port@bus from field 2. *)
      let lines = String.split_on_char '\n' s in
      let replica_ep =
        List.find_map
          (fun line ->
            if String.length line = 0 then None
            else
              let fields = String.split_on_char ' ' line in
              match fields with
              | _id :: addr :: flags :: _ ->
                  (* [flags] is comma-separated. [addr] is
                     host:port@bus,hostname. Extract host + port
                     from the [addr] field. *)
                  let is_replica =
                    List.exists
                      (fun f -> f = "slave" || f = "replica")
                      (String.split_on_char ',' flags)
                  in
                  if not is_replica then None
                  else
                    (* addr = "host:port@bus[,hostname]" *)
                    let addr = List.hd (String.split_on_char ',' addr) in
                    (match String.index_opt addr ':' with
                     | None -> None
                     | Some i ->
                         let host = String.sub addr 0 i in
                         let rest =
                           String.sub addr (i + 1)
                             (String.length addr - i - 1)
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
      in
      (match replica_ep with
       | None ->
           Alcotest.fail "could not find a replica in CLUSTER NODES"
       | Some (host, port) ->
           (* Open a dedicated Connection to that replica and tell it
              to promote with FORCE. *)
           let conn =
             Conn.connect ~sw ~net ~clock
               ~config:Conn.Config.default ~host ~port ()
           in
           let _ = Conn.request conn [| "CLUSTER"; "FAILOVER"; "FORCE" |] in
           Conn.close conn)
  | Ok v -> Alcotest.failf "unexpected CLUSTER NODES reply %a" R.pp v

let test_cluster_failover_flushes_cache () =
  if not (cluster_reachable ()) then
    skipped "cluster failover flushes cache"
  else
    Eio_main.run @@ fun env ->
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
        force_failover env;
        (* Give the refresh fiber + reconnect fibers a couple of
           seconds to notice and reset. *)
        sleep_ms env 3000.0;
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
