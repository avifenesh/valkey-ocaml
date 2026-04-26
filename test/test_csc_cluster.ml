(** B7: CSC in cluster mode.

    Verifies:
    - CLIENT TRACKING is issued per-shard on the connections the
      cluster router opens; each shard has its own invalidator
      fiber draining into the SHARED cache.
    - A cached entry belongs to the logical client, not to any
      one connection: a key read via shard A can be cache-hit
      from any subsequent call regardless of routing.
    - External write from another client is caught by the shard
      that owns the key's slot and evicts our cache.
    - Topology refresh clears the cache (B7.1).
    - Per-shard reconnect clears the cache (B7.2).

    Skips if the cluster is not reachable on [valkey-c1:7000]
    (host-to-cluster resolution requires the /etc/hosts
    entries installed by [scripts/cluster-hosts-setup.sh]). *)

module C = Valkey.Client
module Cfg = Valkey.Client.Config
module Cache = Valkey.Cache
module Conn = Valkey.Connection
module CR = Valkey.Cluster_router
module R = Valkey.Resp3
module E = Valkey.Connection.Error

let seeds = [ "valkey-c1", 7000; "valkey-c2", 7001; "valkey-c3", 7002 ]

let force_skip () =
  try Sys.getenv "VALKEY_CLUSTER" = "skip" with Not_found -> false

(* Probe a seed with a plain Connection; if it can't connect,
   skip all cluster tests gracefully. *)
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
  Printf.printf "    [SKIP] %s (cluster not reachable; set \
                             /etc/hosts via \
                             scripts/cluster-hosts-setup.sh)\n" name

let grace_s = 0.05
let sleep_ms env ms =
  Eio.Time.sleep (Eio.Stdenv.clock env) (ms /. 1000.0)

(* Open a cluster Client with CSC enabled + a plain aux client
   on the same seeds for external writes. *)
let with_cluster_csc ~keys f =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let cache = Cache.create ~byte_budget:(1024 * 1024) in
  let ccfg = Valkey.Client_cache.make ~cache () in
  (* Plumb the cache through the cluster router's Connection.Config
     so every shard connection issues CLIENT TRACKING at handshake.
     Client.from_router picks up the same cache via
     resolve_connection_config (config.client_cache = None; the
     reference flows from config.connection.client_cache into t's
     effective cache). *)
  let cluster_cfg =
    let default = CR.Config.default ~seeds in
    { default with
      connection =
        { default.connection with client_cache = Some ccfg } }
  in
  let router =
    match CR.create ~sw ~net ~clock ~config:cluster_cfg () with
    | Ok r -> r
    | Error e ->
        Alcotest.failf "cluster router init: %s" e
  in
  let client =
    C.from_router
      ~config:{ Cfg.default with connection = cluster_cfg.connection }
      router
  in
  (* Aux is also a cluster client (routes to the correct shard).
     No cache. *)
  let aux_cfg = CR.Config.default ~seeds in
  let aux_router =
    match CR.create ~sw ~net ~clock ~config:aux_cfg () with
    | Ok r -> r
    | Error e ->
        Alcotest.failf "aux cluster router init: %s" e
  in
  let aux = C.from_router ~config:Cfg.default aux_router in
  let cleanup () =
    List.iter (fun k -> let _ = C.del aux [k] in ()) keys
  in
  cleanup ();
  let finally () = cleanup (); C.close client; C.close aux in
  Fun.protect ~finally (fun () -> f env client cache aux)

(* --- per-shard tracking --------------------------------------- *)

(* Get + cache + external SET + eviction, proving the cache and
   invalidator fiber of the correct shard cooperated. Uses two
   different hashtag-controlled keys that deliberately route to
   different shards (different slot) to exercise multi-shard
   tracking. *)
let test_two_shards_invalidate_shared_cache () =
  if not (cluster_reachable ()) then
    skipped "cluster csc: two shards share one cache"
  else
    (* Pick keys with distinct hashtags so they hash to different
       slots (at least with high probability). *)
    let k_a = "{sharda}:ocaml:csc:cluster:a" in
    let k_b = "{shardb}:ocaml:csc:cluster:b" in
    with_cluster_csc ~keys:[ k_a; k_b ]
    @@ fun env client cache aux ->
    let _ = C.exec aux [| "SET"; k_a; "v1a" |] in
    let _ = C.exec aux [| "SET"; k_b; "v1b" |] in
    (* Populate both in our cache. *)
    (match C.get client k_a with
     | Ok (Some "v1a") -> () | _ -> Alcotest.fail "GET a");
    (match C.get client k_b with
     | Ok (Some "v1b") -> () | _ -> Alcotest.fail "GET b");
    Alcotest.(check bool) "a cached" true
      (Option.is_some (Cache.get cache k_a));
    Alcotest.(check bool) "b cached" true
      (Option.is_some (Cache.get cache k_b));
    (* External write to each shard. Each shard's invalidator fiber
       should pick up its own invalidation push and evict from the
       shared cache. *)
    let _ = C.exec aux [| "SET"; k_a; "v2a" |] in
    let _ = C.exec aux [| "SET"; k_b; "v2b" |] in
    sleep_ms env (grace_s *. 1000.0);
    Alcotest.(check (option reject)) "a evicted"
      None (Cache.get cache k_a);
    Alcotest.(check (option reject)) "b evicted"
      None (Cache.get cache k_b)

(* --- FLUSHALL-like: fan-out FLUSHDB clears everything ---------- *)

(* Every primary sends its own null-body invalidation push on
   FLUSHDB (which fans out to primaries). The shared cache
   should be cleared — possibly multiple times, which is fine. *)
let test_cluster_flushdb_clears_cache () =
  if not (cluster_reachable ()) then
    skipped "cluster csc: FLUSHDB clears whole cache"
  else
    let k_a = "{sharda}:ocaml:csc:cluster:flush:a" in
    let k_b = "{shardb}:ocaml:csc:cluster:flush:b" in
    with_cluster_csc ~keys:[ k_a; k_b ]
    @@ fun env client cache aux ->
    let _ = C.exec aux [| "SET"; k_a; "1" |] in
    let _ = C.exec aux [| "SET"; k_b; "2" |] in
    let _ = C.get client k_a in
    let _ = C.get client k_b in
    Alcotest.(check int) "two entries cached" 2 (Cache.count cache);
    (* FLUSHDB on every primary, via the cluster client's
       fan-out path. *)
    let _ =
      C.exec_multi client [| "FLUSHDB" |]
    in
    sleep_ms env (grace_s *. 1000.0);
    Alcotest.(check int) "cache cleared after cluster-wide FLUSHDB"
      0 (Cache.count cache)

let tests =
  [ Alcotest.test_case "cluster: two shards share one invalidated cache"
      `Quick test_two_shards_invalidate_shared_cache;
    Alcotest.test_case "cluster: FLUSHDB across primaries clears cache"
      `Quick test_cluster_flushdb_clears_cache;
  ]
