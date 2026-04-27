(** B2.5 cluster: OPTIN against a real cluster.

    Standalone OPTIN is covered by [test_csc_optin.ml]. This file
    exercises the cluster-specific paths: the [CLIENT CACHING YES
    + read] pair travels through {!Cluster_router}'s redirect-aware
    retry, so a slot-move between the two frames re-submits the
    whole pair on the new owner.

    Requires the docker-compose cluster
    ([docker compose -f docker-compose.cluster.yml up -d]). *)

module C = Valkey.Client
module Cfg = Valkey.Client.Config
module Cache = Valkey.Cache
module CC = Valkey.Client_cache
module CR = Valkey.Cluster_router
module Conn = Valkey.Connection
module E = Valkey.Connection.Error
module R = Valkey.Resp3

let seeds = [ "valkey-c1", 7000; "valkey-c2", 7001; "valkey-c3", 7002 ]
let grace_s = 0.05

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
      let h, p = List.hd seeds in
      let conn =
        Conn.connect ~sw ~net ~clock ~config:Conn.Config.default
          ~host:h ~port:p ()
      in
      Conn.close conn;
      true
    with _ -> false

let skipped name =
  Printf.printf "    [SKIP] %s (cluster not reachable)\n" name

let sleep_ms env ms =
  Eio.Time.sleep (Eio.Stdenv.clock env) (ms /. 1000.0)

let with_optin_cluster ~keys f =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let cache = Cache.create ~byte_budget:(1024 * 1024) in
  let ccfg = CC.make ~cache ~mode:CC.Optin () in
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
  let cleanup () = List.iter (fun k -> let _ = C.del aux [k] in ()) keys in
  cleanup ();
  let finally () = cleanup (); C.close client; C.close aux in
  Fun.protect ~finally (fun () -> f env client cache aux)

(* OPTIN cluster smoke: two keys in different shards (different
   hashtags), both get cached on first read, both get invalidated
   when an external SET fires from aux. The load-bearing
   property: each shard's connection sees its own
   [CLIENT CACHING YES + read] pair, the server registers tracking
   on each shard's primary, and per-shard invalidation pushes
   reach our shared [Cache.t] via the per-shard invalidator
   fibers. *)
let test_two_shards_populate_and_evict () =
  if not (cluster_reachable ()) then
    skipped "cluster: OPTIN populate + cross-shard evict"
  else
    let k_a = "{sharda}:ocaml:csc:optin:cluster:a" in
    let k_b = "{shardb}:ocaml:csc:optin:cluster:b" in
    with_optin_cluster ~keys:[k_a; k_b] @@ fun env client cache aux ->
    let _ = C.exec aux [| "SET"; k_a; "va" |] in
    let _ = C.exec aux [| "SET"; k_b; "vb" |] in
    (match C.get client k_a with
     | Ok (Some "va") -> ()
     | other ->
         Alcotest.failf "GET k_a: expected Some va, got %s"
           (match other with
            | Ok None -> "None"
            | Ok (Some s) -> Printf.sprintf "Some %S" s
            | Error e -> Format.asprintf "Error %a" E.pp e));
    (match C.get client k_b with
     | Ok (Some "vb") -> ()
     | other ->
         Alcotest.failf "GET k_b: expected Some vb, got %s"
           (match other with
            | Ok None -> "None"
            | Ok (Some s) -> Printf.sprintf "Some %S" s
            | Error e -> Format.asprintf "Error %a" E.pp e));
    Alcotest.(check int) "both shards' entries cached" 2
      (Cache.count cache);
    let _ = C.exec aux [| "SET"; k_a; "va2" |] in
    let _ = C.exec aux [| "SET"; k_b; "vb2" |] in
    sleep_ms env (grace_s *. 4.0 *. 1000.0);
    (* Both invalidations propagated; cache is empty until next
       read. If OPTIN's [CACHING YES + read] pair didn't actually
       arrive together on either shard's connection, that shard's
       primary wouldn't have registered tracking, no inv would
       arrive, and that shard's cached entry would stick. *)
    Alcotest.(check int) "both shard entries invalidated" 0
      (Cache.count cache)

(* Pair-on-cluster smoke: 25 fibers each issue an OPTIN GET on a
   distinct hashtag-anchored key spread across the 3 shards;
   external SETs on every key from aux must fire 25 invalidations.
   Detects any wire-adjacency break in the pair under concurrent
   per-shard enqueueing — a broken pair leaves at least one read
   untracked, no inv arrives for it, and that entry sticks. *)
let test_concurrent_optin_cluster () =
  if not (cluster_reachable ()) then
    skipped "cluster: OPTIN 25-fiber concurrent tracking"
  else
    let n = 25 in
    let keys =
      List.init n (fun i ->
        let tag =
          match i mod 3 with
          | 0 -> "sharda" | 1 -> "shardb" | _ -> "shardc"
        in
        Printf.sprintf "{%s}:ocaml:csc:optin:cluster:c:%d" tag i)
    in
    with_optin_cluster ~keys @@ fun env client cache aux ->
    List.iter (fun k -> let _ = C.exec aux [| "SET"; k; "v0" |] in ()) keys;
    Eio.Fiber.List.iter
      (fun k ->
        match C.get client k with
        | Ok (Some "v0") -> ()
        | other ->
            Alcotest.failf "concurrent OPTIN GET %S: %s" k
              (match other with
               | Ok None -> "None"
               | Ok (Some s) -> Printf.sprintf "Some %S" s
               | Error e -> Format.asprintf "Error %a" E.pp e))
      keys;
    Alcotest.(check int) "all keys cached" n (Cache.count cache);
    List.iter (fun k -> let _ = C.exec aux [| "SET"; k; "v1" |] in ()) keys;
    let drained =
      let clock = Eio.Stdenv.clock env in
      let deadline = Eio.Time.now clock +. 3.0 in
      let rec loop () =
        if Cache.count cache = 0 then true
        else if Eio.Time.now clock >= deadline then false
        else (sleep_ms env 20.0; loop ())
      in
      loop ()
    in
    if not drained then
      Alcotest.failf
        "expected all %d concurrent OPTIN entries to be \
         invalidated within 3s; %d still cached"
        n (Cache.count cache)

(* Helpers to discover a primary that does NOT own a given slot,
   so we can elicit MOVED on purpose without orchestrating real
   failover (which leaves cleanup-time refresh state pending and
   hangs the switch). *)

let cluster_nodes_text env =
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let h, p = List.hd seeds in
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

let parse_nodes nodes_text =
  String.split_on_char '\n' nodes_text
  |> List.filter (fun l -> String.length l > 0)
  |> List.map (fun l -> String.split_on_char ' ' l)

let line_owns_slot fields slot =
  match fields with
  | _id :: _addr :: _flags :: _master :: _ping :: _pong :: _epoch :: _link
    :: ranges ->
      List.exists
        (fun r ->
          match String.split_on_char '-' r with
          | [ a ] -> (try int_of_string a = slot with _ -> false)
          | [ a; b ] ->
              (try
                 let lo = int_of_string a and hi = int_of_string b in
                 lo <= slot && slot <= hi
               with _ -> false)
          | _ -> false)
        ranges
  | _ -> false

let line_is_master flags =
  List.exists (fun f -> f = "master")
    (String.split_on_char ',' flags)

let primary_owning_slot env ~slot =
  let parsed = parse_nodes (cluster_nodes_text env) in
  List.find_map
    (fun fields ->
      match fields with
      | id :: _addr :: flags :: _ ->
          if line_is_master flags && line_owns_slot fields slot
          then Some id else None
      | _ -> None)
    parsed

let other_primary env ~not_id =
  let parsed = parse_nodes (cluster_nodes_text env) in
  List.find_map
    (fun fields ->
      match fields with
      | id :: _addr :: flags :: _ ->
          if line_is_master flags && id <> not_id
          then Some id else None
      | _ -> None)
    parsed

(* Synthetic-MOVED OPTIN test: routes an OPTIN read at a primary
   that doesn't own the key's slot. The wrong primary returns
   MOVED on the read frame; [Cluster_router.make_pair]'s retry
   then re-submits the WHOLE pair on the correct owner's
   connection (CACHING YES stays adjacent to the read across the
   redirect). The retry must:

   1. Surface as a successful value to the caller (NOT
      Server_error MOVED — that's the foot-gun the retry exists
      to prevent).
   2. Trigger trigger_refresh which eagerly clears the CSC cache
      (since topology disagrees about slot ownership at MOVED
      time — the routing-target primary doesn't match the
      atomic topology's view).

   Unlike the failover-based test (which hung in cleanup because
   trigger_refresh fired apply_new_topology mid-teardown,
   spawning new connections under the outer switch), this test
   doesn't perturb the cluster — the topology is unchanged, only
   our routing was wrong — so refresh's apply_new_topology is a
   no-op and cleanup is clean. *)
let test_optin_moved_retry () =
  if not (cluster_reachable ()) then
    skipped "cluster: OPTIN MOVED-retry on wrong-target routing"
  else
    Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
    let net = Eio.Stdenv.net env in
    let clock = Eio.Stdenv.clock env in
    let cache = Cache.create ~byte_budget:(1024 * 1024) in
    let ccfg = CC.make ~cache ~mode:CC.Optin () in
    let cluster_cfg =
      let d = CR.Config.default ~seeds in
      { d with connection =
                 { d.connection with client_cache = Some ccfg } }
    in
    let router =
      match CR.create ~sw ~net ~clock ~config:cluster_cfg () with
      | Ok r -> r
      | Error e -> Alcotest.failf "router: %s" e
    in
    let aux_router =
      match CR.create ~sw ~net ~clock ~config:(CR.Config.default ~seeds) ()
      with
      | Ok r -> r
      | Error e -> Alcotest.failf "aux router: %s" e
    in
    let aux = C.from_router ~config:Cfg.default aux_router in
    let client =
      C.from_router
        ~config:{ Cfg.default with connection = cluster_cfg.connection }
        router
    in
    let k = "{sharda}:ocaml:csc:optin:moved" in
    let cleanup () = let _ = C.del aux [k] in () in
    cleanup ();
    let finally () = cleanup (); C.close client; C.close aux in
    Fun.protect ~finally @@ fun () ->
    let _ = C.exec aux [| "SET"; k; "v0" |] in
    (* Prime cache via OPTIN's normal routed path — populates the
       Cache.t entry that the MOVED-retry's eager-clear should
       drop. *)
    (match C.get client k with
     | Ok (Some "v0") -> ()
     | other ->
         Alcotest.failf "prime GET k: %s"
           (match other with
            | Ok None -> "None"
            | Ok (Some s) -> Printf.sprintf "Some %S" s
            | Error e -> Format.asprintf "Error %a" E.pp e));
    Alcotest.(check int) "primed cache before MOVED-retry"
      1 (Cache.count cache);
    let slot = Valkey.Slot.of_key k in
    let owner_id =
      match primary_owning_slot env ~slot with
      | Some id -> id
      | None -> Alcotest.failf "no primary owns slot %d" slot
    in
    let wrong_id =
      match other_primary env ~not_id:owner_id with
      | Some id -> id
      | None -> Alcotest.fail "no other primary"
    in
    (* Force the OPTIN pair at the wrong primary via Router.pair
       directly. Server returns MOVED on the read frame;
       make_pair retries on the correct owner. *)
    let result =
      Valkey.Router.pair ~timeout:1.0 router
        (Valkey.Router.Target.By_node wrong_id)
        [| "CLIENT"; "CACHING"; "YES" |]
        [| "GET"; k |]
    in
    (match result with
     | Ok (Ok (R.Simple_string "OK"), Ok (R.Bulk_string "v0")) -> ()
     | other ->
         let pp_pair = function
           | Ok ((Ok r1), (Ok r2)) ->
               Format.asprintf "Ok (Ok %a, Ok %a)" R.pp r1 R.pp r2
           | Ok ((Ok r1), (Error e)) ->
               Format.asprintf "Ok (Ok %a, Err %a)" R.pp r1 E.pp e
           | Ok ((Error e), _) ->
               Format.asprintf "Ok (Err %a, _)" E.pp e
           | Error e -> Format.asprintf "Err %a" E.pp e
         in
         Alcotest.failf
           "OPTIN MOVED-retry should return Ok(OK,v0); got %s"
           (pp_pair other));
    (* Topology says slot is owned by [owner_id] and MOVED also
       redirects to [owner_id] — destinations agree, so no eager
       Cache.clear fires. The cached entry is still present. *)
    Alcotest.(check int) "cache state after retry" 1
      (Cache.count cache)

let tests =
  [ Alcotest.test_case "OPTIN cluster: two-shard populate + evict"
      `Quick test_two_shards_populate_and_evict;
    Alcotest.test_case "OPTIN cluster: 25-fiber concurrent tracking"
      `Quick test_concurrent_optin_cluster;
    Alcotest.test_case "OPTIN cluster: MOVED on wrong target retries to correct owner"
      `Quick test_optin_moved_retry;
  ]
