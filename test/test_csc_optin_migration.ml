(** B2.5 cluster: OPTIN under live slot migration.

    Drives a real [CLUSTER SETSLOT MIGRATING / IMPORTING] window
    so the source primary returns [-ASK <slot> <target-addr>] for
    keys planted on the target. The CSC OPTIN read path then has
    to recover via {!Connection.request_triple}, sending
    [ASKING + CLIENT CACHING YES + GET] as three wire-adjacent
    frames on the target's connection. If the 3-frame submit isn't
    actually indivisible — or if the ASK handler in
    {!Cluster_router.make_pair} mishandles the redirect — at least
    one fiber sees the wrong value, a stale [Server_error ASK …],
    or an empty cache.

    Cleanup unconditionally clears the migration state on both
    nodes via [CLUSTER SETSLOT … STABLE] and deletes the planted
    keys, so a partial-failure test run can't leave the cluster
    permanently broken.

    Requires [docker compose -f docker-compose.cluster.yml up -d]. *)

module C = Valkey.Client
module Cfg = Valkey.Client.Config
module Cache = Valkey.Cache
module CC = Valkey.Client_cache
module CR = Valkey.Cluster_router
module Conn = Valkey.Connection
module E = Valkey.Connection.Error
module R = Valkey.Resp3

let seeds = Test_support.seeds
let force_skip = Test_support.force_skip
let cluster_reachable = Test_support.cluster_reachable
let skipped = Test_support.skipped

module CN = Test_support.Cluster_nodes

let pp_resp_result = function
  | Ok v -> Format.asprintf "Ok %a" R.pp v
  | Error e -> Format.asprintf "Err %a" E.pp e

let must_ok ~ctx = function
  | Ok _ -> ()
  | Error e -> Alcotest.failf "%s: %a" ctx E.pp e

let must_simple_ok ~ctx = function
  | Ok (R.Simple_string "OK") -> ()
  | other -> Alcotest.failf "%s: expected +OK, got %s" ctx (pp_resp_result other)

(* Resolve source / target by slot, and open direct connections
   to both for raw [CLUSTER SETSLOT] / [ASKING] / [SET]
   sequencing. The CSC client and aux router are separate; only
   raw cluster-state mutation goes through these direct conns. *)
let with_migrating_slot ~slot ~setup f =
  if force_skip () then skipped "cluster: OPTIN slot-migration"
  else if not (cluster_reachable ()) then
    skipped "cluster: OPTIN slot-migration"
  else
    Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
    let net = Eio.Stdenv.net env in
    let clock = Eio.Stdenv.clock env in
    let source_id =
      match CN.primary_owning_slot env ~slot with
      | Some id -> id
      | None -> Alcotest.failf "no primary owns slot %d" slot
    in
    let target_id =
      match CN.other_primary env ~not_id:source_id with
      | Some id -> id
      | None -> Alcotest.fail "no other primary"
    in
    let source_addr =
      match CN.addr_of_id env ~node_id:source_id with
      | Some a -> a
      | None -> Alcotest.failf "addr_of_id source %s missing" source_id
    in
    let target_addr =
      match CN.addr_of_id env ~node_id:target_id with
      | Some a -> a
      | None -> Alcotest.failf "addr_of_id target %s missing" target_id
    in
    let direct host port =
      Conn.connect ~sw ~net ~clock ~config:Conn.Config.default
        ~host ~port ()
    in
    let source = direct (fst source_addr) (snd source_addr) in
    let target = direct (fst target_addr) (snd target_addr) in
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
    let client =
      C.from_router
        ~config:{ Cfg.default with connection = cluster_cfg.connection }
        router
    in
    let aux = C.from_router ~config:Cfg.default aux_router in
    let slot_str = string_of_int slot in
    let restore_stable () =
      (* STABLE on a node with no MIGRATING/IMPORTING is a no-op
         that returns +OK, so it's safe to call unconditionally. *)
      let _ =
        Conn.request source [| "CLUSTER"; "SETSLOT"; slot_str; "STABLE" |]
      in
      let _ =
        Conn.request target [| "CLUSTER"; "SETSLOT"; slot_str; "STABLE" |]
      in
      ()
    in
    let cleanup_keys keys =
      let _ = C.del aux keys in
      List.iter
        (fun k ->
          let _ = Conn.request source [| "DEL"; k |] in
          let _ = Conn.request target [| "DEL"; k |] in
          ())
        keys
    in
    let finally keys () =
      restore_stable ();
      cleanup_keys keys;
      C.close client;
      C.close aux;
      Conn.close source;
      Conn.close target
    in
    let keys_ref = ref [] in
    let register keys = keys_ref := keys @ !keys_ref in
    Fun.protect
      ~finally:(fun () -> finally !keys_ref ())
      (fun () ->
        setup ~source ~target ~source_id ~target_id ~slot_str;
        f ~env ~client ~cache ~aux ~source ~target ~target_addr ~register)

(* Plant key on target via ASKING + SET. Target accepts because
   it has IMPORTING for the slot. *)
let plant_on_target ~target ~key ~value =
  must_simple_ok ~ctx:(Printf.sprintf "ASKING (plant %s)" key)
    (Conn.request target [| "ASKING" |]);
  must_simple_ok ~ctx:(Printf.sprintf "SET %s on target via ASKING" key)
    (Conn.request target [| "SET"; key; value |])

let setup_migrating ~source ~target ~source_id ~target_id ~slot_str =
  must_ok ~ctx:"CLUSTER SETSLOT MIGRATING on source"
    (Conn.request source
       [| "CLUSTER"; "SETSLOT"; slot_str; "MIGRATING"; target_id |]);
  must_ok ~ctx:"CLUSTER SETSLOT IMPORTING on target"
    (Conn.request target
       [| "CLUSTER"; "SETSLOT"; slot_str; "IMPORTING"; source_id |])

(* Single-key smoke: source MIGRATING, target IMPORTING, key on
   target. OPTIN GET via client routes to source per topology;
   source replies ASK; ASK retry's [request_triple] runs
   [ASKING + CACHING YES + GET] on target and surfaces the value.
   Cache must be populated and tracking must be registered on
   target — verified by an external SET from aux that fires an
   invalidation push. *)
let test_optin_ask_smoke () =
  let key = "{stress}:csc:optin:migrate:smoke" in
  let slot = Valkey.Slot.of_key key in
  with_migrating_slot ~slot ~setup:setup_migrating
  @@ fun ~env ~client ~cache ~aux ~source ~target ~target_addr:_ ~register ->
  register [ key ];
  plant_on_target ~target ~key ~value:"v_via_ask";
  (* Sanity: confirm migration state — direct GET on source must
     return ASK, not MOVED, otherwise the routed read can never
     hit the ASK branch we want to test. *)
  (match Conn.request source [| "GET"; key |] with
   | Error (E.Server_error ve) when ve.code = "ASK" -> ()
   | other ->
       Alcotest.failf
         "direct GET on source: expected ASK redirect, got %s"
         (pp_resp_result other));
  (* Sanity: a normal (non-CSC) routed GET via aux should also
     succeed, taking the standard exec ASK retry path. If even
     this fails, the bug isn't in the CSC pair handler. *)
  (match C.get aux key with
   | Ok (Some "v_via_ask") -> ()
   | other ->
       Alcotest.failf
         "aux (non-CSC) GET via ASK: expected v_via_ask, got %s"
         (match other with
          | Ok None -> "None"
          | Ok (Some s) -> Printf.sprintf "Some %S" s
          | Error e -> Format.asprintf "Error %a" E.pp e));
  (match C.get client key with
   | Ok (Some "v_via_ask") -> ()
   | other ->
       Alcotest.failf "OPTIN GET via ASK: expected v_via_ask, got %s"
         (match other with
          | Ok None -> "None"
          | Ok (Some s) -> Printf.sprintf "Some %S" s
          | Error e -> Format.asprintf "Error %a" E.pp e));
  Alcotest.(check int) "ASK-retried read populated cache" 1
    (Cache.count cache);
  (* Second OPTIN read in the same migrating window: cache hits
     locally, no wire traffic. Proves the cache entry from the
     ASK-retry path is usable for subsequent reads under the
     same migration state. *)
  (match C.get client key with
   | Ok (Some "v_via_ask") -> ()
   | other ->
       Alcotest.failf "OPTIN GET (cache hit) under MIGRATING: %s"
         (match other with
          | Ok None -> "None"
          | Ok (Some s) -> Printf.sprintf "Some %S" s
          | Error e -> Format.asprintf "Error %a" E.pp e));
  ignore env;
  ignore aux

(* Stress: 25 keys all in the same MIGRATING slot, planted on
   target, read concurrently via 25 fibers. Every fiber's read
   takes the ASK-retry path. If [request_triple] isn't truly
   wire-adjacent under concurrent enqueueing, you'd see a
   corrupted reply on at least one fiber (out-of-order CACHING vs
   GET, missing invalidation registration, or a leaked ASK
   redirect surfacing as Server_error to the caller). *)
let test_optin_ask_concurrent () =
  let n = 25 in
  let tag = "stress2" in
  let slot = Valkey.Slot.of_key (Printf.sprintf "{%s}:probe" tag) in
  let keys =
    List.init n (fun i ->
      Printf.sprintf "{%s}:csc:optin:migrate:c:%d" tag i)
  in
  with_migrating_slot ~slot ~setup:setup_migrating
  @@ fun ~env ~client ~cache ~aux:_ ~source:_ ~target ~target_addr:_ ~register ->
  register keys;
  let pairs =
    List.mapi (fun i k -> (k, Printf.sprintf "v%d" i)) keys
  in
  List.iter
    (fun (k, v) -> plant_on_target ~target ~key:k ~value:v)
    pairs;
  Eio.Fiber.List.iter
    (fun (k, expected) ->
      match C.get client k with
      | Ok (Some v) when v = expected -> ()
      | other ->
          Alcotest.failf
            "concurrent OPTIN GET via ASK %S (want %S): %s" k expected
            (match other with
             | Ok None -> "None"
             | Ok (Some s) -> Printf.sprintf "Some %S" s
             | Error e -> Format.asprintf "Error %a" E.pp e))
    pairs;
  Alcotest.(check int)
    "all ASK-retried reads cached" n (Cache.count cache);
  ignore env

(* Non-CSC stress for the same migrating window: the cluster
   router's standard [exec] also has an ASK retry that needs
   [ASKING + cmd] to travel as one indivisible submit. Two
   sequential [send_once] calls on a shared node connection let
   another fiber's frame interleave between [ASKING] and the
   retry, eating the one-shot flag and bouncing the actual
   command as MOVED. Filed and fixed alongside the OPTIN ordering
   bug; this test would have caught the latent non-CSC variant
   under concurrency. *)
let test_nonscs_ask_concurrent () =
  let n = 25 in
  let tag = "stress3" in
  let slot = Valkey.Slot.of_key (Printf.sprintf "{%s}:probe" tag) in
  let keys =
    List.init n (fun i ->
      Printf.sprintf "{%s}:noncsc:migrate:c:%d" tag i)
  in
  with_migrating_slot ~slot ~setup:setup_migrating
  @@ fun ~env:_ ~client:_ ~cache:_ ~aux ~source:_ ~target ~target_addr:_
       ~register ->
  register keys;
  let pairs =
    List.mapi (fun i k -> (k, Printf.sprintf "v%d" i)) keys
  in
  List.iter
    (fun (k, v) -> plant_on_target ~target ~key:k ~value:v)
    pairs;
  Eio.Fiber.List.iter
    (fun (k, expected) ->
      match C.get aux k with
      | Ok (Some v) when v = expected -> ()
      | other ->
          Alcotest.failf
            "non-CSC concurrent GET via ASK %S (want %S): %s" k expected
            (match other with
             | Ok None -> "None"
             | Ok (Some s) -> Printf.sprintf "Some %S" s
             | Error e -> Format.asprintf "Error %a" E.pp e))
    pairs

let tests =
  [ Alcotest.test_case "OPTIN cluster: ASK retry single-key smoke"
      `Quick test_optin_ask_smoke;
    Alcotest.test_case
      "OPTIN cluster: 25-fiber ASK retry under live MIGRATING slot"
      `Quick test_optin_ask_concurrent;
    Alcotest.test_case
      "non-CSC: 25-fiber ASK retry under live MIGRATING slot"
      `Quick test_nonscs_ask_concurrent;
  ]
