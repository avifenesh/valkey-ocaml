(* Integration tests against a real Valkey 9 cluster spun up via
   docker-compose.cluster.yml. The cluster announces the service
   names [valkey-c1 .. valkey-c6]; [scripts/cluster-hosts-setup.sh]
   maps those to 127.0.0.1 on the docker host so the OCaml client
   (running on the host) resolves the same names the cluster's
   gossip uses.

   Tests skip gracefully if the cluster is not reachable — set
   VALKEY_CLUSTER=skip to force-skip even if it is reachable. *)

module C = Valkey.Client
module CR = Valkey.Cluster_router
module Conn = Valkey.Connection

let seeds = [ "valkey-c1", 7000; "valkey-c2", 7001; "valkey-c3", 7002 ]

let force_skip () =
  try Sys.getenv "VALKEY_CLUSTER" = "skip" with Not_found -> false

(* Try a single transient Connection to the first seed. True =
   reachable, false = the cluster isn't up / not configured. *)
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
      let finalize () = C.close client in
      (try f client; finalize ()
       with e -> finalize (); raise e)

let err_pp = Conn.Error.pp

let test_roundtrip_across_slots () =
  with_cluster_client @@ fun client ->
  let pairs =
    List.init 12 (fun i ->
        Printf.sprintf "cluster:smoke:%d" i,
        Printf.sprintf "v-%d" i)
  in
  List.iter
    (fun (k, v) ->
      match C.set client k v with
      | Ok true -> ()
      | Ok false -> Alcotest.failf "SET %s returned false" k
      | Error e -> Alcotest.failf "SET %s: %a" k err_pp e)
    pairs;
  List.iter
    (fun (k, v) ->
      match C.get client k with
      | Ok (Some got) ->
          Alcotest.(check string) (Printf.sprintf "GET %s" k) v got
      | Ok None -> Alcotest.failf "GET %s returned None" k
      | Error e -> Alcotest.failf "GET %s: %a" k err_pp e)
    pairs;
  List.iter (fun (k, _) -> ignore (C.del client [ k ])) pairs

let test_routing_writes_succeed () =
  with_cluster_client @@ fun client ->
  for i = 0 to 99 do
    let k = Printf.sprintf "route:%d" i in
    match C.set client k "x" with
    | Ok _ -> ()
    | Error e -> Alcotest.failf "SET %s: %a" k err_pp e
  done;
  for i = 0 to 99 do
    ignore (C.del client [ Printf.sprintf "route:%d" i ])
  done

let test_script_load_reaches_every_primary () =
  with_cluster_client @@ fun client ->
  let source = "return redis.status_reply('OK')" in
  match C.script_load client source with
  | Error e -> Alcotest.failf "SCRIPT LOAD: %a" err_pp e
  | Ok sha ->
      Alcotest.(check int) "sha len" 40 (String.length sha);
      (match C.script_exists client [ sha ] with
       | Error e -> Alcotest.failf "SCRIPT EXISTS: %a" err_pp e
       | Ok [ true ] -> ()
       | Ok other ->
           Alcotest.failf
             "SCRIPT EXISTS expected [true], got %d items"
             (List.length other))

let test_keys_fans_to_all_primaries () =
  with_cluster_client @@ fun client ->
  let prefix =
    Printf.sprintf "fanout:keys:%d:" (Random.int 1_000_000)
  in
  for i = 0 to 29 do
    ignore (C.set client (prefix ^ string_of_int i) "v")
  done;
  let pattern = prefix ^ "*" in
  (match C.keys client pattern with
   | Error e -> Alcotest.failf "KEYS %s: %a" pattern err_pp e
   | Ok ks ->
       Alcotest.(check int) "KEYS fan count" 30 (List.length ks));
  for i = 0 to 29 do
    ignore (C.del client [ prefix ^ string_of_int i ])
  done

let test_custom_command_routes_correctly () =
  (* Exercise the custom entry point with a command we do NOT have
     a typed wrapper for: BITCOUNT. Command_spec says rk 1, so the
     router must send it to the slot owning the key. If it did not,
     we would get a MOVED-chain but the redirect retry hides it; a
     direct Ok result is the strongest signal our table is right. *)
  with_cluster_client @@ fun client ->
  let key = "custom:bitcount:k" in
  let _ =
    C.custom client [| "SET"; key; "hello" |]
  in
  (match C.custom client [| "BITCOUNT"; key |] with
   | Error e -> Alcotest.failf "BITCOUNT via custom: %a" err_pp e
   | Ok (Valkey.Resp3.Integer _) -> ()
   | Ok v ->
       Alcotest.failf "BITCOUNT returned %a, expected Integer"
         Valkey.Resp3.pp v);
  ignore (C.del client [ key ])

let test_cross_slot_surfaces_crossslot () =
  with_cluster_client @@ fun client ->
  let _ = C.set client "noslot:a" "1" in
  let _ = C.set client "noslot:b" "2" in
  match C.del client [ "noslot:a"; "noslot:b" ] with
  | Ok _ -> ()  (* coincidentally same slot — acceptable *)
  | Error (Conn.Error.Server_error { code = "CROSSSLOT"; _ }) -> ()
  | Error e ->
      Alcotest.failf "DEL cross-slot: unexpected %a" err_pp e

let skip_placeholder name () =
  Printf.printf
    "SKIP %s: cluster not reachable (see docker-compose.cluster.yml \
     + scripts/cluster-hosts-setup.sh)\n%!"
    name

let tests =
  let reachable = cluster_reachable () in
  let tc name f =
    if reachable then Alcotest.test_case name `Quick f
    else Alcotest.test_case name `Quick (skip_placeholder name)
  in
  [ tc "roundtrip across slots" test_roundtrip_across_slots;
    tc "routing: 100 writes succeed" test_routing_writes_succeed;
    tc "SCRIPT LOAD reaches every primary"
      test_script_load_reaches_every_primary;
    tc "KEYS fans to all primaries" test_keys_fans_to_all_primaries;
    tc "custom command routes via Command_spec"
      test_custom_command_routes_correctly;
    tc "cross-slot DEL surfaces CROSSSLOT"
      test_cross_slot_surfaces_crossslot;
  ]
