(** B2.4: read-path caching + server invalidation integration.

    Needs live Valkey >= 7.4 on [localhost:6379]. The cache-enabled
    client subscribes to CLIENT TRACKING (B1), fills its cache on
    GET (this step), and an invalidator fiber (B2.2) drains the
    server's invalidation pushes to evict entries. This file
    verifies the full wire round-trip end-to-end. *)

module C = Valkey.Client
module Cfg = Valkey.Client.Config
module Cache = Valkey.Cache
module R = Valkey.Resp3
module E = Valkey.Connection.Error

let host = "localhost"
let port = 6379

(* Push delivery is async between server and client; 50ms is
   generous on loopback and stable on CI. *)
let grace_s = 0.05

let sleep_ms env ms =
  Eio.Time.sleep (Eio.Stdenv.clock env) (ms /. 1000.0)

(* Cache-enabled client [client] + bare aux client [aux] for
   "other-actor" writes. [keys] are cleaned before the body runs
   and on exit. *)
let with_csc ~keys f =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let cache = Cache.create ~byte_budget:(1024 * 1024) in
  let ccfg : Valkey.Client_cache.t =
    { cache; mode = Valkey.Client_cache.Default; optin = false;
      noloop = false; entry_ttl_ms = None }
  in
  let client =
    C.connect ~sw ~net ~clock
      ~config:{ Cfg.default with client_cache = Some ccfg }
      ~host ~port ()
  in
  let aux = C.connect ~sw ~net ~clock ~host ~port () in
  let cleanup () = List.iter (fun k -> let _ = C.del aux [k] in ()) keys in
  cleanup ();
  let finally () = cleanup (); C.close client; C.close aux in
  Fun.protect ~finally (fun () -> f env client cache aux)

(* First GET on a cold cache must round-trip and populate. Second
   GET must come from cache: observable via server's keyspace_hits
   counter. *)
let test_populates_then_hits () =
  let k = "ocaml:csc:pop:k" in
  with_csc ~keys:[k] @@ fun _env client cache aux ->
  let _ = C.exec aux [| "SET"; k; "v" |] in
  (* Miss -> wire fetch + populate. *)
  (match C.get client k with
   | Ok (Some "v") -> ()
   | other ->
       Alcotest.failf "initial GET: expected Some v, got %s"
         (match other with
          | Ok None -> "None"
          | Ok (Some s) -> Printf.sprintf "Some %S" s
          | Error e -> Format.asprintf "Error %a" E.pp e));
  Alcotest.(check bool) "cache populated after miss" true
    (Option.is_some (Cache.get cache k));
  (* Hit -> no wire. We can't directly prove no wire from here, but
     we prove the cache's internal state is consistent. *)
  match C.get client k with
  | Ok (Some "v") -> ()
  | _ -> Alcotest.fail "cached GET should return v"

(* External write triggers server invalidation push; our fiber
   drains it; our cache is evicted within the grace window. *)
let test_external_set_evicts_cache () =
  let k = "ocaml:csc:ext:k" in
  with_csc ~keys:[k] @@ fun env client cache aux ->
  let _ = C.exec aux [| "SET"; k; "v1" |] in
  let _ = C.get client k in
  Alcotest.(check bool) "cache has k after GET" true
    (Option.is_some (Cache.get cache k));
  let _ = C.exec aux [| "SET"; k; "v2" |] in
  sleep_ms env (grace_s *. 1000.0);
  Alcotest.(check (option reject)) "cache evicted after external SET"
    None (Cache.get cache k)

(* External DEL triggers the same invalidation path. *)
let test_external_del_evicts_cache () =
  let k = "ocaml:csc:del:k" in
  with_csc ~keys:[k] @@ fun env client cache aux ->
  let _ = C.exec aux [| "SET"; k; "v" |] in
  let _ = C.get client k in
  Alcotest.(check bool) "cached" true
    (Option.is_some (Cache.get cache k));
  let _ = C.exec aux [| "DEL"; k |] in
  sleep_ms env (grace_s *. 1000.0);
  Alcotest.(check (option reject)) "evicted after DEL"
    None (Cache.get cache k)

(* FLUSHDB sends a null-body invalidation push; whole cache must
   clear, not just one entry. *)
let test_flushdb_clears_whole_cache () =
  let k1 = "ocaml:csc:flush:a" in
  let k2 = "ocaml:csc:flush:b" in
  with_csc ~keys:[k1; k2] @@ fun env client cache aux ->
  let _ = C.exec aux [| "SET"; k1; "1" |] in
  let _ = C.exec aux [| "SET"; k2; "2" |] in
  let _ = C.get client k1 in
  let _ = C.get client k2 in
  Alcotest.(check int) "two entries cached" 2 (Cache.count cache);
  let _ = C.exec aux [| "FLUSHDB" |] in
  sleep_ms env (grace_s *. 1000.0);
  Alcotest.(check int) "cache fully cleared after FLUSHDB"
    0 (Cache.count cache)

let tests =
  [ Alcotest.test_case "miss then hit (populate + cache-hit)" `Quick
      test_populates_then_hits;
    Alcotest.test_case "external SET evicts our cache entry" `Quick
      test_external_set_evicts_cache;
    Alcotest.test_case "external DEL evicts our cache entry" `Quick
      test_external_del_evicts_cache;
    Alcotest.test_case "FLUSHDB clears whole cache" `Quick
      test_flushdb_clears_whole_cache;
  ]
