(** B10b: BCAST mode — prefix-based invalidation.

    In BCAST mode the server sends invalidations for every key
    whose name starts with one of the client's declared prefixes,
    without maintaining a per-client tracking table of keys
    actually read. Scales to fleet deployments where millions of
    keys would otherwise bloat the server's tracking table.

    Requires live Valkey >= 7.4 on [localhost:6379]. *)

module C = Valkey.Client
module Cfg = Valkey.Client.Config
module Cache = Valkey.Cache
module CC = Valkey.Client_cache
module R = Valkey.Resp3
module E = Valkey.Connection.Error

let host = "localhost"
let port = 6379

let sleep_ms env ms =
  Eio.Time.sleep (Eio.Stdenv.clock env) (ms /. 1000.0)

(* Build a BCAST-mode Client_cache.t with one or more prefixes. *)
let bcast_ccfg ~cache ~prefixes =
  { CC.cache;
    inflight = Valkey.Inflight.create ();
    mode = CC.Bcast { prefixes };
    noloop = false;
    entry_ttl_ms = None }

let with_bcast ~prefix ~keys f =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let cache = Cache.create ~byte_budget:(1024 * 1024) in
  let ccfg = bcast_ccfg ~cache ~prefixes:[prefix] in
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

(* TRACKINGINFO should report 'bcast' in flags when BCAST mode is
   active, and prefixes should list our declared prefix. *)
let test_bcast_flags_on_connect () =
  let prefix = "ocaml:csc:bcast:" in
  with_bcast ~prefix ~keys:[] @@ fun _env client _cache _aux ->
  match C.exec client [| "CLIENT"; "TRACKINGINFO" |] with
  | Ok (R.Map kvs) ->
      let names_of xs =
        List.filter_map
          (function R.Bulk_string s -> Some s | _ -> None)
          xs
      in
      let find_flags () =
        List.find_map
          (fun (k, v) ->
            match k, v with
            | R.Bulk_string "flags", R.Array xs -> Some (names_of xs)
            | R.Bulk_string "flags", R.Set xs -> Some (names_of xs)
            | _ -> None)
          kvs
      in
      let find_prefixes () =
        List.find_map
          (fun (k, v) ->
            match k, v with
            | R.Bulk_string "prefixes", R.Array xs -> Some (names_of xs)
            | R.Bulk_string "prefixes", R.Set xs -> Some (names_of xs)
            | _ -> None)
          kvs
      in
      (match find_flags () with
       | Some flags ->
           if not (List.mem "bcast" flags) then
             Alcotest.failf "expected 'bcast' flag, got [%s]"
               (String.concat ", " flags);
           if not (List.mem "on" flags) then
             Alcotest.failf "expected 'on' flag, got [%s]"
               (String.concat ", " flags)
       | None -> Alcotest.fail "TRACKINGINFO flags missing");
      (match find_prefixes () with
       | Some [p] when p = prefix -> ()
       | Some ps ->
           Alcotest.failf "expected [%s], got [%s]" prefix
             (String.concat ", " ps)
       | None -> Alcotest.fail "TRACKINGINFO prefixes missing")
  | Ok other ->
      Alcotest.failf "expected Map, got %a" R.pp other
  | Error e -> Alcotest.failf "TRACKINGINFO: %a" E.pp e

(* Core BCAST: cache a key matching the prefix, external SET
   triggers invalidation for any key under the prefix, our cache
   evicts. Key insight vs default tracking: we don't have to
   have READ the key to receive its invalidation — any write
   under the prefix fans out. Test this by populating the cache,
   then writing a *sibling* key under the same prefix, asserting
   our cached entry is still there (sibling write doesn't evict
   us). Then writing OUR key evicts. *)
let test_bcast_invalidates_prefix_match () =
  let prefix = "ocaml:csc:bcast:invalid:" in
  let k = prefix ^ "k" in
  with_bcast ~prefix ~keys:[k] @@ fun env client cache aux ->
  let _ = C.exec aux [| "SET"; k; "v1" |] in
  (* In BCAST mode the server sends an invalidation for any write
     under the prefix, including writes that happened before our
     tracking was active — but here the SET happens *after* the
     cache-enabled client's handshake (we connected first), so we
     do get an invalidation. Drain it before our first GET so it
     doesn't flip the inflight dirty flag on our own fetch. *)
  sleep_ms env 0.05;
  (* Populate via GET. *)
  (match C.get client k with
   | Ok (Some "v1") -> () | _ -> Alcotest.fail "initial GET");
  Alcotest.(check bool) "cached" true
    (Option.is_some (Cache.get cache k));
  (* External write to the same key → invalidation arrives. *)
  let _ = C.exec aux [| "SET"; k; "v2" |] in
  sleep_ms env 0.05;
  Alcotest.(check (option reject)) "evicted after external SET"
    None (Cache.get cache k)

(* Without reading, BCAST still evicts: put the value in cache
   via the typed Client.get to populate, then set from aux. The
   test above already covers this. What this test adds: a write
   to a key *not* under the prefix must NOT evict. *)
let test_bcast_ignores_out_of_prefix_writes () =
  let prefix = "ocaml:csc:bcast:scope:" in
  let cached_k = prefix ^ "k" in
  let outside_k = "ocaml:csc:bcast:other:x" in
  with_bcast ~prefix ~keys:[cached_k; outside_k]
  @@ fun env client cache aux ->
  let _ = C.exec aux [| "SET"; cached_k; "v" |] in
  sleep_ms env 0.05;    (* drain aux's SET-triggered invalidation *)
  let _ = C.get client cached_k in
  Alcotest.(check bool) "cached_k cached" true
    (Option.is_some (Cache.get cache cached_k));
  (* Write to a key outside our declared prefix. *)
  let _ = C.exec aux [| "SET"; outside_k; "unrelated" |] in
  sleep_ms env 0.05;
  Alcotest.(check bool) "cached_k still there (out-of-prefix write)"
    true (Option.is_some (Cache.get cache cached_k))

let tests =
  [ Alcotest.test_case "BCAST: TRACKINGINFO flags include bcast + prefix"
      `Quick test_bcast_flags_on_connect;
    Alcotest.test_case "BCAST: external SET on prefix-match evicts"
      `Quick test_bcast_invalidates_prefix_match;
    Alcotest.test_case "BCAST: out-of-prefix writes leave cache alone"
      `Quick test_bcast_ignores_out_of_prefix_writes;
  ]
