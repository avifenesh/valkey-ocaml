(** B2.5: OPTIN read-path integration.

    Mirrors [test_csc_invalidation.ml] but with [mode = Optin]. The
    cache-enabled client pipelines [CLIENT CACHING YES] + the read
    as a single wire-atomic submit; the server only tracks keys
    that arrive immediately after [CACHING YES], so any breakage
    of the pair's wire-adjacency surfaces as a missing
    invalidation push.

    Needs live Valkey >= 7.4 on [localhost:6379]. *)

module C = Valkey.Client
module Cfg = Valkey.Client.Config
module Cache = Valkey.Cache
module CC = Valkey.Client_cache
module R = Valkey.Resp3
module E = Valkey.Connection.Error

let host = "localhost"
let port = 6379

let grace_s = 0.05

let sleep_ms env ms =
  Eio.Time.sleep (Eio.Stdenv.clock env) (ms /. 1000.0)

(* Poll [check] until it returns true or [deadline_s] elapses.
   Returns the final value of [check]. Used in concurrency tests
   so a slow runner doesn't fail on the worst-case grace window
   while a fast runner still completes in tens of milliseconds. *)
let wait_until env ~deadline_s ~step_ms check =
  let clock = Eio.Stdenv.clock env in
  let deadline = Eio.Time.now clock +. deadline_s in
  let rec loop () =
    if check () then true
    else if Eio.Time.now clock >= deadline then false
    else begin
      Eio.Time.sleep clock (step_ms /. 1000.0);
      loop ()
    end
  in
  loop ()

let with_optin_csc ~keys f =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let cache = Cache.create ~byte_budget:(1024 * 1024) in
  let ccfg = CC.make ~cache ~mode:CC.Optin () in
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

(* Cold OPTIN read populates the cache; the second read is a hit. *)
let test_populates_then_hits () =
  let k = "ocaml:csc:optin:pop" in
  with_optin_csc ~keys:[k] @@ fun _env client cache aux ->
  let _ = C.exec aux [| "SET"; k; "v" |] in
  (match C.get client k with
   | Ok (Some "v") -> ()
   | other ->
       Alcotest.failf "initial OPTIN GET: expected Some v, got %s"
         (match other with
          | Ok None -> "None"
          | Ok (Some s) -> Printf.sprintf "Some %S" s
          | Error e -> Format.asprintf "Error %a" E.pp e));
  Alcotest.(check bool) "cache populated after OPTIN miss" true
    (Option.is_some (Cache.get cache k));
  match C.get client k with
  | Ok (Some "v") -> ()
  | _ -> Alcotest.fail "cached OPTIN GET should return v"

(* Load-bearing test: external SET on the same key must produce an
   invalidation push. The server only tracks keys that were read
   while CACHING was armed, so if [request_pair] failed to keep
   the [CACHING YES] + read wire-adjacent, the key would not be
   tracked, no invalidation would arrive, and the cache would
   stay hot — failing this assertion. *)
let test_external_set_evicts_optin_cache () =
  let k = "ocaml:csc:optin:ext" in
  with_optin_csc ~keys:[k] @@ fun env client cache aux ->
  let _ = C.exec aux [| "SET"; k; "v1" |] in
  let _ = C.get client k in
  Alcotest.(check bool) "cached after OPTIN GET" true
    (Option.is_some (Cache.get cache k));
  let _ = C.exec aux [| "SET"; k; "v2" |] in
  sleep_ms env (grace_s *. 1000.0);
  Alcotest.(check (option reject))
    "cache evicted after external SET (server tracked our key)"
    None (Cache.get cache k)

(* Concurrency: N fibers each issue an OPTIN GET on a distinct key.
   If the wire-pair atomicity broke under concurrent enqueue, the
   [CACHING YES] of fiber A might be consumed by fiber B's first
   frame, leaving fiber A's read untracked. We then mutate every
   key from aux and assert every entry got invalidated. *)
let test_concurrent_optin_tracking () =
  let n = 50 in
  let keys = List.init n (fun i -> Printf.sprintf "ocaml:csc:optin:c:%d" i) in
  with_optin_csc ~keys @@ fun env client cache aux ->
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
    wait_until env ~deadline_s:2.0 ~step_ms:10.0
      (fun () -> Cache.count cache = 0)
  in
  if not drained then
    Alcotest.failf
      "every concurrent OPTIN entry should have been tracked and \
       invalidated; %d still cached after 2s"
      (Cache.count cache)

(* If tracking is somehow disabled on the OPTIN client (e.g. a user
   issues [CLIENT TRACKING OFF] directly via [Client.exec]), the
   server rejects [CLIENT CACHING YES] with [ERR ... only when the
   client is in tracking mode with OPTIN or OPTOUT mode enabled].
   That error must surface to the caller as [Server_error] from
   [Client.get], not as a [Protocol_violation], not as a silent
   miss-and-don't-cache, and must not poison the [Inflight] table
   for future calls. *)
let test_caching_yes_error_surfaces_cleanly () =
  let k = "ocaml:csc:optin:err" in
  with_optin_csc ~keys:[k] @@ fun _env client cache aux ->
  let _ = C.exec aux [| "SET"; k; "v" |] in
  (* Disable tracking on the client's own connection from the
     inside; subsequent OPTIN reads should fail at the
     [CACHING YES] frame. *)
  (match C.exec client [| "CLIENT"; "TRACKING"; "OFF" |] with
   | Ok (R.Simple_string "OK") -> ()
   | other ->
       Alcotest.failf "CLIENT TRACKING OFF setup: %s"
         (match other with
          | Ok v -> Format.asprintf "got %a" R.pp v
          | Error e -> Format.asprintf "%a" E.pp e));
  (match C.get client k with
   | Error (E.Server_error ve) ->
       Alcotest.(check string) "ERR code" "ERR" ve.code;
       (* The exact message text is server-version-tied; just
          assert it mentions OPTIN/tracking so a regression in
          our error mapping (e.g. surfacing the wrong frame's
          reply) would visibly fail. *)
       let msg_lower = String.lowercase_ascii ve.message in
       let mentions_tracking =
         List.exists
           (fun substr ->
             let len_s = String.length substr in
             let len_m = String.length msg_lower in
             let rec scan i =
               if i + len_s > len_m then false
               else if String.sub msg_lower i len_s = substr then true
               else scan (i + 1)
             in
             scan 0)
           [ "optin"; "tracking" ]
       in
       Alcotest.(check bool)
         "error message mentions tracking/OPTIN" true mentions_tracking
   | other ->
       Alcotest.failf
         "OPTIN GET with tracking off should error with Server_error \
          but got %s"
         (match other with
          | Ok None -> "Ok None"
          | Ok (Some s) -> Printf.sprintf "Ok (Some %S)" s
          | Error e -> Format.asprintf "Error %a" E.pp e));
  Alcotest.(check (option reject))
    "no entry cached for failed OPTIN read"
    None (Cache.get cache k)

let tests =
  [ Alcotest.test_case "OPTIN populates then hits" `Quick
      test_populates_then_hits;
    Alcotest.test_case "OPTIN external SET evicts cache" `Quick
      test_external_set_evicts_optin_cache;
    Alcotest.test_case "OPTIN concurrent reads all get tracked" `Quick
      test_concurrent_optin_tracking;
    Alcotest.test_case "OPTIN CACHING-error path surfaces cleanly" `Quick
      test_caching_yes_error_surfaces_cleanly;
  ]
