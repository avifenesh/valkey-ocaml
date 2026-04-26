(** B1: CLIENT TRACKING handshake.

    Integration tests confirming that when a client is configured
    with client-side caching enabled, the server reports tracking is
    active on the connection, and that tracking is re-issued after
    a forced reconnect.

    Requires a live Valkey >= 7.4 on [localhost:6379]. *)

module C = Valkey.Client
module Cfg = Valkey.Client.Config
module Conn = Valkey.Connection
module E = Valkey.Connection.Error
module R = Valkey.Resp3

let host = "localhost"
let port = 6379

let sleep_ms env ms =
  Eio.Time.sleep (Eio.Stdenv.clock env) (ms /. 1000.0)

(* Open a cache-enabled client + a bare aux client, run the body,
   close both. *)
let with_csc f =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let cache = Valkey.Cache.create ~byte_budget:(1024 * 1024) in
  let ccfg : Valkey.Client_cache.t =
    { cache; mode = Valkey.Client_cache.Default; optin = true;
      noloop = false; entry_ttl_ms = None }
  in
  let connection =
    { Conn.Config.default with client_cache = Some ccfg }
  in
  let client =
    C.connect ~sw ~net ~clock
      ~config:{ Cfg.default with connection } ~host ~port ()
  in
  let aux = C.connect ~sw ~net ~clock ~host ~port () in
  let finally () = C.close client; C.close aux in
  Fun.protect ~finally (fun () -> f env client aux)

(* Extract the "flags" collection from a CLIENT TRACKINGINFO map
   reply. The server may return the flags as Array *or* Set
   depending on version (observed: valkey 9 returns Set); accept
   both. *)
let trackinginfo_flags reply =
  let names_of xs =
    List.filter_map
      (function R.Bulk_string s -> Some s | _ -> None)
      xs
  in
  match reply with
  | R.Map kvs ->
      List.find_map
        (fun (k, v) ->
          match k, v with
          | R.Bulk_string "flags", R.Array flags -> Some (names_of flags)
          | R.Bulk_string "flags", R.Set flags -> Some (names_of flags)
          | _ -> None)
        kvs
  | _ -> None

(* After connect, TRACKINGINFO should show the connection as
   actively tracking ("on") in OPTIN mode. *)
let test_tracking_enabled_on_connect () =
  with_csc @@ fun _env client _aux ->
  match C.exec client [| "CLIENT"; "TRACKINGINFO" |] with
  | Ok reply ->
      (match trackinginfo_flags reply with
       | Some flags ->
           if not (List.mem "on" flags) then
             Alcotest.failf "expected 'on' in TRACKINGINFO flags, got [%s]"
               (String.concat ", " flags);
           if not (List.mem "optin" flags) then
             Alcotest.failf "expected 'optin' in TRACKINGINFO flags, got [%s]"
               (String.concat ", " flags)
       | None ->
           Alcotest.failf "TRACKINGINFO flags missing in reply: %a"
             R.pp reply)
  | Error e -> Alcotest.failf "TRACKINGINFO: %a" E.pp e

(* After a forced server-side kill, the reconnect must re-issue
   CLIENT TRACKING. Assert TRACKINGINFO on the new connection still
   reports 'on'. *)
let test_tracking_reinstated_after_reconnect () =
  with_csc @@ fun env client aux ->
  let self_id =
    match C.exec client [| "CLIENT"; "ID" |] with
    | Ok (R.Integer id) -> id
    | other ->
        Alcotest.failf "CLIENT ID: %s"
          (match other with
           | Ok v -> Format.asprintf "got %a" R.pp v
           | Error e -> Format.asprintf "%a" E.pp e)
  in
  let _ =
    C.exec aux [| "CLIENT"; "KILL"; "ID"; Int64.to_string self_id |]
  in
  sleep_ms env 100.0;
  (* Kick the client to trigger reconnect. *)
  let _ = C.exec client [| "PING" |] in
  sleep_ms env 200.0;
  match C.exec client [| "CLIENT"; "TRACKINGINFO" |] with
  | Ok reply ->
      (match trackinginfo_flags reply with
       | Some flags when List.mem "on" flags -> ()
       | Some flags ->
           Alcotest.failf
             "tracking not re-enabled after reconnect; flags=[%s]"
             (String.concat ", " flags)
       | None ->
           Alcotest.failf "TRACKINGINFO missing flags: %a" R.pp reply)
  | Error e -> Alcotest.failf "TRACKINGINFO: %a" E.pp e

let tests =
  [ Alcotest.test_case "tracking enabled on connect" `Quick
      test_tracking_enabled_on_connect;
    Alcotest.test_case "tracking reinstated after reconnect" `Slow
      test_tracking_reinstated_after_reconnect;
  ]
