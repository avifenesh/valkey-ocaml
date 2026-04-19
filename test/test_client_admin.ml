(* CLIENT admin command tests — Batch 1d. *)

module C = Valkey.Client
module E = Valkey.Connection.Error

let host = "localhost"
let port = 6379
let err_pp = E.pp

let with_client f =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let c = C.connect ~sw ~net ~clock ~host ~port () in
  Fun.protect ~finally:(fun () -> C.close c) (fun () -> f c)

let check_ok msg = function
  | Ok v -> v
  | Error e -> Alcotest.failf "%s: %a" msg err_pp e

let contains haystack needle =
  let ls = String.length haystack and ln = String.length needle in
  if ln = 0 then true
  else
    let rec loop i =
      if i + ln > ls then false
      else if String.sub haystack i ln = needle then true
      else loop (i + 1)
    in
    loop 0

let test_id () =
  with_client @@ fun c ->
  let id1 = check_ok "CLIENT ID" (C.client_id c) in
  if id1 <= 0 then Alcotest.failf "nonpositive id %d" id1;
  let id2 = check_ok "CLIENT ID again" (C.client_id c) in
  Alcotest.(check int) "same id on same conn" id1 id2

let test_setname_getname () =
  with_client @@ fun c ->
  check_ok "SETNAME"
    (C.client_setname c ~name:"test-valkey-ocaml");
  let n = check_ok "GETNAME set" (C.client_getname c) in
  Alcotest.(check (option string)) "name reads back"
    (Some "test-valkey-ocaml") n

let test_info_contains_id () =
  with_client @@ fun c ->
  let id = check_ok "CLIENT ID" (C.client_id c) in
  let info = check_ok "CLIENT INFO" (C.client_info c) in
  let needle = Printf.sprintf "id=%d" id in
  if not (contains info needle) then
    Alcotest.failf "CLIENT INFO missing %S in:\n%s" needle info

let test_list_contains_self () =
  with_client @@ fun c ->
  let id = check_ok "CLIENT ID" (C.client_id c) in
  let list = check_ok "CLIENT LIST" (C.client_list c) in
  let needle = Printf.sprintf "id=%d" id in
  if not (contains list needle) then
    Alcotest.failf "CLIENT LIST missing %S in:\n%s" needle list

let test_no_evict_and_no_touch () =
  with_client @@ fun c ->
  check_ok "NO-EVICT on" (C.client_no_evict c true);
  check_ok "NO-EVICT off" (C.client_no_evict c false);
  check_ok "NO-TOUCH on" (C.client_no_touch c true);
  check_ok "NO-TOUCH off" (C.client_no_touch c false)

let test_unpause () =
  with_client @@ fun c ->
  (* Unpause is safe to call unconditionally. Fans to all
     primaries in cluster; against standalone hits the one node. *)
  check_ok "CLIENT UNPAUSE" (C.client_unpause c)

let test_tracking_on_off () =
  with_client @@ fun c ->
  check_ok "TRACKING ON" (C.client_tracking_on c ());
  check_ok "TRACKING OFF" (C.client_tracking_off c)

let test_tracking_bcast_prefix () =
  with_client @@ fun c ->
  check_ok "TRACKING ON BCAST PREFIX"
    (C.client_tracking_on c ~bcast:true
       ~prefixes:[ "user:"; "session:" ] ());
  check_ok "TRACKING OFF" (C.client_tracking_off c)

let test_kill_with_filter () =
  (* Open a secondary connection; kill it via the first using a
     typed ID filter. *)
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let admin = C.connect ~sw ~net ~clock ~host ~port () in
  let victim = C.connect ~sw ~net ~clock ~host ~port () in
  Fun.protect
    ~finally:(fun () -> C.close admin; C.close victim)
  @@ fun () ->
  let vid = check_ok "victim ID" (C.client_id victim) in
  let killed =
    check_ok "CLIENT KILL ID"
      (C.client_kill admin ~filters:[ C.Kill_id vid ])
  in
  Alcotest.(check int) "killed exactly one" 1 killed

let tests =
  [ Alcotest.test_case "CLIENT ID is stable on a connection" `Quick
      test_id;
    Alcotest.test_case "CLIENT SETNAME / GETNAME round-trip" `Quick
      test_setname_getname;
    Alcotest.test_case "CLIENT INFO contains our id=" `Quick
      test_info_contains_id;
    Alcotest.test_case "CLIENT LIST contains our id=" `Quick
      test_list_contains_self;
    Alcotest.test_case "CLIENT NO-EVICT / NO-TOUCH toggle" `Quick
      test_no_evict_and_no_touch;
    Alcotest.test_case "CLIENT UNPAUSE (fan)" `Quick test_unpause;
    Alcotest.test_case "CLIENT TRACKING on / off" `Quick
      test_tracking_on_off;
    Alcotest.test_case "CLIENT TRACKING ON BCAST PREFIX" `Quick
      test_tracking_bcast_prefix;
    Alcotest.test_case "CLIENT KILL ID filter" `Quick
      test_kill_with_filter;
  ]
