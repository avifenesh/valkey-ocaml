module C = Valkey.Connection
module R = Valkey.Resp3

let host = "localhost"
let port = 6379

let with_connection ?config f =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let conn = C.connect ~sw ~net ~clock ?config ~host ~port () in
  let result = f env sw clock conn in
  C.close conn;
  result

let expect_bulk_eq ~ctx ~expected got =
  match got with
  | Ok (R.Bulk_string s) when s = expected -> ()
  | Ok other ->
      Alcotest.failf "%s: expected bulk %S, got %a" ctx expected R.pp other
  | Error e ->
      Alcotest.failf "%s: expected bulk %S, got error %a" ctx expected
        C.Error.pp e

let expect_simple_eq ~ctx ~expected got =
  match got with
  | Ok (R.Simple_string s) when s = expected -> ()
  | Ok other ->
      Alcotest.failf "%s: expected +%s, got %a" ctx expected R.pp other
  | Error e ->
      Alcotest.failf "%s: expected +%s, got error %a" ctx expected
        C.Error.pp e

let client_id conn =
  match C.server_info conn with
  | Some (R.Map kvs) ->
      let id =
        List.find_map
          (fun (k, v) ->
            match k, v with
            | R.Bulk_string "id", R.Integer i -> Some i
            | _ -> None)
          kvs
      in
      (match id with
       | Some i -> Int64.to_string i
       | None -> Alcotest.fail "no id in server_info")
  | _ -> Alcotest.fail "no server_info"

let wait_for_alive ~clock ~deadline conn =
  let start = Eio.Time.now clock in
  let rec loop () =
    match C.state conn with
    | C.Alive -> ()
    | C.Dead e -> Alcotest.failf "connection went Dead during recovery: %a"
                    C.Error.pp e
    | _ ->
        if Eio.Time.now clock -. start > deadline then
          Alcotest.fail "timed out waiting for Alive"
        else (
          Eio.Time.sleep clock 0.02;
          loop ())
  in
  loop ()

let result_str = function
  | Ok v -> Format.asprintf "Ok %a" R.pp v
  | Error e -> Format.asprintf "Error %a" C.Error.pp e

let test_ping () =
  with_connection @@ fun _env _sw _clock conn ->
  expect_simple_eq ~ctx:"PING" ~expected:"PONG"
    (C.request conn [| "PING" |]);
  expect_bulk_eq ~ctx:"PING msg" ~expected:"hi"
    (C.request conn [| "PING"; "hi" |])

let test_set_and_get () =
  with_connection @@ fun _env _sw _clock conn ->
  expect_simple_eq ~ctx:"SET" ~expected:"OK"
    (C.request conn [| "SET"; "ocaml:test:k"; "v1" |]);
  expect_bulk_eq ~ctx:"GET" ~expected:"v1"
    (C.request conn [| "GET"; "ocaml:test:k" |]);
  ignore (C.request conn [| "DEL"; "ocaml:test:k" |])

let test_wrong_type_error () =
  with_connection @@ fun _env _sw _clock conn ->
  ignore (C.request conn [| "DEL"; "ocaml:test:wt" |]);
  ignore (C.request conn [| "SET"; "ocaml:test:wt"; "scalar" |]);
  (match C.request conn [| "LPUSH"; "ocaml:test:wt"; "x" |] with
   | Error (C.Error.Server_error ve) when ve.code = "WRONGTYPE" -> ()
   | Error e ->
       Alcotest.failf "expected WRONGTYPE, got error %a" C.Error.pp e
   | Ok v -> Alcotest.failf "expected error, got %a" R.pp v);
  ignore (C.request conn [| "DEL"; "ocaml:test:wt" |])

let test_concurrent_set_get () =
  with_connection @@ fun _env _sw _clock conn ->
  let n = 50 in
  let keys = List.init n (fun i -> Printf.sprintf "ocaml:test:c:%d" i) in
  ignore (C.request conn (Array.of_list ("DEL" :: keys)));
  Eio.Fiber.all
    (List.mapi
       (fun i k () ->
         let v = Printf.sprintf "val-%d" i in
         match C.request conn [| "SET"; k; v |] with
         | Ok _ -> ()
         | Error e ->
             Alcotest.failf "SET %s failed: %a" k C.Error.pp e)
       keys);
  Eio.Fiber.all
    (List.mapi
       (fun i k () ->
         let expected = Printf.sprintf "val-%d" i in
         expect_bulk_eq ~ctx:(Printf.sprintf "GET %s" k)
           ~expected (C.request conn [| "GET"; k |]))
       keys);
  ignore (C.request conn (Array.of_list ("DEL" :: keys)))

let test_large_value () =
  with_connection @@ fun _env _sw _clock conn ->
  let key = "ocaml:test:large" in
  let size = 16 * 1024 in
  let value = String.make size 'x' in
  ignore (C.request conn [| "DEL"; key |]);
  expect_simple_eq ~ctx:"SET large" ~expected:"OK"
    (C.request conn [| "SET"; key; value |]);
  expect_bulk_eq ~ctx:"GET large" ~expected:value
    (C.request conn [| "GET"; key |]);
  ignore (C.request conn [| "DEL"; key |])

let test_availability_zone () =
  with_connection @@ fun _env _sw _clock conn ->
  let _ = C.availability_zone conn in
  ()

let tls_port = 6390

let rec find_repo_file rel dir =
  let candidate = Filename.concat dir rel in
  if Sys.file_exists candidate then candidate
  else
    let parent = Filename.dirname dir in
    if parent = dir then
      Alcotest.failf "cannot locate %s walking up from %s" rel (Sys.getcwd ())
    else find_repo_file rel parent

let ca_pem () =
  let path = find_repo_file "tls/ca.crt" (Sys.getcwd ()) in
  In_channel.with_open_bin path In_channel.input_all

let with_tls_connection ~tls f =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config = { C.Config.default with tls = Some tls } in
  let conn =
    C.connect ~sw ~net ~clock ~config ~host:"localhost" ~port:tls_port ()
  in
  let r = f conn in
  C.close conn;
  r

let test_tls_insecure () =
  with_tls_connection ~tls:(Valkey.Tls_config.insecure ()) @@ fun conn ->
  expect_simple_eq ~ctx:"PING over TLS (insecure)" ~expected:"PONG"
    (C.request conn [| "PING" |])

let test_tls_ca_verify () =
  let ca_pem = ca_pem () in
  let tls =
    Valkey.Tls_config.with_ca_cert ~server_name:"localhost" ~ca_pem ()
  in
  with_tls_connection ~tls @@ fun conn ->
  expect_simple_eq ~ctx:"PING over TLS (CA-verified)" ~expected:"PONG"
    (C.request conn [| "PING" |])

let test_keepalive () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config =
    { C.Config.default with keepalive_interval = Some 0.05 }
  in
  let conn =
    C.connect ~sw ~net ~clock ~config ~host:"localhost" ~port:6379 ()
  in
  Eio.Time.sleep clock 0.25;
  let n = C.keepalive_count conn in
  if n < 2 then
    Alcotest.failf "expected >=2 keepalive PINGs in 250ms at 50ms interval, got %d" n;
  C.close conn

(* System CA bundle should REJECT our self-signed cert — proves the
   authenticator is actually verifying. *)
let test_tls_system_cas_rejects_self_signed () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let tls =
    match Valkey.Tls_config.with_system_cas ~server_name:"localhost" () with
    | Ok t -> t
    | Error m -> Alcotest.failf "system cas not available: %s" m
  in
  let config = { C.Config.default with tls = Some tls } in
  try
    let conn =
      C.connect ~sw ~net ~clock ~config ~host:"localhost" ~port:tls_port ()
    in
    C.close conn;
    Alcotest.fail "expected handshake to fail against self-signed cert"
  with
  | C.Handshake_failed (C.Error.Tls_failed _) -> ()
  | C.Handshake_failed e ->
      Alcotest.failf "expected Tls_failed, got %a" C.Error.pp e

(* Second connection kills first's socket; first should recover + serve. *)
let test_recovery_client_kill () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let victim = C.connect ~sw ~net ~clock ~host ~port () in
  let killer = C.connect ~sw ~net ~clock ~host ~port () in
  let vid = client_id victim in
  Eio.Fiber.both
    (fun () ->
      match C.request victim [| "BLPOP"; "ocaml:test:nokey"; "0" |] with
      | Error C.Error.Interrupted -> ()
      | other ->
          Alcotest.failf "victim: expected Interrupted, got %s"
            (result_str other))
    (fun () ->
      Eio.Time.sleep clock 0.1;
      match C.request killer [| "CLIENT"; "KILL"; "ID"; vid |] with
      | Ok _ -> ()
      | Error e -> Alcotest.failf "CLIENT KILL failed: %a" C.Error.pp e);
  wait_for_alive ~clock ~deadline:3.0 victim;
  expect_simple_eq ~ctx:"PING after recovery" ~expected:"PONG"
    (C.request victim [| "PING" |]);
  C.close killer;
  C.close victim

(* User's timeout fires; reply arrives later; reader must drop silently and
   release the budget (so next request works). *)
let test_timeout_late_reply () =
  with_connection @@ fun _env _sw clock conn ->
  (match
     C.request ~timeout:0.05 conn [| "BLPOP"; "ocaml:test:nokey"; "0.3" |]
   with
   | Error C.Error.Timeout -> ()
   | other ->
       Alcotest.failf "expected Timeout, got %s" (result_str other));
  Eio.Time.sleep clock 0.5;
  expect_simple_eq ~ctx:"PING after late reply" ~expected:"PONG"
    (C.request conn [| "PING" |])

(* SPSC sent queue + byte-budget under load. *)
let test_stress () =
  with_connection @@ fun _env _sw _clock conn ->
  let n_fibers = 200 in
  let n_ops = 20 in
  let ok = Atomic.make 0 in
  let err = Atomic.make 0 in
  Eio.Fiber.all
    (List.init n_fibers (fun fid () ->
         for op = 0 to n_ops - 1 do
           let k = Printf.sprintf "ocaml:stress:%d:%d" fid op in
           let v = Printf.sprintf "v:%d:%d" fid op in
           match C.request conn [| "SET"; k; v |] with
           | Ok _ -> Atomic.incr ok
           | Error _ -> Atomic.incr err
         done));
  Alcotest.(check int) "no failures" 0 (Atomic.get err);
  Alcotest.(check int) "all succeeded" (n_fibers * n_ops) (Atomic.get ok)

(* Tiny budget + slow commands + tight timeout; verify backpressure + balance. *)
let test_budget_exhaustion () =
  let config = { C.Config.default with max_queued_bytes = 200 } in
  with_connection ~config @@ fun _env _sw clock conn ->
  Eio.Fiber.both
    (fun () ->
      Eio.Fiber.all
        (List.init 5 (fun _ () ->
             match C.request conn [| "BLPOP"; "ocaml:test:nokey"; "0.3" |] with
             | Ok _ -> ()
             | Error e ->
                 Alcotest.failf "BLPOP failed: %a" C.Error.pp e)))
    (fun () ->
      Eio.Time.sleep clock 0.05;
      match C.request ~timeout:0.05 conn [| "PING" |] with
      | Error C.Error.Timeout -> ()
      | other ->
          Alcotest.failf "expected Timeout, got %s" (result_str other));
  expect_simple_eq ~ctx:"PING after drain" ~expected:"PONG"
    (C.request conn [| "PING" |])

let tests =
  [ Alcotest.test_case "ping" `Quick test_ping;
    Alcotest.test_case "set_and_get" `Quick test_set_and_get;
    Alcotest.test_case "wrong_type_error" `Quick test_wrong_type_error;
    Alcotest.test_case "concurrent" `Quick test_concurrent_set_get;
    Alcotest.test_case "large_value" `Quick test_large_value;
    Alcotest.test_case "availability_zone" `Quick test_availability_zone;
    Alcotest.test_case "recovery" `Quick test_recovery_client_kill;
    Alcotest.test_case "timeout_late_reply" `Quick test_timeout_late_reply;
    Alcotest.test_case "stress" `Quick test_stress;
    Alcotest.test_case "budget_exhaustion" `Quick test_budget_exhaustion;
    Alcotest.test_case "tls_insecure" `Quick test_tls_insecure;
    Alcotest.test_case "tls_ca_verify" `Quick test_tls_ca_verify;
    Alcotest.test_case "tls_system_cas_rejects_self_signed" `Quick
      test_tls_system_cas_rejects_self_signed;
    Alcotest.test_case "keepalive fires" `Quick test_keepalive;
  ]
