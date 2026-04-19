(* Integration tests for Named_commands against standalone Valkey. *)

module C = Valkey.Client
module NC = Valkey.Named_commands
module E = Valkey.Connection.Error
module R = Valkey.Resp3

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

let test_register_and_run_command () =
  with_client @@ fun c ->
  let nc = NC.create c in
  let _ = C.del c [ "nc:a" ] in
  NC.register_command nc
    ~name:"set-x"
    ~template:[| "SET"; "$1"; "$2" |]
    ();
  NC.register_command nc
    ~name:"get-x"
    ~template:[| "GET"; "$1" |]
    ();
  (match NC.run_command nc ~name:"set-x" ~args:["nc:a"; "hello"] with
   | Ok (R.Simple_string "OK") -> ()
   | Ok v -> Alcotest.failf "SET: %a" R.pp v
   | Error e -> Alcotest.failf "SET: %a" err_pp e);
  (match NC.run_command nc ~name:"get-x" ~args:["nc:a"] with
   | Ok (R.Bulk_string "hello") -> ()
   | Ok v -> Alcotest.failf "GET: %a" R.pp v
   | Error e -> Alcotest.failf "GET: %a" err_pp e);
  ignore (C.del c [ "nc:a" ])

let test_unknown_name () =
  with_client @@ fun c ->
  let nc = NC.create c in
  match NC.run_command nc ~name:"does-not-exist" ~args:[] with
  | Error (E.Terminal _) -> ()
  | Ok _ -> Alcotest.fail "expected Terminal error"
  | Error e -> Alcotest.failf "unexpected error: %a" err_pp e

let test_wrong_kind () =
  with_client @@ fun c ->
  let nc = NC.create c in
  NC.register_command nc ~name:"x"
    ~template:[| "GET"; "$1" |] ();
  (match NC.run_transaction nc ~name:"x" ~args:["k"] with
   | Error (E.Terminal _) -> ()
   | Ok _ -> Alcotest.fail "expected Terminal error"
   | Error e -> Alcotest.failf "unexpected: %a" err_pp e);
  NC.register_transaction nc ~name:"tx"
    ~commands:[ [| "GET"; "$1" |] ] ();
  (match NC.run_command nc ~name:"tx" ~args:["k"] with
   | Error (E.Terminal _) -> ()
   | Ok _ -> Alcotest.fail "expected Terminal error"
   | Error e -> Alcotest.failf "unexpected: %a" err_pp e)

let test_run_transaction () =
  with_client @@ fun c ->
  let nc = NC.create c in
  let _ = C.del c [ "nc:t" ] in
  NC.register_transaction nc
    ~name:"incr-and-read"
    ~commands:[
      [| "SET"; "$1"; "$2" |];
      [| "INCR"; "$1" |];
      [| "GET"; "$1" |];
    ]
    ();
  match
    NC.run_transaction nc ~name:"incr-and-read" ~args:["nc:t"; "10"]
  with
  | Ok (Some replies) ->
      Alcotest.(check int) "reply count" 3 (List.length replies);
      (match replies with
       | [ R.Simple_string "OK"; R.Integer 11L; R.Bulk_string "11" ] -> ()
       | _ ->
           Alcotest.failf "unexpected replies: %s"
             (String.concat ", "
                (List.map (Format.asprintf "%a" R.pp) replies)));
      ignore (C.del c [ "nc:t" ])
  | Ok None -> Alcotest.fail "unexpected watch abort"
  | Error e -> Alcotest.failf "run_transaction: %a" err_pp e

let test_placeholder_literal_passthrough () =
  (* A token like "$1foo" or "$$1" is NOT a placeholder; it's
     literal. Verify by registering a SET whose value is literally
     "$X". *)
  with_client @@ fun c ->
  let nc = NC.create c in
  let _ = C.del c [ "nc:lit" ] in
  NC.register_command nc ~name:"set-lit"
    ~template:[| "SET"; "$1"; "price:$1" |]
    ();
  (* The value "price:$1" should also be substituted only for the
     exact "$1" match at token level. Since the whole value token
     "price:$1" is not a pure "$N", it stays literal. *)
  (match NC.run_command nc ~name:"set-lit" ~args:["nc:lit"] with
   | Ok (R.Simple_string "OK") -> ()
   | Ok v -> Alcotest.failf "SET literal: %a" R.pp v
   | Error e -> Alcotest.failf "SET literal: %a" err_pp e);
  (match C.get c "nc:lit" with
   | Ok (Some v) ->
       Alcotest.(check string) "literal preserved" "price:$1" v
   | _ -> Alcotest.fail "value missing");
  ignore (C.del c [ "nc:lit" ])

let test_has_and_unregister () =
  with_client @@ fun c ->
  let nc = NC.create c in
  Alcotest.(check bool) "initially missing" false (NC.has nc "x");
  NC.register_command nc ~name:"x" ~template:[| "PING" |] ();
  Alcotest.(check bool) "after register" true (NC.has nc "x");
  NC.unregister nc "x";
  Alcotest.(check bool) "after unregister" false (NC.has nc "x")

let tests =
  [ Alcotest.test_case "register + run_command" `Quick
      test_register_and_run_command;
    Alcotest.test_case "unknown name errors" `Quick
      test_unknown_name;
    Alcotest.test_case "wrong-kind invocation errors" `Quick
      test_wrong_kind;
    Alcotest.test_case "register + run_transaction" `Quick
      test_run_transaction;
    Alcotest.test_case "placeholder literal pass-through" `Quick
      test_placeholder_literal_passthrough;
    Alcotest.test_case "has / unregister" `Quick
      test_has_and_unregister;
  ]
