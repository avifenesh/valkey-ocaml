module C = Valkey.Client
module E = Valkey.Connection.Error
module R = Valkey.Resp3

let host = "localhost"
let port = 6379

let with_client f =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let c = C.connect ~sw ~net ~clock ~host ~port () in
  let r = f c in
  C.close c;
  r

let result_str = function
  | Ok v -> Format.asprintf "Ok %a" R.pp v
  | Error e -> Format.asprintf "Error %a" E.pp e

let test_exec_ping () =
  with_client @@ fun c ->
  match C.exec c [| "PING" |] with
  | Ok (R.Simple_string "PONG") -> ()
  | other -> Alcotest.failf "expected +PONG, got %s" (result_str other)

let test_get_missing () =
  with_client @@ fun c ->
  let key = "ocaml:client:missing" in
  let _ = C.del c [ key ] in
  match C.get c key with
  | Ok None -> ()
  | Ok (Some s) -> Alcotest.failf "expected None, got Some %S" s
  | Error e -> Alcotest.failf "GET error: %a" E.pp e

let test_set_get_del () =
  with_client @@ fun c ->
  let key = "ocaml:client:rt" in
  (match C.set c key "v1" with
   | Ok true -> ()
   | Ok false -> Alcotest.fail "SET returned false unexpectedly"
   | Error e -> Alcotest.failf "SET: %a" E.pp e);
  (match C.get c key with
   | Ok (Some "v1") -> ()
   | other ->
       Alcotest.failf "GET after SET: expected Some v1, got %s"
         (match other with
          | Ok None -> "None"
          | Ok (Some s) -> Printf.sprintf "Some %S" s
          | Error e -> Format.asprintf "Error %a" E.pp e));
  (match C.del c [ key ] with
   | Ok 1 -> ()
   | other ->
       Alcotest.failf "DEL: expected 1, got %s"
         (match other with
          | Ok n -> string_of_int n
          | Error e -> Format.asprintf "Error %a" E.pp e))

let test_set_nx_conflict () =
  with_client @@ fun c ->
  let key = "ocaml:client:nx" in
  let _ = C.del c [ key ] in
  (match C.set c key "a" ~nx:true with
   | Ok true -> ()
   | other -> Alcotest.failf "first NX should succeed: %s"
       (match other with
        | Ok b -> Printf.sprintf "Ok %b" b
        | Error e -> Format.asprintf "Error %a" E.pp e));
  (match C.set c key "b" ~nx:true with
   | Ok false -> ()
   | other -> Alcotest.failf "second NX should return false: %s"
       (match other with
        | Ok b -> Printf.sprintf "Ok %b" b
        | Error e -> Format.asprintf "Error %a" E.pp e));
  let _ = C.del c [ key ] in
  ()

let test_set_with_expiry () =
  with_client @@ fun c ->
  let key = "ocaml:client:ex" in
  (match C.set c key "tmp" ~ex_seconds:5.0 with
   | Ok true -> ()
   | other -> Alcotest.failf "SET EX: %s"
       (match other with
        | Ok b -> Printf.sprintf "Ok %b" b
        | Error e -> Format.asprintf "Error %a" E.pp e));
  (match C.get c key with
   | Ok (Some "tmp") -> ()
   | other -> Alcotest.failf "GET: %s"
       (match other with
        | Ok None -> "None"
        | Ok (Some s) -> Printf.sprintf "Some %S" s
        | Error e -> Format.asprintf "Error %a" E.pp e));
  let _ = C.del c [ key ] in
  ()

let test_del_multiple () =
  with_client @@ fun c ->
  let keys = [ "ocaml:client:m1"; "ocaml:client:m2"; "ocaml:client:m3" ] in
  List.iter (fun k -> ignore (C.set c k "x")) keys;
  (match C.del c keys with
   | Ok 3 -> ()
   | other ->
       Alcotest.failf "DEL 3 keys: %s"
         (match other with
          | Ok n -> string_of_int n
          | Error e -> Format.asprintf "Error %a" E.pp e))

let tests =
  [ Alcotest.test_case "exec PING" `Quick test_exec_ping;
    Alcotest.test_case "GET missing returns None" `Quick test_get_missing;
    Alcotest.test_case "SET / GET / DEL roundtrip" `Quick test_set_get_del;
    Alcotest.test_case "SET NX conflict returns false" `Quick
      test_set_nx_conflict;
    Alcotest.test_case "SET with ex_seconds" `Quick test_set_with_expiry;
    Alcotest.test_case "DEL multiple keys" `Quick test_del_multiple;
  ]
