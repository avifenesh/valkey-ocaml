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

let test_unlink () =
  with_client @@ fun c ->
  let keys = [ "ocaml:c:u1"; "ocaml:c:u2" ] in
  List.iter (fun k -> ignore (C.set c k "v")) keys;
  match C.unlink c keys with
  | Ok 2 -> ()
  | other ->
      Alcotest.failf "UNLINK: %s"
        (match other with
         | Ok n -> string_of_int n
         | Error e -> Format.asprintf "Error %a" E.pp e)

let test_setex_setnx_and_ttl () =
  with_client @@ fun c ->
  let key = "ocaml:c:se" in
  let _ = C.del c [ key ] in
  (match C.setex c key ~seconds:30 "v" with
   | Ok () -> ()
   | Error e -> Alcotest.failf "SETEX: %a" E.pp e);
  (match C.ttl c key with
   | Ok (C.Expires_in n) when n > 20 && n <= 30 -> ()
   | Ok Absent -> Alcotest.fail "key unexpectedly absent"
   | Ok Persistent -> Alcotest.fail "expected TTL, got persistent"
   | Ok (C.Expires_in n) -> Alcotest.failf "TTL out of range: %d" n
   | Error e -> Alcotest.failf "TTL: %a" E.pp e);
  (match C.setnx c key "other" with
   | Ok false -> ()
   | Ok true -> Alcotest.fail "SETNX on existing key should return false"
   | Error e -> Alcotest.failf "SETNX: %a" E.pp e);
  let _ = C.del c [ key ] in
  (match C.setnx c key "first" with
   | Ok true -> ()
   | Ok false -> Alcotest.fail "SETNX on absent key should return true"
   | Error e -> Alcotest.failf "SETNX: %a" E.pp e);
  let _ = C.del c [ key ] in
  ()

let test_ttl_absent_and_persistent () =
  with_client @@ fun c ->
  let key = "ocaml:c:ttl" in
  let _ = C.del c [ key ] in
  (match C.ttl c key with
   | Ok Absent -> ()
   | other -> Alcotest.failf "missing key should be Absent: %s"
       (match other with
        | Ok Absent -> "Absent"
        | Ok Persistent -> "Persistent"
        | Ok (C.Expires_in n) -> Printf.sprintf "Expires_in %d" n
        | Error e -> Format.asprintf "Error %a" E.pp e));
  ignore (C.set c key "v");
  (match C.ttl c key with
   | Ok Persistent -> ()
   | other -> Alcotest.failf "SET without TTL should be Persistent: %s"
       (match other with
        | Ok Absent -> "Absent"
        | Ok Persistent -> "Persistent"
        | Ok (C.Expires_in n) -> Printf.sprintf "Expires_in %d" n
        | Error e -> Format.asprintf "Error %a" E.pp e));
  let _ = C.del c [ key ] in
  ()

let test_expire_pexpire () =
  with_client @@ fun c ->
  let key = "ocaml:c:exp" in
  ignore (C.set c key "v");
  (match C.expire c key ~seconds:60 with
   | Ok true -> ()
   | other -> Alcotest.failf "EXPIRE: %s"
       (match other with
        | Ok b -> Printf.sprintf "Ok %b" b
        | Error e -> Format.asprintf "Error %a" E.pp e));
  (match C.pexpire c key ~millis:45_000 with
   | Ok true -> ()
   | other -> Alcotest.failf "PEXPIRE: %s"
       (match other with
        | Ok b -> Printf.sprintf "Ok %b" b
        | Error e -> Format.asprintf "Error %a" E.pp e));
  (match C.pttl c key with
   | Ok (C.Expires_in n) when n <= 45_000 && n > 30_000 -> ()
   | other -> Alcotest.failf "PTTL out of range: %s"
       (match other with
        | Ok Absent -> "Absent"
        | Ok Persistent -> "Persistent"
        | Ok (C.Expires_in n) -> Printf.sprintf "Expires_in %d" n
        | Error e -> Format.asprintf "Error %a" E.pp e));
  let _ = C.del c [ key ] in
  ()

let test_incr_incrby () =
  with_client @@ fun c ->
  let key = "ocaml:c:cnt" in
  let _ = C.del c [ key ] in
  Alcotest.(check int64) "INCR from 0" 1L
    (match C.incr c key with Ok n -> n | Error e -> Alcotest.failf "INCR: %a" E.pp e);
  Alcotest.(check int64) "INCRBY +10" 11L
    (match C.incrby c key 10 with Ok n -> n | Error e -> Alcotest.failf "INCRBY: %a" E.pp e);
  Alcotest.(check int64) "INCRBY -5" 6L
    (match C.incrby c key (-5) with Ok n -> n | Error e -> Alcotest.failf "INCRBY: %a" E.pp e);
  let _ = C.del c [ key ] in
  ()

let test_mget () =
  with_client @@ fun c ->
  let keys = [ "ocaml:c:m1"; "ocaml:c:m2"; "ocaml:c:m3" ] in
  List.iter (fun k -> ignore (C.del c [ k ])) keys;
  ignore (C.set c "ocaml:c:m1" "a");
  ignore (C.set c "ocaml:c:m3" "c");
  (match C.mget c keys with
   | Ok [ Some "a"; None; Some "c" ] -> ()
   | other ->
       Alcotest.failf "MGET unexpected: %s"
         (match other with
          | Ok vs ->
              "[" ^ String.concat "; "
                (List.map (function
                   | None -> "None" | Some s -> Printf.sprintf "Some %S" s)
                   vs) ^ "]"
          | Error e -> Format.asprintf "Error %a" E.pp e));
  List.iter (fun k -> ignore (C.del c [ k ])) keys

let test_type_of () =
  with_client @@ fun c ->
  let sk = "ocaml:c:t_str" in
  let hk = "ocaml:c:t_hash" in
  let missing = "ocaml:c:t_missing" in
  List.iter (fun k -> ignore (C.del c [ k ])) [ sk; hk; missing ];
  ignore (C.set c sk "v");
  ignore (C.exec c [| "HSET"; hk; "f"; "v" |]);
  Alcotest.(check bool) "string" true
    (match C.type_of c sk with Ok T_string -> true | _ -> false);
  Alcotest.(check bool) "hash" true
    (match C.type_of c hk with Ok T_hash -> true | _ -> false);
  Alcotest.(check bool) "none" true
    (match C.type_of c missing with Ok T_none -> true | _ -> false);
  List.iter (fun k -> ignore (C.del c [ k ])) [ sk; hk ]

let test_set_ifeq () =
  (* SET IFEQ requires Valkey 8.1+; skip cleanly if the server rejects. *)
  with_client @@ fun c ->
  let key = "ocaml:c:ifeq" in
  let _ = C.del c [ key ] in
  ignore (C.set c key "hello");
  (match C.set c key "world" ~ifeq:"hello" with
   | Ok true -> ()
   | Ok false -> Alcotest.fail "IFEQ match should succeed"
   | Error (E.Server_error _) ->
       (* older server returns a syntax/unknown error; skip *)
       ()
   | Error e -> Alcotest.failf "SET IFEQ: %a" E.pp e);
  (match C.set c key "other" ~ifeq:"nope" with
   | Ok false -> ()
   | Ok true -> Alcotest.fail "IFEQ mismatch should fail"
   | Error _ ->
       (* older server; skip *)
       ());
  let _ = C.del c [ key ] in
  ()

let test_delifeq () =
  (* Valkey 9+ only. Skip if server rejects. *)
  with_client @@ fun c ->
  let key = "ocaml:c:delifeq" in
  let _ = C.del c [ key ] in
  ignore (C.set c key "match");
  (match C.delifeq c key ~value:"match" with
   | Ok true -> ()
   | Ok false -> Alcotest.fail "DELIFEQ match should delete"
   | Error _ -> (* older server; skip the remainder *) ());
  let _ = C.del c [ key ] in
  ()

let test_set_and_get () =
  with_client @@ fun c ->
  let key = "ocaml:c:setget" in
  let _ = C.del c [ key ] in
  (match C.set_and_get c key "v1" with
   | Ok None -> ()
   | other -> Alcotest.failf "set_and_get (first): %s"
       (match other with
        | Ok None -> "None"
        | Ok (Some s) -> Printf.sprintf "Some %S" s
        | Error e -> Format.asprintf "Error %a" E.pp e));
  (match C.set_and_get c key "v2" with
   | Ok (Some "v1") -> ()
   | other -> Alcotest.failf "set_and_get (second): %s"
       (match other with
        | Ok None -> "None"
        | Ok (Some s) -> Printf.sprintf "Some %S" s
        | Error e -> Format.asprintf "Error %a" E.pp e));
  let _ = C.del c [ key ] in
  ()

let tests =
  [ Alcotest.test_case "exec PING" `Quick test_exec_ping;
    Alcotest.test_case "GET missing returns None" `Quick test_get_missing;
    Alcotest.test_case "SET / GET / DEL roundtrip" `Quick test_set_get_del;
    Alcotest.test_case "SET NX conflict returns false" `Quick
      test_set_nx_conflict;
    Alcotest.test_case "SET with ex_seconds" `Quick test_set_with_expiry;
    Alcotest.test_case "DEL multiple keys" `Quick test_del_multiple;
    Alcotest.test_case "UNLINK" `Quick test_unlink;
    Alcotest.test_case "SETEX / SETNX / TTL" `Quick test_setex_setnx_and_ttl;
    Alcotest.test_case "TTL Absent/Persistent" `Quick
      test_ttl_absent_and_persistent;
    Alcotest.test_case "EXPIRE / PEXPIRE / PTTL" `Quick test_expire_pexpire;
    Alcotest.test_case "INCR / INCRBY" `Quick test_incr_incrby;
    Alcotest.test_case "MGET mixed" `Quick test_mget;
    Alcotest.test_case "TYPE" `Quick test_type_of;
    Alcotest.test_case "SET IFEQ (needs Valkey 8.1+)" `Quick test_set_ifeq;
    Alcotest.test_case "DELIFEQ (needs Valkey 9+)" `Quick test_delifeq;
    Alcotest.test_case "SET ... GET variant" `Quick test_set_and_get;
  ]
