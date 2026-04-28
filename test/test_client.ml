module C = Valkey.Client
module E = Valkey.Connection.Error
module R = Valkey.Resp3
module BP = Valkey.Blocking_pool

let host = "localhost"
let port = 6379

(* Client for non-blocking tests. *)
let with_client f =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let c = C.connect ~sw ~net ~clock ~host ~port () in
  let r = f c in
  C.close c;
  r

(* Client for blocking tests: enables the Blocking_pool with a
   small per-node capacity so BLPOP/BRPOP/BLMOVE/XREAD BLOCK
   have a lease bucket. *)
let with_blocking_client f =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config = {
    C.Config.default with
    blocking_pool = {
      BP.Config.default with
      max_per_node = 2;
      on_exhaustion = `Block;
      borrow_timeout = Some 2.0;
    };
  } in
  let c = C.connect ~sw ~net ~clock ~config ~host ~port () in
  let r = f c in
  C.close c;
  r

let blocking_err_str = function
  | Ok _ -> "Ok _"
  | Error e -> Format.asprintf "Error %a" C.pp_blocking_error e

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
  (match C.set c key "a" ~cond:Set_nx with
   | Ok true -> ()
   | other -> Alcotest.failf "first NX should succeed: %s"
       (match other with
        | Ok b -> Printf.sprintf "Ok %b" b
        | Error e -> Format.asprintf "Error %a" E.pp e));
  (match C.set c key "b" ~cond:Set_nx with
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
  (match C.set c key "tmp" ~ttl:(Set_ex_seconds 5) with
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
    Alcotest.test_case "HSET / HGET / HDEL" `Quick (fun () ->
      with_client @@ fun c ->
      let key = "ocaml:c:h1" in
      let _ = C.del c [ key ] in
      (match C.hset c key [ "a", "1"; "b", "2" ] with
       | Ok 2 -> ()
       | other ->
           Alcotest.failf "HSET: %s"
             (match other with
              | Ok n -> string_of_int n
              | Error e -> Format.asprintf "Error %a" E.pp e));
      (match C.hget c key "a" with
       | Ok (Some "1") -> ()
       | other ->
           Alcotest.failf "HGET: %s"
             (match other with
              | Ok None -> "None"
              | Ok (Some s) -> Printf.sprintf "Some %S" s
              | Error e -> Format.asprintf "Error %a" E.pp e));
      (match C.hget c key "nope" with
       | Ok None -> ()
       | other -> Alcotest.failf "HGET missing should be None (%s)"
           (match other with
            | Ok (Some s) -> Printf.sprintf "Some %S" s
            | Ok None -> "impossible"
            | Error e -> Format.asprintf "Error %a" E.pp e));
      let _ = C.del c [ key ] in
      ());
    Alcotest.test_case "HGETALL" `Quick (fun () ->
      with_client @@ fun c ->
      let key = "ocaml:c:h2" in
      let _ = C.del c [ key ] in
      ignore (C.hset c key [ "x", "1"; "y", "2" ]);
      (match C.hgetall c key with
       | Ok kvs ->
           let sorted =
             List.sort (fun (a, _) (b, _) -> compare a b) kvs
           in
           Alcotest.(check (list (pair string string)))
             "hgetall sorted"
             [ "x", "1"; "y", "2" ] sorted
       | Error e -> Alcotest.failf "HGETALL: %a" E.pp e);
      (match C.hgetall c "ocaml:c:h_missing" with
       | Ok [] -> ()
       | Ok xs ->
           Alcotest.failf "HGETALL on missing: expected [], got %d items"
             (List.length xs)
       | Error e -> Alcotest.failf "HGETALL missing: %a" E.pp e);
      let _ = C.del c [ key ] in
      ());
    Alcotest.test_case "HMGET mixed" `Quick (fun () ->
      with_client @@ fun c ->
      let key = "ocaml:c:h3" in
      let _ = C.del c [ key ] in
      ignore (C.hset c key [ "a", "A"; "c", "C" ]);
      (match C.hmget c key [ "a"; "b"; "c" ] with
       | Ok [ Some "A"; None; Some "C" ] -> ()
       | other ->
           Alcotest.failf "HMGET unexpected: %s"
             (match other with
              | Ok vs ->
                  "["
                  ^ String.concat "; "
                      (List.map (function
                         | None -> "None"
                         | Some s -> Printf.sprintf "Some %S" s)
                         vs)
                  ^ "]"
              | Error e -> Format.asprintf "Error %a" E.pp e));
      let _ = C.del c [ key ] in
      ());
    Alcotest.test_case "HEXPIRE / HTTL / HPERSIST" `Quick (fun () ->
      with_client @@ fun c ->
      let key = "ocaml:c:hftl1" in
      let _ = C.del c [ key ] in
      ignore (C.hset c key [ "a", "1"; "b", "2" ]);
      (match C.hexpire c key ~seconds:60 [ "a"; "b"; "nope" ] with
       | Ok [ Hfield_ttl_set; Hfield_ttl_set; Hfield_missing ] -> ()
       | Ok other ->
           Alcotest.failf "HEXPIRE: unexpected pattern (%d items)"
             (List.length other)
       | Error e -> Alcotest.failf "HEXPIRE: %a" E.pp e);
      (match C.httl c key [ "a"; "nope"; "b" ] with
       | Ok [ Expires_in n; Absent; Expires_in m ]
         when n > 30 && n <= 60 && m > 30 && m <= 60 -> ()
       | Ok _ -> Alcotest.fail "HTTL values out of expected range"
       | Error e -> Alcotest.failf "HTTL: %a" E.pp e);
      (match C.hpersist c key [ "a"; "nope" ] with
       | Ok [ Persist_ttl_removed; Persist_field_missing ] -> ()
       | Ok _ -> Alcotest.fail "HPERSIST unexpected pattern"
       | Error e -> Alcotest.failf "HPERSIST: %a" E.pp e);
      (match C.httl c key [ "a" ] with
       | Ok [ Persistent ] -> ()
       | Ok _ -> Alcotest.fail "field should be Persistent after HPERSIST"
       | Error e -> Alcotest.failf "HTTL: %a" E.pp e);
      let _ = C.del c [ key ] in
      ());
    Alcotest.test_case "HSETEX + HGETEX" `Quick (fun () ->
      with_client @@ fun c ->
      let key = "ocaml:c:hsetex" in
      let _ = C.del c [ key ] in
      (match C.hsetex c key ~ttl:(Hse_ex_seconds 60) [ "a", "1"; "b", "2" ] with
       | Ok true -> ()
       | Ok false -> Alcotest.fail "HSETEX: returned false unexpectedly"
       | Error e -> Alcotest.failf "HSETEX: %a" E.pp e);
      (match C.httl c key [ "a" ] with
       | Ok [ Expires_in n ] when n > 30 && n <= 60 -> ()
       | Ok _ -> Alcotest.fail "expected Expires_in 30..60"
       | Error e -> Alcotest.failf "HTTL: %a" E.pp e);
      (match C.hgetex c key ~ttl:Hge_persist [ "a"; "missing" ] with
       | Ok [ Some "1"; None ] -> ()
       | Ok _ -> Alcotest.fail "HGETEX unexpected"
       | Error e -> Alcotest.failf "HGETEX: %a" E.pp e);
      (match C.httl c key [ "a" ] with
       | Ok [ Persistent ] -> ()
       | Ok _ -> Alcotest.fail "field should be persistent after HGETEX PERSIST"
       | Error e -> Alcotest.failf "HTTL: %a" E.pp e);
      let _ = C.del c [ key ] in
      ());
    Alcotest.test_case "HEXPIRE with 0 sec triggers immediate expiry" `Quick
      (fun () ->
      with_client @@ fun c ->
      let key = "ocaml:c:hftl0" in
      let _ = C.del c [ key ] in
      ignore (C.hset c key [ "f", "v" ]);
      (match C.hexpire c key ~seconds:0 [ "f" ] with
       | Ok [ Hfield_expired_now ] -> ()
       | Ok _ -> Alcotest.fail "expected Hfield_expired_now"
       | Error e -> Alcotest.failf "HEXPIRE 0: %a" E.pp e);
      (* field should be gone now *)
      (match C.hget c key "f" with
       | Ok None -> ()
       | Ok (Some s) -> Alcotest.failf "expected None, got Some %S" s
       | Error e -> Alcotest.failf "HGET: %a" E.pp e);
      let _ = C.del c [ key ] in
      ());
    Alcotest.test_case "SADD / SCARD / SMEMBERS / SISMEMBER" `Quick (fun () ->
      with_client @@ fun c ->
      let key = "ocaml:c:set" in
      let _ = C.del c [ key ] in
      Alcotest.(check int) "SADD 3 new" 3
        (match C.sadd c key [ "a"; "b"; "c" ] with
         | Ok n -> n | Error e -> Alcotest.failf "SADD: %a" E.pp e);
      Alcotest.(check int) "SADD dup returns 1" 1
        (match C.sadd c key [ "a"; "d" ] with
         | Ok n -> n | Error e -> Alcotest.failf "SADD: %a" E.pp e);
      Alcotest.(check int) "SCARD" 4
        (match C.scard c key with
         | Ok n -> n | Error e -> Alcotest.failf "SCARD: %a" E.pp e);
      (match C.sismember c key "a" with
       | Ok true -> () | Ok false -> Alcotest.fail "SISMEMBER a false"
       | Error e -> Alcotest.failf "SISMEMBER: %a" E.pp e);
      (match C.sismember c key "zzz" with
       | Ok false -> () | Ok true -> Alcotest.fail "SISMEMBER zzz true"
       | Error e -> Alcotest.failf "SISMEMBER: %a" E.pp e);
      (match C.smembers c key with
       | Ok members ->
           let sorted = List.sort compare members in
           Alcotest.(check (list string)) "members sorted"
             [ "a"; "b"; "c"; "d" ] sorted
       | Error e -> Alcotest.failf "SMEMBERS: %a" E.pp e);
      let _ = C.del c [ key ] in
      ());
    Alcotest.test_case "LPUSH / RPUSH / LRANGE / LLEN / LPOP / RPOP" `Quick
      (fun () ->
      with_client @@ fun c ->
      let key = "ocaml:c:list" in
      let _ = C.del c [ key ] in
      Alcotest.(check int) "LPUSH b a -> len 2" 2
        (match C.lpush c key [ "b"; "a" ] with
         | Ok n -> n | Error e -> Alcotest.failf "LPUSH: %a" E.pp e);
      (* after LPUSH b a: list is [a; b] (each pushed to head, so a ends up front) *)
      Alcotest.(check int) "RPUSH c d -> len 4" 4
        (match C.rpush c key [ "c"; "d" ] with
         | Ok n -> n | Error e -> Alcotest.failf "RPUSH: %a" E.pp e);
      (* list now: [a; b; c; d] *)
      Alcotest.(check int) "LLEN" 4
        (match C.llen c key with
         | Ok n -> n | Error e -> Alcotest.failf "LLEN: %a" E.pp e);
      (match C.lrange c key ~start:0 ~stop:(-1) with
       | Ok xs -> Alcotest.(check (list string)) "LRANGE 0 -1"
           [ "a"; "b"; "c"; "d" ] xs
       | Error e -> Alcotest.failf "LRANGE: %a" E.pp e);
      (match C.lpop c key with
       | Ok (Some "a") -> ()
       | other -> Alcotest.failf "LPOP: %s"
           (match other with
            | Ok None -> "None"
            | Ok (Some s) -> Printf.sprintf "Some %S" s
            | Error e -> Format.asprintf "Error %a" E.pp e));
      (match C.rpop c key with
       | Ok (Some "d") -> ()
       | other -> Alcotest.failf "RPOP: %s"
           (match other with
            | Ok None -> "None"
            | Ok (Some s) -> Printf.sprintf "Some %S" s
            | Error e -> Format.asprintf "Error %a" E.pp e));
      (match C.lpop_n c key 5 with
       | Ok [ "b"; "c" ] -> ()
       | Ok xs -> Alcotest.failf "LPOP 5 from 2-elem: got %d items"
           (List.length xs)
       | Error e -> Alcotest.failf "LPOP_n: %a" E.pp e);
      (match C.lpop c key with
       | Ok None -> ()
       | other -> Alcotest.failf "LPOP empty: %s"
           (match other with
            | Ok (Some s) -> Printf.sprintf "Some %S" s
            | Ok None -> "impossible"
            | Error e -> Format.asprintf "Error %a" E.pp e));
      let _ = C.del c [ key ] in
      ());
    Alcotest.test_case "ZADD (via exec) / ZRANGE / ZRANGEBYSCORE / ZCARD / ZREMRANGEBYSCORE"
      `Quick (fun () ->
      with_client @@ fun c ->
      let key = "ocaml:c:zset" in
      let _ = C.del c [ key ] in
      (* Seed via exec since ZADD isn't in the typed API yet. *)
      let _ = C.exec c [| "ZADD"; key; "1"; "a"; "2"; "b"; "3"; "c"; "10"; "d" |] in
      Alcotest.(check int) "ZCARD" 4
        (match C.zcard c key with
         | Ok n -> n | Error e -> Alcotest.failf "ZCARD: %a" E.pp e);
      (match C.zrange c key ~start:0 ~stop:(-1) with
       | Ok xs -> Alcotest.(check (list string))
           "zrange ascending" [ "a"; "b"; "c"; "d" ] xs
       | Error e -> Alcotest.failf "ZRANGE: %a" E.pp e);
      (match C.zrange c key ~rev:true ~start:0 ~stop:(-1) with
       | Ok xs -> Alcotest.(check (list string))
           "zrange rev" [ "d"; "c"; "b"; "a" ] xs
       | Error e -> Alcotest.failf "ZRANGE REV: %a" E.pp e);
      (match
         C.zrangebyscore c key ~min:(Score 2.0) ~max:(Score 3.0)
       with
       | Ok xs -> Alcotest.(check (list string))
           "zrangebyscore 2..3" [ "b"; "c" ] xs
       | Error e -> Alcotest.failf "ZRANGEBYSCORE: %a" E.pp e);
      (match
         C.zrangebyscore c key ~min:Score_neg_inf ~max:(Score_excl 3.0)
       with
       | Ok xs -> Alcotest.(check (list string))
           "zrangebyscore -inf..(3" [ "a"; "b" ] xs
       | Error e -> Alcotest.failf "ZRANGEBYSCORE: %a" E.pp e);
      (match C.zremrangebyscore c key ~min:(Score 3.0) ~max:Score_pos_inf with
       | Ok 2 -> ()
       | other ->
           Alcotest.failf "ZREMRANGEBYSCORE: %s"
             (match other with
              | Ok n -> string_of_int n
              | Error e -> Format.asprintf "Error %a" E.pp e));
      Alcotest.(check int) "ZCARD after remove" 2
        (match C.zcard c key with
         | Ok n -> n | Error e -> Alcotest.failf "ZCARD: %a" E.pp e);
      let _ = C.del c [ key ] in
      ());
    Alcotest.test_case "SCRIPT LOAD / EVALSHA / SCRIPT EXISTS / EVAL" `Quick
      (fun () ->
      with_client @@ fun c ->
      let script = "return redis.call('GET', KEYS[1])" in
      let key = "ocaml:c:script" in
      ignore (C.set c key "hello");
      let sha =
        match C.script_load c script with
        | Ok s -> s
        | Error e -> Alcotest.failf "SCRIPT LOAD: %a" E.pp e
      in
      Alcotest.(check (list bool)) "SCRIPT EXISTS"
        [ true; false ]
        (match C.script_exists c [ sha; "deadbeef" ^ String.make 32 '0' ] with
         | Ok bs -> bs | Error e -> Alcotest.failf "SCRIPT EXISTS: %a" E.pp e);
      (match C.evalsha c ~sha ~keys:[ key ] ~args:[] with
       | Ok (R.Bulk_string "hello") -> ()
       | other ->
           Alcotest.failf "EVALSHA: %s"
             (match other with
              | Ok v -> Format.asprintf "Ok %a" R.pp v
              | Error e -> Format.asprintf "Error %a" E.pp e));
      (match C.eval c ~script ~keys:[ key ] ~args:[] with
       | Ok (R.Bulk_string "hello") -> ()
       | other ->
           Alcotest.failf "EVAL: %s"
             (match other with
              | Ok v -> Format.asprintf "Ok %a" R.pp v
              | Error e -> Format.asprintf "Error %a" E.pp e));
      let _ = C.del c [ key ] in
      ());
    Alcotest.test_case "SCAN + KEYS" `Quick (fun () ->
      with_client @@ fun c ->
      let prefix = "ocaml:c:scan:" in
      let n = 25 in
      let keys = List.init n (fun i -> prefix ^ string_of_int i) in
      List.iter (fun k -> ignore (C.set c k "v")) keys;
      (* SCAN iterates until cursor = "0" *)
      let rec drain cursor acc =
        match C.scan c ~cursor ~match_:(prefix ^ "*") ~count:10 with
        | Error e -> Alcotest.failf "SCAN: %a" E.pp e
        | Ok page ->
            let acc = page.keys @ acc in
            if page.cursor = "0" then acc else drain page.cursor acc
      in
      let found = drain "0" [] in
      let dedup =
        List.sort_uniq compare found |> List.filter (fun k ->
          String.length k >= String.length prefix
          && String.sub k 0 (String.length prefix) = prefix)
      in
      Alcotest.(check int) "SCAN found all seeded keys" n (List.length dedup);
      (match C.keys c (prefix ^ "*") with
       | Ok ks ->
           Alcotest.(check int) "KEYS found all seeded" n (List.length ks)
       | Error e -> Alcotest.failf "KEYS: %a" E.pp e);
      List.iter (fun k -> ignore (C.del c [ k ])) keys);
    Alcotest.test_case "streams: XADD / XLEN / XRANGE / XREAD" `Quick
      (fun () ->
      with_client @@ fun c ->
      let key = "ocaml:c:stream" in
      let _ = C.del c [ key ] in
      let id1 =
        match C.xadd c key [ "a", "1" ] with
        | Ok s -> s | Error e -> Alcotest.failf "XADD 1: %a" E.pp e
      in
      let id2 =
        match C.xadd c key [ "a", "2"; "b", "3" ] with
        | Ok s -> s | Error e -> Alcotest.failf "XADD 2: %a" E.pp e
      in
      Alcotest.(check int) "XLEN" 2
        (match C.xlen c key with
         | Ok n -> n | Error e -> Alcotest.failf "XLEN: %a" E.pp e);
      (match C.xrange c key ~start:"-" ~end_:"+" with
       | Ok [ e1; e2 ] ->
           Alcotest.(check string) "first id" id1 e1.id;
           Alcotest.(check string) "second id" id2 e2.id;
           Alcotest.(check (list (pair string string)))
             "first fields" [ "a", "1" ] e1.fields;
           Alcotest.(check (list (pair string string)))
             "second fields" [ "a", "2"; "b", "3" ] e2.fields
       | Ok xs ->
           Alcotest.failf "XRANGE: expected 2 entries, got %d"
             (List.length xs)
       | Error e -> Alcotest.failf "XRANGE: %a" E.pp e);
      (match C.xread c ~streams:[ key, "0" ] with
       | Ok [ (k, entries) ] when k = key ->
           Alcotest.(check int) "XREAD entries" 2 (List.length entries)
       | Ok _ -> Alcotest.fail "XREAD unexpected structure"
       | Error e -> Alcotest.failf "XREAD: %a" E.pp e);
      let _ = C.del c [ key ] in
      ());
    Alcotest.test_case "streams: XDEL / XTRIM" `Quick (fun () ->
      with_client @@ fun c ->
      let key = "ocaml:c:stream2" in
      let _ = C.del c [ key ] in
      let ids =
        List.init 5 (fun i ->
          match C.xadd c key [ "i", string_of_int i ] with
          | Ok s -> s | Error e -> Alcotest.failf "XADD: %a" E.pp e)
      in
      (match C.xdel c key [ List.nth ids 2 ] with
       | Ok 1 -> ()
       | other -> Alcotest.failf "XDEL: %s"
           (match other with
            | Ok n -> string_of_int n
            | Error e -> Format.asprintf "Error %a" E.pp e));
      (match
         C.xtrim c key (Xtrim_maxlen { approx = false; threshold = 2 })
       with
       | Ok n when n >= 1 -> ()
       | Ok n -> Alcotest.failf "XTRIM: expected >=1 removed, got %d" n
       | Error e -> Alcotest.failf "XTRIM: %a" E.pp e);
      let _ = C.del c [ key ] in
      ());
    Alcotest.test_case "streams: XGROUP / XREADGROUP / XACK" `Quick
      (fun () ->
      with_client @@ fun c ->
      let key = "ocaml:c:sg" in
      let group = "g1" in
      let consumer = "c1" in
      let _ = C.del c [ key ] in
      let _ =
        match
          C.xgroup_create c key ~group ~id:"0" ~opts:[ Xgroup_mkstream ]
        with
        | Ok () -> ()
        | Error e -> Alcotest.failf "XGROUP CREATE: %a" E.pp e
      in
      let id =
        match C.xadd c key [ "k", "v" ] with
        | Ok s -> s | Error e -> Alcotest.failf "XADD: %a" E.pp e
      in
      (match
         C.xreadgroup c ~group ~consumer ~streams:[ key, ">" ]
       with
       | Ok [ (k, [ e ]) ] when k = key && e.id = id -> ()
       | Ok xs ->
           Alcotest.failf "XREADGROUP unexpected (%d streams)"
             (List.length xs)
       | Error e -> Alcotest.failf "XREADGROUP: %a" E.pp e);
      (match C.xack c key ~group [ id ] with
       | Ok 1 -> ()
       | other -> Alcotest.failf "XACK: %s"
           (match other with
            | Ok n -> string_of_int n
            | Error e -> Format.asprintf "Error %a" E.pp e));
      (match C.xgroup_destroy c key ~group with
       | Ok true -> ()
       | other -> Alcotest.failf "XGROUP DESTROY: %s"
           (match other with
            | Ok b -> Printf.sprintf "Ok %b" b
            | Error e -> Format.asprintf "Error %a" E.pp e));
      let _ = C.del c [ key ] in
      ());
    Alcotest.test_case "eval_script: first EVAL, second EVALSHA, NOSCRIPT fallback" `Quick
      (fun () ->
      with_client @@ fun c ->
      let key = "ocaml:c:es" in
      ignore (C.set c key "hi");
      let s = C.Script.create "return redis.call('GET', KEYS[1])" in
      (* First invocation: server hasn't seen it — EVAL path *)
      (match C.eval_script c s ~keys:[ key ] ~args:[] with
       | Ok (R.Bulk_string "hi") -> ()
       | other ->
           Alcotest.failf "eval_script 1: %s"
             (match other with
              | Ok v -> Format.asprintf "Ok %a" R.pp v
              | Error e -> Format.asprintf "Error %a" E.pp e));
      (* Second invocation: EVALSHA (same result) *)
      (match C.eval_script c s ~keys:[ key ] ~args:[] with
       | Ok (R.Bulk_string "hi") -> ()
       | other -> Alcotest.failf "eval_script 2: %s"
           (match other with
            | Ok v -> Format.asprintf "Ok %a" R.pp v
            | Error e -> Format.asprintf "Error %a" E.pp e));
      (* Force server flush via raw command so OUR cache stays stale *)
      ignore (C.exec c [| "SCRIPT"; "FLUSH" |]);
      (* Next eval_script: our cache thinks loaded, but server doesn't.
         EVALSHA returns NOSCRIPT, fallback to EVAL. *)
      (match C.eval_script c s ~keys:[ key ] ~args:[] with
       | Ok (R.Bulk_string "hi") -> ()
       | other -> Alcotest.failf "eval_script after flush: %s"
           (match other with
            | Ok v -> Format.asprintf "Ok %a" R.pp v
            | Error e -> Format.asprintf "Error %a" E.pp e));
      (* Typed script_flush also clears our cache *)
      ignore (C.script_flush c);
      (match C.eval_script c s ~keys:[ key ] ~args:[] with
       | Ok (R.Bulk_string "hi") -> ()
       | other -> Alcotest.failf "eval_script after typed flush: %s"
           (match other with
            | Ok v -> Format.asprintf "Ok %a" R.pp v
            | Error e -> Format.asprintf "Error %a" E.pp e));
      let _ = C.del c [ key ] in
      ());
    Alcotest.test_case "eval_cached (one-shot)" `Quick (fun () ->
      with_client @@ fun c ->
      let key = "ocaml:c:ec" in
      ignore (C.set c key "42");
      (match
         C.eval_cached c
           ~script:"return redis.call('GET', KEYS[1])"
           ~keys:[ key ] ~args:[]
       with
       | Ok (R.Bulk_string "42") -> ()
       | other -> Alcotest.failf "eval_cached: %s"
           (match other with
            | Ok v -> Format.asprintf "Ok %a" R.pp v
            | Error e -> Format.asprintf "Error %a" E.pp e));
      let _ = C.del c [ key ] in
      ());
    Alcotest.test_case "stream admin: XPENDING / XCLAIM / XAUTOCLAIM / XINFO"
      `Quick (fun () ->
      with_client @@ fun c ->
      let key = "ocaml:c:admin" in
      let group = "g" in
      let c1 = "alice" in
      let c2 = "bob" in
      let _ = C.del c [ key ] in
      let _ =
        match
          C.xgroup_create c key ~group ~id:"0" ~opts:[ Xgroup_mkstream ]
        with
        | Ok () -> ()
        | Error e -> Alcotest.failf "XGROUP CREATE: %a" E.pp e
      in
      let id =
        match C.xadd c key [ "k", "v" ] with
        | Ok s -> s | Error e -> Alcotest.failf "XADD: %a" E.pp e
      in
      (* Alice reads but doesn't ACK *)
      let _ =
        match C.xreadgroup c ~group ~consumer:c1 ~streams:[ key, ">" ] with
        | Ok _ -> ()
        | Error e -> Alcotest.failf "XREADGROUP: %a" E.pp e
      in
      (* XPENDING summary: 1 pending, alice has 1 *)
      (match C.xpending c key ~group with
       | Ok s ->
           Alcotest.(check int) "count" 1 s.count;
           Alcotest.(check (option string)) "min" (Some id) s.min_id;
           Alcotest.(check (option string)) "max" (Some id) s.max_id;
           (match s.consumers with
            | [ (name, 1) ] when name = c1 -> ()
            | _ -> Alcotest.fail "consumers shape unexpected")
       | Error e -> Alcotest.failf "XPENDING: %a" E.pp e);
      (* XPENDING range *)
      (match
         C.xpending_range c key ~group ~start:"-" ~end_:"+" ~count:10
       with
       | Ok [ e ] ->
           Alcotest.(check string) "entry id" id e.id;
           Alcotest.(check string) "entry consumer" c1 e.consumer
       | Ok xs ->
           Alcotest.failf "expected 1 pending entry, got %d" (List.length xs)
       | Error e -> Alcotest.failf "XPENDING range: %a" E.pp e);
      (* XCLAIM to bob (min_idle_ms=0 so claim succeeds immediately) *)
      (match
         C.xclaim c key ~group ~consumer:c2 ~min_idle_ms:0 ~ids:[ id ]
       with
       | Ok [ e ] ->
           Alcotest.(check string) "claimed id" id e.id;
           Alcotest.(check (list (pair string string)))
             "claimed fields" [ "k", "v" ] e.fields
       | Ok xs ->
           Alcotest.failf "XCLAIM: expected 1, got %d" (List.length xs)
       | Error e -> Alcotest.failf "XCLAIM: %a" E.pp e);
      (* XCLAIM JUSTID *)
      (match
         C.xclaim_ids c key ~group ~consumer:c1 ~min_idle_ms:0 ~ids:[ id ]
       with
       | Ok [ only_id ] -> Alcotest.(check string) "just-id" id only_id
       | Ok ids ->
           Alcotest.failf "XCLAIM JUSTID: expected 1 id, got %d"
             (List.length ids)
       | Error e -> Alcotest.failf "XCLAIM JUSTID: %a" E.pp e);
      (* XAUTOCLAIM from cursor 0-0 *)
      (match
         C.xautoclaim c key ~group ~consumer:c2 ~min_idle_ms:0 ~cursor:"0-0"
       with
       | Ok r ->
           (* Claimed list should contain our entry *)
           let claimed_ids = List.map (fun (e : C.stream_entry) -> e.id) r.claimed in
           Alcotest.(check bool) "autoclaim contains id" true
             (List.mem id claimed_ids)
       | Error e -> Alcotest.failf "XAUTOCLAIM: %a" E.pp e);
      (* XINFO GROUPS returns at least one entry *)
      (match C.xinfo_groups c key with
       | Ok xs ->
           Alcotest.(check bool) "at least one group" true (xs <> [])
       | Error e -> Alcotest.failf "XINFO GROUPS: %a" E.pp e);
      (* XINFO CONSUMERS returns at least one *)
      (match C.xinfo_consumers c key ~group with
       | Ok xs ->
           Alcotest.(check bool) "at least one consumer" true (xs <> [])
       | Error e -> Alcotest.failf "XINFO CONSUMERS: %a" E.pp e);
      (* XINFO STREAM returns something parseable *)
      (match C.xinfo_stream c key with
       | Ok _ -> ()
       | Error e -> Alcotest.failf "XINFO STREAM: %a" E.pp e);
      let _ = C.del c [ key ] in
      ());
    Alcotest.test_case "blocking: BLPOP / BRPOP with data ready" `Quick
      (fun () ->
      with_blocking_client @@ fun c ->
      let key = "ocaml:c:block:l" in
      let _ = C.del c [ key ] in
      ignore (C.rpush c key [ "a"; "b"; "c" ]);
      (match C.blpop c ~keys:[ key ] ~block_seconds:0.1 with
       | Ok (Some (k, v)) ->
           Alcotest.(check string) "key" key k;
           Alcotest.(check string) "popped left" "a" v
       | Ok None -> Alcotest.fail "BLPOP unexpectedly timed out"
       | Error e ->
           Alcotest.failf "BLPOP: %a" C.pp_blocking_error e);
      (match C.brpop c ~keys:[ key ] ~block_seconds:0.1 with
       | Ok (Some (k, v)) ->
           Alcotest.(check string) "key" key k;
           Alcotest.(check string) "popped right" "c" v
       | Ok None -> Alcotest.fail "BRPOP unexpectedly timed out"
       | Error e ->
           Alcotest.failf "BRPOP: %a" C.pp_blocking_error e);
      let _ = C.del c [ key ] in
      ());
    Alcotest.test_case "blocking: BLPOP timeout on empty" `Quick (fun () ->
      with_blocking_client @@ fun c ->
      let key = "ocaml:c:block:empty" in
      let _ = C.del c [ key ] in
      (match C.blpop c ~keys:[ key ] ~block_seconds:0.1 with
       | Ok None -> ()
       | Ok (Some (_, _)) -> Alcotest.fail "BLPOP should have timed out"
       | Error e ->
           Alcotest.failf "BLPOP: %a" C.pp_blocking_error e));
    Alcotest.test_case "blocking: BLMOVE timeout on empty source" `Quick (fun () ->
      with_blocking_client @@ fun c ->
      let src = "ocaml:c:blmove:empty:src" in
      let dst = "ocaml:c:blmove:empty:dst" in
      let _ = C.del c [ src; dst ] in
      (* Empty source list, server parks us for ~block_seconds
         and returns Null on timeout. Routed through the
         Blocking_pool so the lease-conn carries this wait
         instead of the multiplexed conn — the trailing PING
         (on the multiplexed path) confirms other fibers were
         never blocked. *)
      (match
         C.blmove c ~source:src ~destination:dst
           ~from:Left ~to_:Right ~block_seconds:0.1
       with
       | Ok None -> ()
       | Ok (Some s) ->
           Alcotest.failf "BLMOVE on empty src: expected timeout, got Some %S" s
       | Error e ->
           Alcotest.failf "BLMOVE: %a" C.pp_blocking_error e);
      (match C.exec c [| "PING" |] with
       | Ok (R.Simple_string "PONG") -> ()
       | other ->
           Alcotest.failf
             "post-timeout PING should be PONG (multiplexed conn healthy); got %s"
             (match other with
              | Ok v -> Format.asprintf "%a" R.pp v
              | Error e -> Format.asprintf "Error %a" E.pp e)));
    Alcotest.test_case "LMOVE / BLMOVE with data" `Quick (fun () ->
      with_blocking_client @@ fun c ->
      let src = "ocaml:c:src" in
      let dst = "ocaml:c:dst" in
      let _ = C.del c [ src; dst ] in
      ignore (C.rpush c src [ "x"; "y"; "z" ]);
      (match C.lmove c ~source:src ~destination:dst ~from:Left ~to_:Right with
       | Ok (Some "x") -> ()
       | other -> Alcotest.failf "LMOVE: %s"
           (match other with
            | Ok None -> "None"
            | Ok (Some s) -> Printf.sprintf "Some %S" s
            | Error e -> Format.asprintf "Error %a" E.pp e));
      (match
         C.blmove c ~source:src ~destination:dst
           ~from:Right ~to_:Left ~block_seconds:0.1
       with
       | Ok (Some "z") -> ()
       | other -> Alcotest.failf "BLMOVE: %s" (blocking_err_str other));
      let _ = C.del c [ src; dst ] in
      ());
    Alcotest.test_case "WAIT returns Wait_needs_dedicated_conn" `Quick (fun () ->
      with_blocking_client @@ fun c ->
      (* WAIT can't be run correctly on a multiplexed client.
         The typed error directs users to the dedicated-conn
         escape hatch (Commit 4). *)
      (match C.wait_replicas c ~num_replicas:0 ~block_ms:100 with
       | Error (C.Wait_needs_dedicated_conn _) -> ()
       | Ok n ->
           Alcotest.failf
             "WAIT: expected Wait_needs_dedicated_conn, got Ok %d" n
       | Error e ->
           Alcotest.failf "WAIT: expected Wait_needs_dedicated_conn, got %a"
             C.pp_blocking_error e));
    Alcotest.test_case "XREAD BLOCK timeout on empty stream" `Quick (fun () ->
      with_blocking_client @@ fun c ->
      let key = "ocaml:c:xread:empty" in
      let _ = C.del c [ key ] in
      (match
         C.xread_block c ~block_ms:100 ~streams:[ key, "$" ]
       with
       | Ok [] -> ()
       | Ok _ -> Alcotest.fail "XREAD BLOCK should have timed out"
       | Error e ->
           Alcotest.failf "XREAD BLOCK: %a" C.pp_blocking_error e));
    Alcotest.test_case "XREADGROUP BLOCK timeout leaves consumer state intact"
      `Quick (fun () ->
      with_blocking_client @@ fun c ->
      let key = "ocaml:c:xrg:block" in
      let group = "g1" in
      let consumer = "c1" in
      let _ = C.del c [ key ] in
      (match
         C.xgroup_create c key ~group ~id:"0" ~opts:[ Xgroup_mkstream ]
       with
       | Ok () -> ()
       | Error e -> Alcotest.failf "XGROUP CREATE: %a" E.pp e);
      (* No new messages; ">" id, BLOCK 100ms. Server returns nil
         (Ok []) on timeout. The load-bearing assertion: a real
         server-side group entry exists for [consumer] AFTER the
         block times out — i.e. registering the consumer didn't
         depend on returning a message, and timing out didn't tear
         the group state down. A failure mode where the client
         abandons the in-flight reply but the server still
         registered + then expired the consumer would surface as
         an empty XINFO CONSUMERS list. *)
      (match
         C.xreadgroup_block c ~block_ms:100 ~group ~consumer
           ~streams:[ key, ">" ]
       with
       | Ok [] -> ()
       | Ok _ ->
           Alcotest.fail "XREADGROUP BLOCK should have timed out (no new messages)"
       | Error e ->
           Alcotest.failf "XREADGROUP BLOCK: %a" C.pp_blocking_error e);
      (match C.xinfo_consumers c key ~group with
       | Ok xs ->
           let extract_name = function
             | R.Map kvs ->
                 List.find_map
                   (fun (k, v) -> match k, v with
                      | R.Bulk_string "name", R.Bulk_string n -> Some n
                      | _ -> None)
                   kvs
             | _ -> None
           in
           let names = List.filter_map extract_name xs in
           if not (List.mem consumer names) then
             Alcotest.failf
               "consumer %S not in group after BLOCK timeout (got [%s])"
               consumer (String.concat ", " names)
       | Error e -> Alcotest.failf "XINFO CONSUMERS: %a" E.pp e);
      (* Matching FIFO sanity: a subsequent command must return
         its own reply, not a stale reply from the timed-out
         BLOCK. *)
      (match C.exec c [| "PING" |] with
       | Ok (R.Simple_string "PONG") -> ()
       | other ->
           Alcotest.failf
             "post-timeout PING expected PONG, got %s"
             (match other with
              | Ok v -> Format.asprintf "%a" R.pp v
              | Error e -> Format.asprintf "Error %a" E.pp e));
      let _ = C.del c [ key ] in
      ());
    Alcotest.test_case "HINCRBY" `Quick (fun () ->
      with_client @@ fun c ->
      let key = "ocaml:c:h4" in
      let _ = C.del c [ key ] in
      Alcotest.(check int64) "first +1" 1L
        (match C.hincrby c key "n" 1 with
         | Ok n -> n | Error e -> Alcotest.failf "HINCRBY: %a" E.pp e);
      Alcotest.(check int64) "then +10" 11L
        (match C.hincrby c key "n" 10 with
         | Ok n -> n | Error e -> Alcotest.failf "HINCRBY: %a" E.pp e);
      Alcotest.(check int64) "then -5" 6L
        (match C.hincrby c key "n" (-5) with
         | Ok n -> n | Error e -> Alcotest.failf "HINCRBY: %a" E.pp e);
      let _ = C.del c [ key ] in
      ());
  ]
