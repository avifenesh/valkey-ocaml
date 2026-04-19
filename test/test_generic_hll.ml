(* HyperLogLog + generic-keyspace integration tests — Batch 1b. *)

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

let test_pfadd_pfcount () =
  with_client @@ fun c ->
  let k = "hll:a" in
  ignore (C.del c [ k ]);
  let changed1 =
    check_ok "PFADD new elements"
      (C.pfadd c k ~elements:[ "alpha"; "beta"; "gamma" ])
  in
  Alcotest.(check bool) "registers altered" true changed1;
  let changed2 =
    check_ok "PFADD existing element"
      (C.pfadd c k ~elements:[ "alpha" ])
  in
  Alcotest.(check bool) "no change on dup" false changed2;
  let card = check_ok "PFCOUNT single" (C.pfcount c [ k ]) in
  Alcotest.(check int) "cardinality" 3 card;
  ignore (C.del c [ k ])

let test_pfmerge_then_count () =
  with_client @@ fun c ->
  let a = "{hll}:a" and b = "{hll}:b" and dst = "{hll}:u" in
  ignore (C.del c [ a; b; dst ]);
  ignore (C.pfadd c a ~elements:[ "x"; "y"; "z" ]);
  ignore (C.pfadd c b ~elements:[ "z"; "w" ]);
  check_ok "PFMERGE"
    (C.pfmerge c ~destination:dst ~sources:[ a; b ]);
  let card =
    check_ok "PFCOUNT merged" (C.pfcount c [ dst ])
  in
  Alcotest.(check int) "union cardinality" 4 card;
  let card_union =
    check_ok "PFCOUNT multi-key" (C.pfcount c [ a; b ])
  in
  Alcotest.(check int) "implicit union count" 4 card_union;
  ignore (C.del c [ a; b; dst ])

let test_touch_count () =
  with_client @@ fun c ->
  let k1 = "tk:1" and k2 = "tk:2" and k3 = "tk:3" in
  ignore (C.del c [ k1; k2; k3 ]);
  ignore (C.set c k1 "a");
  ignore (C.set c k2 "b");
  let n = check_ok "TOUCH" (C.touch c [ k1; k2; k3 ]) in
  (* k3 doesn't exist — should be ignored, count = 2. *)
  Alcotest.(check int) "existing keys touched" 2 n;
  ignore (C.del c [ k1; k2 ])

let test_randomkey () =
  with_client @@ fun c ->
  let k = "rk:probe" in
  ignore (C.set c k "whatever");
  match C.randomkey c with
  | Ok (Some _) -> (* any key is fine *) ignore (C.del c [ k ])
  | Ok None -> Alcotest.fail "DB was empty"
  | Error e -> Alcotest.failf "RANDOMKEY: %a" err_pp e

let test_object_encoding () =
  with_client @@ fun c ->
  let k = "oe:str" in
  ignore (C.del c [ k ]);
  ignore (C.set c k "hello");
  let enc =
    check_ok "OBJECT ENCODING existing"
      (C.object_encoding c k)
  in
  (* Short strings live in embstr or int; either is documented. *)
  (match enc with
   | Some ("embstr" | "raw" | "int") -> ()
   | Some other ->
       Alcotest.failf "unexpected encoding: %s" other
   | None -> Alcotest.fail "None on existing key");
  let missing =
    check_ok "OBJECT ENCODING missing"
      (C.object_encoding c "oe:does-not-exist")
  in
  (match missing with
   | None -> ()
   | Some _ ->
       Alcotest.fail "expected None for missing key");
  ignore (C.del c [ k ])

let test_object_refcount () =
  with_client @@ fun c ->
  let k = "or:k" in
  ignore (C.del c [ k ]);
  ignore (C.set c k "v");
  let rc =
    check_ok "OBJECT REFCOUNT" (C.object_refcount c k)
  in
  (match rc with
   | Some n when n >= 1 -> ()
   | Some n -> Alcotest.failf "refcount %d < 1" n
   | None -> Alcotest.fail "None on existing key");
  (match C.object_refcount c "or:none" with
   | Ok None -> ()
   | Ok (Some _) ->
       Alcotest.fail "expected None for missing key"
   | Error e ->
       Alcotest.failf "REFCOUNT missing: %a" err_pp e);
  ignore (C.del c [ k ])

let test_dump_restore_roundtrip () =
  with_client @@ fun c ->
  let src = "dr:src" and dst = "dr:dst" in
  ignore (C.del c [ src; dst ]);
  ignore (C.set c src "payload-42");
  let serialized =
    match check_ok "DUMP" (C.dump c src) with
    | Some s -> s
    | None -> Alcotest.fail "DUMP returned None"
  in
  (* Restore into dst with no TTL, no replace. *)
  check_ok "RESTORE"
    (C.restore c dst ~ttl_ms:0 ~serialized);
  (match C.get c dst with
   | Ok (Some v) ->
       Alcotest.(check string) "value survived roundtrip"
         "payload-42" v
   | _ -> Alcotest.fail "GET after RESTORE failed");
  (* Restoring into an existing key without REPLACE must error. *)
  (match C.restore c dst ~ttl_ms:0 ~serialized with
   | Error (E.Server_error { code; _ }) ->
       (* Valkey uses "BUSYKEY" for this. *)
       Alcotest.(check string) "busykey code" "BUSYKEY" code
   | Error e -> Alcotest.failf "unexpected: %a" err_pp e
   | Ok () -> Alcotest.fail "expected BUSYKEY error");
  (* With REPLACE it succeeds. *)
  check_ok "RESTORE REPLACE"
    (C.restore c dst ~ttl_ms:0 ~serialized ~replace:true);
  ignore (C.del c [ src; dst ])

let test_copy () =
  with_client @@ fun c ->
  let src = "{cp}:src" and dst = "{cp}:dst" in
  ignore (C.del c [ src; dst ]);
  ignore (C.set c src "v1");
  let copied1 =
    check_ok "COPY fresh" (C.copy c ~source:src ~destination:dst)
  in
  Alcotest.(check bool) "fresh copy succeeds" true copied1;
  let copied2 =
    check_ok "COPY without REPLACE"
      (C.copy c ~source:src ~destination:dst)
  in
  Alcotest.(check bool) "existing dst without REPLACE fails"
    false copied2;
  let copied3 =
    check_ok "COPY with REPLACE"
      (C.copy c ~source:src ~destination:dst ~replace:true)
  in
  Alcotest.(check bool) "REPLACE overwrites" true copied3;
  ignore (C.del c [ src; dst ])

let tests =
  [ Alcotest.test_case "PFADD / PFCOUNT" `Quick test_pfadd_pfcount;
    Alcotest.test_case "PFMERGE + union cardinality" `Quick
      test_pfmerge_then_count;
    Alcotest.test_case "TOUCH counts only existing keys" `Quick
      test_touch_count;
    Alcotest.test_case "RANDOMKEY" `Quick test_randomkey;
    Alcotest.test_case "OBJECT ENCODING" `Quick test_object_encoding;
    Alcotest.test_case "OBJECT REFCOUNT + missing" `Quick
      test_object_refcount;
    Alcotest.test_case "DUMP / RESTORE round-trip + BUSYKEY" `Quick
      test_dump_restore_roundtrip;
    Alcotest.test_case "COPY + REPLACE" `Quick test_copy;
  ]
