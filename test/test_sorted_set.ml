(* Typed sorted-set wrapper tests.

   Covers the wrappers added in this batch:
     ZADD (default + CH + NX + XX + INCR variant)
     ZINCRBY
     ZREM
     ZRANK / ZREVRANK (+ WITHSCORE variants)
     ZSCORE / ZMSCORE
     ZCOUNT
     ZRANGE WITHSCORES (and REV)
     ZRANGEBYSCORE WITHSCORES (with LIMIT)
     ZPOPMIN / ZPOPMAX (single + count)

   Requires docker compose Valkey 9 on localhost:6379. *)

module C = Valkey.Client
module E = Valkey.Connection.Error

let standalone_reachable () =
  try
    Eio_main.run @@ fun env ->
    Eio.Switch.run @@ fun sw ->
    let c =
      C.connect ~sw
        ~net:(Eio.Stdenv.net env) ~clock:(Eio.Stdenv.clock env)
        ~host:"localhost" ~port:6379 ()
    in
    C.close c;
    true
  with _ -> false

let with_client f =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let c =
    C.connect ~sw
      ~net:(Eio.Stdenv.net env) ~clock:(Eio.Stdenv.clock env)
      ~host:"localhost" ~port:6379 ()
  in
  Fun.protect ~finally:(fun () -> C.close c) @@ fun () -> f c

let key = "ocaml:zset:typed"

let approx a b =
  Float.abs (a -. b) < 1e-9

let unwrap_score label = function
  | Some s -> s
  | None -> Alcotest.failf "%s: expected Some, got None" label

let test_zadd_basic () =
  with_client @@ fun c ->
  let _ = C.del c [ key ] in
  (match
     C.zadd c key
       [ 1.0, "a"; 2.0, "b"; 3.0, "c"; 10.0, "d" ]
   with
   | Ok 4 -> ()
   | Ok n -> Alcotest.failf "expected 4 added, got %d" n
   | Error e -> Alcotest.failf "ZADD: %a" E.pp e);
  Alcotest.(check int) "ZCARD" 4
    (match C.zcard c key with Ok n -> n
     | Error e -> Alcotest.failf "ZCARD: %a" E.pp e)

let test_zadd_modes () =
  with_client @@ fun c ->
  let _ = C.del c [ key ] in
  (* XX (only-update) on a missing key adds nothing. *)
  (match C.zadd c key ~mode:C.Z_only_update [ 1.0, "ghost" ] with
   | Ok 0 -> ()
   | Ok n -> Alcotest.failf "ZADD XX: expected 0, got %d" n
   | Error e -> Alcotest.failf "ZADD XX: %a" E.pp e);
  (* Seed, then NX (only-add) doesn't update an existing member. *)
  let _ = C.zadd c key [ 5.0, "alice" ] in
  (match C.zadd c key ~mode:C.Z_only_add [ 9.0, "alice" ] with
   | Ok 0 -> ()
   | Ok n -> Alcotest.failf "ZADD NX existing: expected 0, got %d" n
   | Error e -> Alcotest.failf "ZADD NX: %a" E.pp e);
  Alcotest.(check (float 1e-6)) "score unchanged" 5.0
    (unwrap_score "ZSCORE alice"
       (match C.zscore c key "alice" with
        | Ok x -> x | Error e -> Alcotest.failf "ZSCORE: %a" E.pp e));
  (* GT alone: adds new members AND updates if greater. Updates
     "alice" 5 -> 10 (greater), adds new "bob". *)
  (match
     C.zadd c key ~mode:C.Z_add_or_update_if_greater
       [ 10.0, "alice"; 3.0, "bob" ]
   with
   | Ok 1 -> () (* one new member added *)
   | Ok n -> Alcotest.failf "ZADD GT: expected 1, got %d" n
   | Error e -> Alcotest.failf "ZADD GT: %a" E.pp e);
  Alcotest.(check (float 1e-6)) "alice updated to 10" 10.0
    (unwrap_score "ZSCORE alice"
       (match C.zscore c key "alice" with
        | Ok x -> x | Error e -> Alcotest.failf "%a" E.pp e));
  (* GT again with a smaller score: doesn't update. *)
  let _ =
    C.zadd c key ~mode:C.Z_add_or_update_if_greater [ 1.0, "alice" ]
  in
  Alcotest.(check (float 1e-6)) "alice still 10 (GT blocked)" 10.0
    (unwrap_score "ZSCORE alice"
       (match C.zscore c key "alice" with
        | Ok x -> x | Error e -> Alcotest.failf "%a" E.pp e));
  (* XX_GT: doesn't add new, conditional on greater. *)
  (match
     C.zadd c key ~mode:C.Z_only_update_if_greater [ 99.0, "carol" ]
   with
   | Ok 0 -> () (* carol doesn't exist; XX blocks add *)
   | Ok n -> Alcotest.failf "ZADD XX GT new: expected 0, got %d" n
   | Error e -> Alcotest.failf "%a" E.pp e)

let test_zadd_ch () =
  with_client @@ fun c ->
  let _ = C.del c [ key ] in
  let _ = C.zadd c key [ 1.0, "a"; 2.0, "b" ] in
  (* Default: returns count of NEW members. Updating "a" adds 0,
     adding "c" adds 1. *)
  (match C.zadd c key [ 5.0, "a"; 3.0, "c" ] with
   | Ok 1 -> () | Ok n -> Alcotest.failf "default: expected 1, got %d" n
   | Error e -> Alcotest.failf "ZADD: %a" E.pp e);
  (* With CH, score changes also count. Updating a (5 -> 6),
     adding d. *)
  (match C.zadd c key ~ch:true [ 6.0, "a"; 7.0, "d" ] with
   | Ok 2 -> () | Ok n -> Alcotest.failf "CH: expected 2, got %d" n
   | Error e -> Alcotest.failf "ZADD CH: %a" E.pp e)

let test_zadd_incr () =
  with_client @@ fun c ->
  let _ = C.del c [ key ] in
  (* Plain INCR creates the member at the increment value. *)
  (match C.zadd_incr c key ~score:5.0 ~member:"x" with
   | Ok (Some f) when approx f 5.0 -> ()
   | Ok r -> Alcotest.failf "ZADD INCR initial: %s"
                (match r with Some f -> string_of_float f | None -> "None")
   | Error e -> Alcotest.failf "%a" E.pp e);
  (* Increment again, should be 8.0. *)
  (match C.zadd_incr c key ~score:3.0 ~member:"x" with
   | Ok (Some f) when approx f 8.0 -> ()
   | _ -> Alcotest.fail "ZADD INCR cumulative");
  (* NX on existing -> None. *)
  (match
     C.zadd_incr c key ~mode:C.Z_only_add ~score:1.0 ~member:"x"
   with
   | Ok None -> ()
   | _ -> Alcotest.fail "ZADD NX INCR existing: expected None")

let test_zincrby_zscore () =
  with_client @@ fun c ->
  let _ = C.del c [ key ] in
  let _ = C.zadd c key [ 10.0, "k" ] in
  (match C.zincrby c key ~by:5.5 ~member:"k" with
   | Ok f when approx f 15.5 -> ()
   | Ok f -> Alcotest.failf "ZINCRBY: %g" f
   | Error e -> Alcotest.failf "%a" E.pp e);
  (match C.zscore c key "k" with
   | Ok (Some f) when approx f 15.5 -> ()
   | _ -> Alcotest.fail "ZSCORE k expected 15.5");
  (match C.zscore c key "ghost" with
   | Ok None -> ()
   | _ -> Alcotest.fail "ZSCORE ghost expected None")

let test_zmscore () =
  with_client @@ fun c ->
  let _ = C.del c [ key ] in
  let _ = C.zadd c key [ 1.0, "a"; 3.0, "c" ] in
  (match C.zmscore c key [ "a"; "b"; "c" ] with
   | Ok [ Some 1.0; None; Some 3.0 ] -> ()
   | Ok xs ->
       Alcotest.failf "ZMSCORE: got %d items"
         (List.length xs)
   | Error e -> Alcotest.failf "%a" E.pp e)

let test_zrem_zcount () =
  with_client @@ fun c ->
  let _ = C.del c [ key ] in
  let _ =
    C.zadd c key [ 1.0, "a"; 2.0, "b"; 3.0, "c"; 4.0, "d" ]
  in
  (match C.zcount c key ~min:(C.Score 2.0) ~max:(C.Score 3.0) with
   | Ok 2 -> () | _ -> Alcotest.fail "ZCOUNT");
  (match C.zrem c key [ "b"; "d"; "ghost" ] with
   | Ok 2 -> ()
   | Ok n -> Alcotest.failf "ZREM: expected 2, got %d" n
   | Error e -> Alcotest.failf "%a" E.pp e)

let test_zrank () =
  with_client @@ fun c ->
  let _ = C.del c [ key ] in
  let _ = C.zadd c key [ 5.0, "low"; 50.0, "mid"; 500.0, "high" ] in
  Alcotest.(check (option int)) "ZRANK low" (Some 0)
    (match C.zrank c key "low" with Ok x -> x
     | Error e -> Alcotest.failf "%a" E.pp e);
  Alcotest.(check (option int)) "ZREVRANK low" (Some 2)
    (match C.zrevrank c key "low" with Ok x -> x
     | Error e -> Alcotest.failf "%a" E.pp e);
  Alcotest.(check (option int)) "ZRANK ghost" None
    (match C.zrank c key "ghost" with Ok x -> x
     | Error _ -> Alcotest.fail "ZRANK ghost");
  (* WITHSCORE variant — Valkey 7.2+. *)
  (match C.zrank_with_score c key "mid" with
   | Ok (Some (1, s)) when approx s 50.0 -> ()
   | _ -> Alcotest.fail "ZRANK WITHSCORE mid")

let test_zrange_with_scores () =
  with_client @@ fun c ->
  let _ = C.del c [ key ] in
  let _ = C.zadd c key [ 1.0, "a"; 2.0, "b"; 3.0, "c" ] in
  (match
     C.zrange_with_scores c key ~start:0 ~stop:(-1)
   with
   | Ok pairs ->
       Alcotest.(check int) "len" 3 (List.length pairs);
       (match pairs with
        | [ ("a", _); ("b", _); ("c", _) ] -> ()
        | _ -> Alcotest.fail "members order asc")
   | Error e -> Alcotest.failf "%a" E.pp e);
  (match
     C.zrange_with_scores c key ~rev:true ~start:0 ~stop:(-1)
   with
   | Ok [ ("c", _); ("b", _); ("a", _) ] -> ()
   | _ -> Alcotest.fail "ZRANGE REV WITHSCORES order");
  (match
     C.zrangebyscore_with_scores c key
       ~min:(C.Score 2.0) ~max:(C.Score 3.0)
       ~limit:(0, 5)
   with
   | Ok [ ("b", s1); ("c", s2) ] when approx s1 2.0 && approx s2 3.0 -> ()
   | _ -> Alcotest.fail "ZRANGEBYSCORE WITHSCORES")

let test_zpop () =
  with_client @@ fun c ->
  let _ = C.del c [ key ] in
  let _ = C.zadd c key [ 1.0, "a"; 2.0, "b"; 3.0, "c" ] in
  (match C.zpopmin c key with
   | Ok [ ("a", s) ] when approx s 1.0 -> ()
   | Ok xs ->
       Alcotest.failf "ZPOPMIN single: got %d items" (List.length xs)
   | Error e -> Alcotest.failf "%a" E.pp e);
  (match C.zpopmax c key ~count:5 with
   | Ok [ ("c", _); ("b", _) ] -> ()
   | Ok xs ->
       Alcotest.failf "ZPOPMAX count: got %d items" (List.length xs)
   | Error e -> Alcotest.failf "%a" E.pp e);
  (* Now empty. *)
  (match C.zpopmin c key with
   | Ok [] -> ()
   | _ -> Alcotest.fail "ZPOPMIN on empty key")

let skip_placeholder name () =
  Printf.printf
    "[skipped] %s (need docker valkey :6379)\n%!" name

let tests =
  let reachable = standalone_reachable () in
  let tc name f =
    if reachable then Alcotest.test_case name `Quick f
    else Alcotest.test_case name `Quick (skip_placeholder name)
  in
  [ tc "ZADD basic" test_zadd_basic;
    tc "ZADD modes (NX/XX/GT/LT/XX_GT)" test_zadd_modes;
    tc "ZADD CH" test_zadd_ch;
    tc "ZADD INCR" test_zadd_incr;
    tc "ZINCRBY + ZSCORE" test_zincrby_zscore;
    tc "ZMSCORE" test_zmscore;
    tc "ZREM + ZCOUNT" test_zrem_zcount;
    tc "ZRANK / ZREVRANK / WITHSCORE" test_zrank;
    tc "ZRANGE / ZRANGEBYSCORE WITHSCORES" test_zrange_with_scores;
    tc "ZPOPMIN / ZPOPMAX" test_zpop;
  ]
