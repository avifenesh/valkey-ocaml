module I = Valkey.Invalidation
module R = Valkey.Resp3

(* --- well-formed pushes ------------------------------------------ *)

let test_keys () =
  let push =
    R.Push [ R.Bulk_string "invalidate";
             R.Array [ R.Bulk_string "k1"; R.Bulk_string "k2" ] ]
  in
  match I.of_push push with
  | Some (I.Keys [ "k1"; "k2" ]) -> ()
  | _ -> Alcotest.fail "expected Some (Keys [k1;k2])"

let test_flush_all () =
  let push =
    R.Push [ R.Bulk_string "invalidate"; R.Null ]
  in
  match I.of_push push with
  | Some I.Flush_all -> ()
  | _ -> Alcotest.fail "expected Some Flush_all"

let test_empty_keys_ok () =
  (* Vacuous invalidation — not forbidden by spec, treat as no-op. *)
  let push =
    R.Push [ R.Bulk_string "invalidate"; R.Array [] ]
  in
  match I.of_push push with
  | Some (I.Keys []) -> ()
  | _ -> Alcotest.fail "expected Some (Keys [])"

(* --- not-our-frame — must return None, no exception ------------- *)

let test_pubsub_push_ignored () =
  let push =
    R.Push [ R.Bulk_string "message";
             R.Bulk_string "channel"; R.Bulk_string "payload" ]
  in
  match I.of_push push with
  | None -> ()
  | Some _ -> Alcotest.fail "pubsub push must not be recognised"

let test_non_push_ignored () =
  let arr = R.Array [ R.Bulk_string "invalidate"; R.Null ] in
  match I.of_push arr with
  | None -> ()
  | Some _ -> Alcotest.fail "plain Array must not parse as invalidation"

let test_simple_string_ignored () =
  match I.of_push (R.Simple_string "OK") with
  | None -> ()
  | Some _ -> Alcotest.fail "Simple_string must not parse"

(* --- malformed invalidation pushes — must return None ---------- *)

let test_one_element_push () =
  let push = R.Push [ R.Bulk_string "invalidate" ] in
  match I.of_push push with
  | None -> ()
  | Some _ -> Alcotest.fail "1-element push must be rejected"

let test_three_element_push () =
  let push =
    R.Push [ R.Bulk_string "invalidate";
             R.Array []; R.Bulk_string "extra" ]
  in
  match I.of_push push with
  | None -> ()
  | Some _ -> Alcotest.fail "3-element push must be rejected"

let test_body_wrong_type () =
  let push =
    R.Push [ R.Bulk_string "invalidate"; R.Bulk_string "not-an-array" ]
  in
  match I.of_push push with
  | None -> ()
  | Some _ -> Alcotest.fail "string body must be rejected"

let test_non_string_keys () =
  let push =
    R.Push [ R.Bulk_string "invalidate";
             R.Array [ R.Bulk_string "ok-key"; R.Integer 42L ] ]
  in
  match I.of_push push with
  | None -> ()
  | Some _ ->
      Alcotest.fail "invalidation with non-string key must be rejected whole"

let test_empty_push () =
  match I.of_push (R.Push []) with
  | None -> ()
  | Some _ -> Alcotest.fail "empty push must be rejected"

let test_verb_wrong () =
  let push =
    R.Push [ R.Bulk_string "INVALIDATE"; R.Null ]   (* case-sensitive *)
  in
  match I.of_push push with
  | None -> ()
  | Some _ -> Alcotest.fail "verb is case-sensitive; uppercase must not match"

(* --- apply: the effectful half of the fiber body --------------- *)

module C = Valkey.Cache
module IF = Valkey.Inflight

let bs s = R.Bulk_string s

let fresh_infl () : (R.t, Valkey.Connection_error.t) result IF.t =
  IF.create ()

let test_apply_keys_evicts () =
  let c = C.create ~byte_budget:1024 in
  let inf = fresh_infl () in
  C.put c "k1" (bs "v1");
  C.put c "k2" (bs "v2");
  C.put c "k3" (bs "v3");
  I.apply c inf (I.Keys [ "k1"; "k3" ]);
  Alcotest.(check int) "one entry left" 1 (C.count c);
  Alcotest.(check (option reject)) "k1 gone" None (C.get c "k1");
  Alcotest.(check bool) "k2 present" true (Option.is_some (C.get c "k2"));
  Alcotest.(check (option reject)) "k3 gone" None (C.get c "k3")

let test_apply_flush_all_clears () =
  let c = C.create ~byte_budget:1024 in
  let inf = fresh_infl () in
  C.put c "k1" (bs "v1");
  C.put c "k2" (bs "v2");
  I.apply c inf I.Flush_all;
  Alcotest.(check int) "empty after Flush_all" 0 (C.count c)

let test_apply_keys_absent_is_noop () =
  let c = C.create ~byte_budget:1024 in
  let inf = fresh_infl () in
  C.put c "real" (bs "v");
  I.apply c inf (I.Keys [ "ghost1"; "ghost2" ]);
  Alcotest.(check int) "unrelated entry intact" 1 (C.count c);
  Alcotest.(check bool) "real still there" true (Option.is_some (C.get c "real"))

let test_apply_keys_empty () =
  let c = C.create ~byte_budget:1024 in
  let inf = fresh_infl () in
  C.put c "k" (bs "v");
  I.apply c inf (I.Keys []);
  Alcotest.(check int) "empty-keys invalidation touches nothing" 1 (C.count c)

(* New: verify apply flips Inflight dirty for each key. *)
let test_apply_keys_marks_inflight_dirty () =
  let c = C.create ~byte_budget:1024 in
  let inf = fresh_infl () in
  let _ = IF.begin_fetch inf "hot" in
  let _ = IF.begin_fetch inf "cold" in
  I.apply c inf (I.Keys [ "hot" ]);
  (match IF.complete inf "hot" with
   | IF.Dirty -> ()
   | IF.Clean -> Alcotest.fail "hot should be dirty after apply");
  (match IF.complete inf "cold" with
   | IF.Clean -> ()
   | IF.Dirty -> Alcotest.fail "cold should still be clean")

(* Flush_all closes the in-flight-race gap via mark_all_dirty. *)
let test_apply_flush_all_marks_every_inflight_dirty () =
  let c = C.create ~byte_budget:1024 in
  let inf = fresh_infl () in
  let _ = IF.begin_fetch inf "a" in
  let _ = IF.begin_fetch inf "b" in
  I.apply c inf I.Flush_all;
  (match IF.complete inf "a" with
   | IF.Dirty -> ()
   | IF.Clean -> Alcotest.fail "a should be dirty after Flush_all");
  (match IF.complete inf "b" with
   | IF.Dirty -> ()
   | IF.Clean -> Alcotest.fail "b should be dirty after Flush_all")

let tests =
  [ Alcotest.test_case "keys list" `Quick test_keys;
    Alcotest.test_case "null body -> flush all" `Quick test_flush_all;
    Alcotest.test_case "empty keys array -> Keys []" `Quick test_empty_keys_ok;
    Alcotest.test_case "pubsub push ignored" `Quick test_pubsub_push_ignored;
    Alcotest.test_case "non-push ignored" `Quick test_non_push_ignored;
    Alcotest.test_case "simple string ignored" `Quick test_simple_string_ignored;
    Alcotest.test_case "1-element push rejected" `Quick test_one_element_push;
    Alcotest.test_case "3-element push rejected" `Quick test_three_element_push;
    Alcotest.test_case "wrong body type rejected" `Quick test_body_wrong_type;
    Alcotest.test_case "non-string keys rejected whole" `Quick test_non_string_keys;
    Alcotest.test_case "empty push rejected" `Quick test_empty_push;
    Alcotest.test_case "verb case-sensitive" `Quick test_verb_wrong;
    Alcotest.test_case "apply Keys evicts listed" `Quick test_apply_keys_evicts;
    Alcotest.test_case "apply Flush_all clears cache" `Quick
      test_apply_flush_all_clears;
    Alcotest.test_case "apply Keys with absent keys is noop" `Quick
      test_apply_keys_absent_is_noop;
    Alcotest.test_case "apply Keys [] is noop" `Quick test_apply_keys_empty;
    Alcotest.test_case "apply Keys marks inflight dirty" `Quick
      test_apply_keys_marks_inflight_dirty;
    Alcotest.test_case "apply Flush_all marks every inflight dirty" `Quick
      test_apply_flush_all_marks_every_inflight_dirty;
  ]
