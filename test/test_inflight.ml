(* Pure-unit tests for the in-flight coordination module. No Eio
   runtime is required — we only ever create promises and inspect
   the decision the module returns. *)

module I = Valkey.Inflight

let fresh_resolver : _ I.begin_result -> _ Eio.Promise.u = function
  | I.Fresh u -> u
  | I.Joining _ -> Alcotest.fail "expected Fresh, got Joining"

let joining_promise : _ I.begin_result -> _ Eio.Promise.t = function
  | I.Joining p -> p
  | I.Fresh _ -> Alcotest.fail "expected Joining, got Fresh"

(* --- single-flight ---------------------------------------------- *)

let test_first_is_fresh () =
  let t = I.create () in
  let _ = fresh_resolver (I.begin_fetch t "k") in
  Alcotest.(check int) "one in flight" 1 (I.pending_count t)

let test_second_joins () =
  let t = I.create () in
  let _owner = fresh_resolver (I.begin_fetch t "k") in
  let _joiner = joining_promise (I.begin_fetch t "k") in
  Alcotest.(check int) "still just one entry" 1 (I.pending_count t)

let test_many_joiners_share_promise () =
  let t = I.create () in
  let owner = fresh_resolver (I.begin_fetch t "k") in
  let joiners =
    List.init 5 (fun _ -> joining_promise (I.begin_fetch t "k"))
  in
  (* Resolve the owner, confirm every joiner's promise is resolved
     with the same value. *)
  Eio.Promise.resolve owner "hello";
  List.iter
    (fun p ->
      match Eio.Promise.peek p with
      | Some "hello" -> ()
      | Some v ->
          Alcotest.failf "joiner got %S, expected hello" v
      | None -> Alcotest.fail "joiner still unresolved")
    joiners

let test_entry_cleared_after_complete () =
  let t = I.create () in
  let _ = fresh_resolver (I.begin_fetch t "k") in
  let c = I.complete t "k" in
  Alcotest.(check int) "empty after complete" 0 (I.pending_count t);
  (match c with I.Clean -> () | I.Dirty -> Alcotest.fail "expected Clean")

let test_next_fetch_after_complete_is_fresh () =
  let t = I.create () in
  let _ = fresh_resolver (I.begin_fetch t "k") in
  let _ = I.complete t "k" in
  let _ = fresh_resolver (I.begin_fetch t "k") in
  Alcotest.(check int) "new fetch started" 1 (I.pending_count t)

(* --- invalidation race ----------------------------------------- *)

let test_mark_dirty_during_fetch () =
  let t = I.create () in
  let _ = fresh_resolver (I.begin_fetch t "k") in
  I.mark_dirty t "k";
  match I.complete t "k" with
  | I.Dirty -> ()
  | I.Clean ->
      Alcotest.fail "mark_dirty should flip completion to Dirty"

let test_mark_dirty_without_fetch_is_noop () =
  let t = I.create () in
  I.mark_dirty t "absent";
  Alcotest.(check int) "still empty" 0 (I.pending_count t)

let test_mark_dirty_affects_only_that_key () =
  let t = I.create () in
  let _ = fresh_resolver (I.begin_fetch t "a") in
  let _ = fresh_resolver (I.begin_fetch t "b") in
  I.mark_dirty t "a";
  (match I.complete t "a" with
   | I.Dirty -> ()
   | I.Clean -> Alcotest.fail "a should be dirty");
  (match I.complete t "b" with
   | I.Clean -> ()
   | I.Dirty -> Alcotest.fail "b should still be clean")

let test_complete_absent_is_clean () =
  let t = I.create () in
  match I.complete t "ghost" with
  | I.Clean -> ()
  | I.Dirty -> Alcotest.fail "absent entry should complete Clean"

let test_abandon_clears_entry () =
  let t = I.create () in
  let _ = fresh_resolver (I.begin_fetch t "k") in
  I.abandon t "k";
  Alcotest.(check int) "empty after abandon" 0 (I.pending_count t)

(* --- mark_all_dirty (Flush_all hook) --------------------------- *)

let test_mark_all_dirty_flips_every_pending () =
  let t = I.create () in
  let _ = fresh_resolver (I.begin_fetch t "a") in
  let _ = fresh_resolver (I.begin_fetch t "b") in
  let _ = fresh_resolver (I.begin_fetch t "c") in
  I.mark_all_dirty t;
  (match I.complete t "a" with
   | I.Dirty -> () | I.Clean -> Alcotest.fail "a should be dirty");
  (match I.complete t "b" with
   | I.Dirty -> () | I.Clean -> Alcotest.fail "b should be dirty");
  (match I.complete t "c" with
   | I.Dirty -> () | I.Clean -> Alcotest.fail "c should be dirty")

let test_mark_all_dirty_on_empty_is_noop () =
  let t = I.create () in
  I.mark_all_dirty t;
  Alcotest.(check int) "still empty" 0 (I.pending_count t)

let tests =
  [ Alcotest.test_case "first fetch is fresh" `Quick test_first_is_fresh;
    Alcotest.test_case "second concurrent fetch joins" `Quick
      test_second_joins;
    Alcotest.test_case "many joiners share one promise" `Quick
      test_many_joiners_share_promise;
    Alcotest.test_case "complete removes entry" `Quick
      test_entry_cleared_after_complete;
    Alcotest.test_case "fetch after complete is fresh again" `Quick
      test_next_fetch_after_complete_is_fresh;
    Alcotest.test_case "mark_dirty during fetch flips completion"
      `Quick test_mark_dirty_during_fetch;
    Alcotest.test_case "mark_dirty without pending is noop" `Quick
      test_mark_dirty_without_fetch_is_noop;
    Alcotest.test_case "mark_dirty is scoped to its key" `Quick
      test_mark_dirty_affects_only_that_key;
    Alcotest.test_case "complete on absent key returns Clean" `Quick
      test_complete_absent_is_clean;
    Alcotest.test_case "abandon clears entry" `Quick
      test_abandon_clears_entry;
    Alcotest.test_case "mark_all_dirty flips every pending" `Quick
      test_mark_all_dirty_flips_every_pending;
    Alcotest.test_case "mark_all_dirty on empty is noop" `Quick
      test_mark_all_dirty_on_empty_is_noop;
  ]
