module C = Valkey.Cache

let bs s = Valkey.Resp3.Bulk_string s

let test_create_empty () =
  let c = C.create ~byte_budget:1024 in
  Alcotest.(check int) "count starts at 0" 0 (C.count c);
  Alcotest.(check int) "size starts at 0" 0 (C.size c);
  Alcotest.(check (option reject)) "miss on empty" None (C.get c "k")

let test_invalid_argument_on_negative_budget () =
  match C.create ~byte_budget:(-1) with
  | exception Invalid_argument _ -> ()
  | _ -> Alcotest.fail "expected Invalid_argument on negative byte_budget"

let test_put_then_get () =
  let c = C.create ~byte_budget:1024 in
  C.put c "k" (bs "v");
  match C.get c "k" with
  | Some (Valkey.Resp3.Bulk_string "v") -> ()
  | _ -> Alcotest.fail "expected Some (Bulk_string \"v\")"

let test_put_overwrite () =
  let c = C.create ~byte_budget:1024 in
  C.put c "k" (bs "v1");
  C.put c "k" (bs "v2");
  Alcotest.(check int) "still one entry" 1 (C.count c);
  match C.get c "k" with
  | Some (Valkey.Resp3.Bulk_string "v2") -> ()
  | _ -> Alcotest.fail "expected overwritten value"

let test_evict_explicit () =
  let c = C.create ~byte_budget:1024 in
  C.put c "k" (bs "v");
  C.evict c "k";
  Alcotest.(check int) "count drops to 0 after evict" 0 (C.count c);
  Alcotest.(check (option reject)) "miss after evict" None (C.get c "k")

let test_evict_absent_is_noop () =
  let c = C.create ~byte_budget:1024 in
  C.evict c "absent";
  Alcotest.(check int) "still empty" 0 (C.count c)

let test_clear () =
  let c = C.create ~byte_budget:1024 in
  C.put c "a" (bs "1");
  C.put c "b" (bs "2");
  C.put c "c" (bs "3");
  C.clear c;
  Alcotest.(check int) "count zero after clear" 0 (C.count c);
  Alcotest.(check int) "size zero after clear" 0 (C.size c);
  Alcotest.(check (option reject)) "miss for a" None (C.get c "a");
  Alcotest.(check (option reject)) "miss for b" None (C.get c "b");
  Alcotest.(check (option reject)) "miss for c" None (C.get c "c")

let test_size_accounting_increases_on_put () =
  let c = C.create ~byte_budget:1024 in
  let v = bs "hello" in
  let v_size = C.size_of v in
  C.put c "k" v;
  Alcotest.(check int) "size matches size_of value" v_size (C.size c)

let test_size_accounting_decreases_on_evict () =
  let c = C.create ~byte_budget:1024 in
  C.put c "k" (bs "hello");
  C.evict c "k";
  Alcotest.(check int) "size returns to 0" 0 (C.size c)

(* The byte budget is the LRU eviction trigger. Push enough entries
   that the oldest must be evicted to fit a new one. *)
let test_lru_evicts_oldest_when_over_budget () =
  let v = bs (String.make 100 'x') in
  let v_size = C.size_of v in
  (* Budget = 3 entries' worth, then we insert a fourth. *)
  let c = C.create ~byte_budget:(3 * v_size) in
  C.put c "k1" v;
  C.put c "k2" v;
  C.put c "k3" v;
  Alcotest.(check int) "three entries fit" 3 (C.count c);
  C.put c "k4" v;
  Alcotest.(check int) "still three entries (k1 evicted)" 3 (C.count c);
  Alcotest.(check (option reject)) "k1 evicted" None (C.get c "k1");
  Alcotest.(check bool) "k2 still present" true (Option.is_some (C.get c "k2"));
  Alcotest.(check bool) "k3 still present" true (Option.is_some (C.get c "k3"));
  Alcotest.(check bool) "k4 still present" true (Option.is_some (C.get c "k4"))

(* A get on an entry should mark it most-recently-used so a
   subsequent eviction wave skips it. *)
let test_get_updates_recency () =
  let v = bs (String.make 100 'x') in
  let v_size = C.size_of v in
  let c = C.create ~byte_budget:(3 * v_size) in
  C.put c "k1" v;
  C.put c "k2" v;
  C.put c "k3" v;
  (* Touch k1 — it should become MRU; next eviction targets k2. *)
  let _ = C.get c "k1" in
  C.put c "k4" v;
  Alcotest.(check bool) "k1 still present (touched)" true
    (Option.is_some (C.get c "k1"));
  Alcotest.(check (option reject)) "k2 evicted (now LRU)" None (C.get c "k2")

(* A value strictly larger than the budget must not be inserted, and
   any prior entry for that key must remain. *)
let test_oversize_value_rejected () =
  let small = bs "small" in
  let oversize = bs (String.make 10_000 'x') in
  let c = C.create ~byte_budget:1024 in
  C.put c "k" small;
  let size_before = C.size c in
  C.put c "k" oversize;
  Alcotest.(check int) "size unchanged after oversize put"
    size_before (C.size c);
  match C.get c "k" with
  | Some (Valkey.Resp3.Bulk_string "small") -> ()
  | _ -> Alcotest.fail "prior entry should remain"

(* Sanity: size_of a Bulk_string is at least the string's length. *)
let test_size_of_bulk_string () =
  let s = "hello" in
  let v = bs s in
  Alcotest.(check bool) "size_of >= String.length"
    true (C.size_of v >= String.length s)

let test_zero_budget_rejects_everything () =
  let c = C.create ~byte_budget:0 in
  C.put c "k" (bs "v");
  Alcotest.(check int) "zero-budget cache stays empty" 0 (C.count c);
  Alcotest.(check (option reject)) "miss" None (C.get c "k")

(* --- TTL ---------------------------------------------------- *)

let sleep_ms ms = Unix.sleepf (ms /. 1000.0)

let test_ttl_unset_entries_never_expire () =
  let c = C.create ~byte_budget:1024 in
  C.put c "k" (bs "v");
  sleep_ms 30.0;
  Alcotest.(check bool) "still there without ttl" true
    (Option.is_some (C.get c "k"))

let test_ttl_expired_entry_is_miss () =
  let c = C.create ~byte_budget:1024 in
  C.put ~ttl_ms:30 c "k" (bs "v");
  (* Not expired yet. *)
  Alcotest.(check bool) "hit before expiry" true
    (Option.is_some (C.get c "k"));
  sleep_ms 60.0;
  Alcotest.(check (option reject)) "miss after expiry"
    None (C.get c "k");
  (* Expired entry was evicted in place; count reflects it. *)
  Alcotest.(check int) "count 0 after expired get" 0 (C.count c)

let test_ttl_put_refreshes_deadline () =
  let c = C.create ~byte_budget:1024 in
  C.put ~ttl_ms:30 c "k" (bs "v1");
  sleep_ms 20.0;
  (* Refresh with longer TTL before expiry. *)
  C.put ~ttl_ms:200 c "k" (bs "v2");
  sleep_ms 30.0;
  (* Original ttl would have expired by now (~50ms elapsed). *)
  Alcotest.(check bool) "still hit after refresh" true
    (Option.is_some (C.get c "k"));
  match C.get c "k" with
  | Some (Valkey.Resp3.Bulk_string "v2") -> ()
  | _ -> Alcotest.fail "expected v2"

let tests =
  [ Alcotest.test_case "empty cache" `Quick test_create_empty;
    Alcotest.test_case "negative byte_budget rejected" `Quick
      test_invalid_argument_on_negative_budget;
    Alcotest.test_case "put then get" `Quick test_put_then_get;
    Alcotest.test_case "put overwrite" `Quick test_put_overwrite;
    Alcotest.test_case "evict explicit" `Quick test_evict_explicit;
    Alcotest.test_case "evict absent is no-op" `Quick test_evict_absent_is_noop;
    Alcotest.test_case "clear" `Quick test_clear;
    Alcotest.test_case "size accounting on put" `Quick
      test_size_accounting_increases_on_put;
    Alcotest.test_case "size accounting on evict" `Quick
      test_size_accounting_decreases_on_evict;
    Alcotest.test_case "LRU evicts oldest under budget" `Quick
      test_lru_evicts_oldest_when_over_budget;
    Alcotest.test_case "get updates recency" `Quick test_get_updates_recency;
    Alcotest.test_case "oversize value rejected" `Quick
      test_oversize_value_rejected;
    Alcotest.test_case "size_of bulk string" `Quick test_size_of_bulk_string;
    Alcotest.test_case "zero budget rejects everything" `Quick
      test_zero_budget_rejects_everything;
    Alcotest.test_case "ttl unset entries never expire" `Quick
      test_ttl_unset_entries_never_expire;
    Alcotest.test_case "ttl expired entry is miss" `Slow
      test_ttl_expired_entry_is_miss;
    Alcotest.test_case "put refreshes ttl deadline" `Slow
      test_ttl_put_refreshes_deadline;
  ]
