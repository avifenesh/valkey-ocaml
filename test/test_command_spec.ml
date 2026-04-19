module CS = Valkey.Command_spec
module T = Valkey.Router.Target
module RF = Valkey.Router.Read_from

let args_of_list l = Array.of_list l

let slot_of k = Valkey.Slot.of_key k

let test_get_readonly_by_slot () =
  let args = args_of_list [ "GET"; "user:42" ] in
  match CS.target_and_rf RF.Prefer_replica args with
  | Some (T.By_slot s, RF.Prefer_replica) when s = slot_of "user:42" -> ()
  | Some _ -> Alcotest.fail "GET should route By_slot with rf preserved"
  | None -> Alcotest.fail "GET should be single-reply"

let test_set_write_forces_primary () =
  let args = args_of_list [ "SET"; "user:42"; "ada" ] in
  match CS.target_and_rf RF.Prefer_replica args with
  | Some (T.By_slot s, RF.Primary) when s = slot_of "user:42" -> ()
  | Some _ -> Alcotest.fail "SET must force Primary and route By_slot"
  | None -> Alcotest.fail "SET should be single-reply"

let test_ping_keyless_random () =
  match CS.target_and_rf RF.Primary [| "PING" |] with
  | Some (T.Random, _) -> ()
  | _ -> Alcotest.fail "PING should be Random"

let test_del_multi_key_first_slot () =
  (* Without hashtags the keys may hash to different slots — DEL is
     still routed by the first key; the cluster will error if they
     diverge. *)
  let args = args_of_list [ "DEL"; "{grp}:a"; "{grp}:b" ] in
  match CS.target_and_rf RF.Primary args with
  | Some (T.By_slot s, RF.Primary) when s = slot_of "{grp}:a" -> ()
  | _ -> Alcotest.fail "DEL should pick slot of first key"

let test_flushall_returns_none () =
  match CS.target_and_rf RF.Primary [| "FLUSHALL" |] with
  | None -> ()
  | Some _ -> Alcotest.fail "FLUSHALL is fan-out: target_and_rf = None"

let test_script_load_fan_primaries () =
  match CS.lookup [| "SCRIPT"; "LOAD"; "..." |] with
  | CS.Fan_primaries -> ()
  | _ -> Alcotest.fail "SCRIPT LOAD should be Fan_primaries"

let test_xinfo_stream_key_index_2 () =
  let args = args_of_list [ "XINFO"; "STREAM"; "events" ] in
  match CS.target_and_rf RF.Primary args with
  | Some (T.By_slot s, _) when s = slot_of "events" -> ()
  | _ -> Alcotest.fail "XINFO STREAM: key at index 2"

let test_unknown_command_safe_default () =
  match CS.target_and_rf RF.Prefer_replica [| "ZZZNOPE"; "x" |] with
  | Some (T.Random, RF.Primary) -> ()  (* unknown -> readonly=false -> Primary *)
  | _ -> Alcotest.fail "unknown command should fall back to Random + Primary"

let test_lowercase_command_still_matches () =
  match CS.target_and_rf RF.Primary [| "get"; "k" |] with
  | Some (T.By_slot s, _) when s = slot_of "k" -> ()
  | _ -> Alcotest.fail "lookup should be case-insensitive"

let test_eval_key_index_3 () =
  (* EVAL script numkeys key1 key2 ... argv ... *)
  let args = args_of_list [ "EVAL"; "return 1"; "1"; "mykey" ] in
  match CS.target_and_rf RF.Primary args with
  | Some (T.By_slot s, RF.Primary) when s = slot_of "mykey" -> ()
  | _ -> Alcotest.fail "EVAL: first key at index 3"

let tests =
  [ Alcotest.test_case "GET: readonly, By_slot, rf preserved" `Quick
      test_get_readonly_by_slot;
    Alcotest.test_case "SET: forces Primary" `Quick
      test_set_write_forces_primary;
    Alcotest.test_case "PING: Random" `Quick
      test_ping_keyless_random;
    Alcotest.test_case "DEL: By_slot of first key" `Quick
      test_del_multi_key_first_slot;
    Alcotest.test_case "FLUSHALL: target_and_rf returns None" `Quick
      test_flushall_returns_none;
    Alcotest.test_case "SCRIPT LOAD: Fan_primaries" `Quick
      test_script_load_fan_primaries;
    Alcotest.test_case "XINFO STREAM: key at index 2" `Quick
      test_xinfo_stream_key_index_2;
    Alcotest.test_case "unknown command: Random + Primary" `Quick
      test_unknown_command_safe_default;
    Alcotest.test_case "lowercase command name" `Quick
      test_lowercase_command_still_matches;
    Alcotest.test_case "EVAL: first key at index 3" `Quick
      test_eval_key_index_3;
  ]
