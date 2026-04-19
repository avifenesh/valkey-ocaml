(* Batch 1e tests: FUNCTION + FCALL, CLUSTER introspection,
   LATENCY, MEMORY. *)

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

let contains haystack needle =
  let ls = String.length haystack and ln = String.length needle in
  if ln = 0 then true
  else
    let rec loop i =
      if i + ln > ls then false
      else if String.sub haystack i ln = needle then true
      else loop (i + 1)
    in
    loop 0

(* A minimal Lua library with one function that echoes KEYS[1]. *)
let demo_library_source =
  "#!lua name=demo\n\
   redis.register_function('demo_echo',\n\
   \  function(keys, args) return keys[1] end)\n"

let test_function_lifecycle () =
  with_client @@ fun c ->
  (* Clean slate. *)
  ignore (C.function_flush c ~mode:C.Flush_sync);
  let name =
    check_ok "FUNCTION LOAD"
      (C.function_load c ~source:demo_library_source)
  in
  Alcotest.(check string) "library name" "demo" name;
  (* Reloading without REPLACE must error. *)
  (match C.function_load c ~source:demo_library_source with
   | Error (E.Server_error _) -> ()
   | Error e -> Alcotest.failf "unexpected: %a" err_pp e
   | Ok _ ->
       Alcotest.fail "expected error on re-LOAD without REPLACE");
  check_ok "FUNCTION LOAD REPLACE"
    (C.function_load c ~replace:true ~source:demo_library_source
     |> Result.map ignore);
  check_ok "FUNCTION DELETE"
    (C.function_delete c ~library_name:"demo")

let test_fcall () =
  with_client @@ fun c ->
  ignore (C.function_flush c ~mode:C.Flush_sync);
  let _ = C.function_load c ~source:demo_library_source in
  (match
     C.fcall c ~function_:"demo_echo" ~keys:[ "hello" ] ~args:[]
   with
   | Ok (Valkey.Resp3.Bulk_string "hello") -> ()
   | Ok (Valkey.Resp3.Simple_string "hello") -> ()
   | Ok v ->
       Alcotest.failf "unexpected FCALL reply: %a"
         Valkey.Resp3.pp v
   | Error e -> Alcotest.failf "FCALL: %a" err_pp e);
  ignore (C.function_delete c ~library_name:"demo")

let test_function_list () =
  with_client @@ fun c ->
  ignore (C.function_flush c ~mode:C.Flush_sync);
  let _ = C.function_load c ~source:demo_library_source in
  (match C.function_list c with
   | Ok libs when List.length libs >= 1 -> ()
   | Ok [] -> Alcotest.fail "FUNCTION LIST empty after LOAD"
   | Ok _ -> ()
   | Error e -> Alcotest.failf "FUNCTION LIST: %a" err_pp e);
  ignore (C.function_delete c ~library_name:"demo")

(* CLUSTER KEYSLOT / CLUSTER INFO require cluster mode — those
   live in test_cluster.ml and run against the live docker
   cluster. Standalone Valkey rejects them with
   "This instance has cluster support disabled". *)

let test_latency_reset () =
  with_client @@ fun c ->
  let n = check_ok "LATENCY RESET" (C.latency_reset c) in
  (* Could be 0 on a fresh server with no events; just verify
     it's a non-negative integer. *)
  if n < 0 then Alcotest.failf "negative reset count %d" n

let test_memory_usage () =
  with_client @@ fun c ->
  let k = "mu:present" in
  ignore (C.del c [ k ]);
  ignore (C.set c k (String.make 100 'x'));
  let bytes =
    check_ok "MEMORY USAGE existing" (C.memory_usage c k)
  in
  (match bytes with
   | Some n when n >= 100 -> ()
   | Some n -> Alcotest.failf "%d bytes too small" n
   | None -> Alcotest.fail "None on existing key");
  (* Missing key -> None per docs. *)
  (match C.memory_usage c "mu:missing" with
   | Ok None -> ()
   | Ok (Some _) ->
       Alcotest.fail "expected None for missing key"
   | Error e ->
       Alcotest.failf "MEMORY USAGE missing: %a" err_pp e);
  ignore (C.del c [ k ])

let test_memory_purge () =
  with_client @@ fun c ->
  check_ok "MEMORY PURGE" (C.memory_purge c)

let tests =
  [ Alcotest.test_case "FUNCTION LOAD / re-LOAD / REPLACE / DELETE"
      `Quick test_function_lifecycle;
    Alcotest.test_case "FCALL runs a loaded function" `Quick
      test_fcall;
    Alcotest.test_case "FUNCTION LIST returns at least one library"
      `Quick test_function_list;
    Alcotest.test_case "LATENCY RESET returns non-negative" `Quick
      test_latency_reset;
    Alcotest.test_case "MEMORY USAGE present + missing" `Quick
      test_memory_usage;
    Alcotest.test_case "MEMORY PURGE" `Quick test_memory_purge;
  ]
