(* Pub/sub integration tests against standalone Valkey at :6379. *)

module C = Valkey.Client
module PS = Valkey.Pubsub
module E = Valkey.Connection.Error

let host = "localhost"
let port = 6379
let err_pp = E.pp

let test_channel_roundtrip () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let sub = PS.connect ~sw ~net ~clock ~host ~port () in
  let pub = C.connect ~sw ~net ~clock ~host ~port () in
  Fun.protect
    ~finally:(fun () -> PS.close sub; C.close pub)
  @@ fun () ->
  (match PS.subscribe sub [ "ps:chan:a" ] with
   | Ok () -> ()
   | Error e -> Alcotest.failf "subscribe: %a" err_pp e);
  (* The server needs a moment to register the subscription before
     we publish, otherwise PUBLISH returns 0. Wait for the
     subscription ACK to flow back; easier: brief sleep. *)
  Eio.Time.sleep clock 0.05;
  (match C.publish pub ~channel:"ps:chan:a" ~message:"hello" with
   | Ok n when n >= 1 -> ()
   | Ok n -> Alcotest.failf "publish returned %d, expected >= 1" n
   | Error e -> Alcotest.failf "publish: %a" err_pp e);
  match PS.next_message ~timeout:2.0 sub with
  | Error `Timeout ->
      Alcotest.fail "no message arrived within 2s"
  | Ok (PS.Channel { channel; payload }) ->
      Alcotest.(check string) "channel" "ps:chan:a" channel;
      Alcotest.(check string) "payload" "hello" payload
  | Ok _ -> Alcotest.fail "expected Channel delivery"

let test_pattern_roundtrip () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let sub = PS.connect ~sw ~net ~clock ~host ~port () in
  let pub = C.connect ~sw ~net ~clock ~host ~port () in
  Fun.protect
    ~finally:(fun () -> PS.close sub; C.close pub)
  @@ fun () ->
  (match PS.psubscribe sub [ "ps:p:*" ] with
   | Ok () -> ()
   | Error e -> Alcotest.failf "psubscribe: %a" err_pp e);
  Eio.Time.sleep clock 0.05;
  (match C.publish pub ~channel:"ps:p:one" ~message:"payload-one" with
   | Ok _ -> ()
   | Error e -> Alcotest.failf "publish: %a" err_pp e);
  match PS.next_message ~timeout:2.0 sub with
  | Error `Timeout ->
      Alcotest.fail "no pattern message arrived within 2s"
  | Ok (PS.Pattern { pattern; channel; payload }) ->
      Alcotest.(check string) "pattern" "ps:p:*" pattern;
      Alcotest.(check string) "channel" "ps:p:one" channel;
      Alcotest.(check string) "payload" "payload-one" payload
  | Ok _ -> Alcotest.fail "expected Pattern delivery"

let test_multi_subscribe_on_same_handle () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let sub = PS.connect ~sw ~net ~clock ~host ~port () in
  let pub = C.connect ~sw ~net ~clock ~host ~port () in
  Fun.protect
    ~finally:(fun () -> PS.close sub; C.close pub)
  @@ fun () ->
  (match PS.subscribe sub [ "ps:multi:a"; "ps:multi:b" ] with
   | Ok () -> ()
   | Error e -> Alcotest.failf "subscribe: %a" err_pp e);
  Eio.Time.sleep clock 0.05;
  ignore (C.publish pub ~channel:"ps:multi:a" ~message:"m-a");
  ignore (C.publish pub ~channel:"ps:multi:b" ~message:"m-b");
  (* Messages can arrive in either order — collect two and check set. *)
  let got = ref [] in
  for _ = 1 to 2 do
    match PS.next_message ~timeout:2.0 sub with
    | Ok (PS.Channel { channel; payload }) ->
        got := (channel, payload) :: !got
    | Ok _ -> Alcotest.fail "expected Channel delivery"
    | Error `Timeout ->
        Alcotest.failf "timed out; got %d messages" (List.length !got)
  done;
  let expected =
    List.sort compare
      [ "ps:multi:a", "m-a"; "ps:multi:b", "m-b" ]
  in
  let actual = List.sort compare !got in
  Alcotest.(check (list (pair string string))) "delivered set"
    expected actual

let test_unsubscribe_then_publish () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let sub = PS.connect ~sw ~net ~clock ~host ~port () in
  let pub = C.connect ~sw ~net ~clock ~host ~port () in
  Fun.protect
    ~finally:(fun () -> PS.close sub; C.close pub)
  @@ fun () ->
  let _ = PS.subscribe sub [ "ps:unsub" ] in
  Eio.Time.sleep clock 0.05;
  let _ = PS.unsubscribe sub [ "ps:unsub" ] in
  Eio.Time.sleep clock 0.05;
  let _ = C.publish pub ~channel:"ps:unsub" ~message:"should-not-arrive" in
  match PS.next_message ~timeout:0.25 sub with
  | Error `Timeout -> ()
  | Ok (PS.Channel { payload; _ }) ->
      Alcotest.failf "unexpected message after unsubscribe: %s" payload
  | Ok _ -> Alcotest.fail "unexpected non-channel delivery"

let test_auto_resubscribe_after_kill () =
  (* Open a Pubsub, subscribe, confirm delivery, then from a
     separate admin Client force-kill all pub/sub connections via
     CLIENT KILL TYPE pubsub. Connection.recovery_loop reconnects
     and fires on_connected; Pubsub replays the subscription set.
     A subsequent PUBLISH must be delivered. *)
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let sub = PS.connect ~sw ~net ~clock ~host ~port () in
  let pub = C.connect ~sw ~net ~clock ~host ~port () in
  let admin = C.connect ~sw ~net ~clock ~host ~port () in
  Fun.protect
    ~finally:(fun () -> PS.close sub; C.close pub; C.close admin)
  @@ fun () ->
  let channel = "ps:rec:chan" in
  let _ = PS.subscribe sub [ channel ] in
  Eio.Time.sleep clock 0.05;

  (* Baseline: message flows. *)
  let _ = C.publish pub ~channel ~message:"before-kill" in
  (match PS.next_message ~timeout:2.0 sub with
   | Ok (PS.Channel { payload = "before-kill"; _ }) -> ()
   | Ok _ -> Alcotest.fail "unexpected pre-kill delivery"
   | Error `Timeout -> Alcotest.fail "baseline delivery timed out");

  (* Kill every pub/sub client on the server. *)
  (match C.custom admin [| "CLIENT"; "KILL"; "TYPE"; "pubsub" |] with
   | Ok _ -> ()
   | Error e ->
       Alcotest.failf "CLIENT KILL: %a" err_pp e);

  (* Give the subscriber time to reconnect + replay. Reconnect
     backoff defaults kick in here. *)
  Eio.Time.sleep clock 0.8;

  (* Publish again. If the replay worked, this message lands. *)
  (match C.publish pub ~channel ~message:"after-kill" with
   | Ok n when n >= 1 -> ()
   | Ok n ->
       Alcotest.failf
         "publish returned %d after reconnect; replay didn't fire" n
   | Error e -> Alcotest.failf "publish (post-kill): %a" err_pp e);

  match PS.next_message ~timeout:2.0 sub with
  | Ok (PS.Channel { payload = "after-kill"; _ }) -> ()
  | Ok _ ->
      Alcotest.fail "post-reconnect delivery had wrong payload"
  | Error `Timeout ->
      Alcotest.fail "no message arrived after reconnect + replay"

let tests =
  [ Alcotest.test_case "channel round-trip" `Quick
      test_channel_roundtrip;
    Alcotest.test_case "pattern round-trip" `Quick
      test_pattern_roundtrip;
    Alcotest.test_case "multi-channel subscribe on one handle" `Quick
      test_multi_subscribe_on_same_handle;
    Alcotest.test_case "unsubscribe stops delivery" `Quick
      test_unsubscribe_then_publish;
    Alcotest.test_case "auto-resubscribe after CLIENT KILL" `Quick
      test_auto_resubscribe_after_kill;
  ]
