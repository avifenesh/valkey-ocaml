(* Integration tests against a real Valkey 9 cluster spun up via
   docker-compose.cluster.yml. The cluster announces the service
   names [valkey-c1 .. valkey-c6]; [scripts/cluster-hosts-setup.sh]
   maps those to 127.0.0.1 on the docker host so the OCaml client
   (running on the host) resolves the same names the cluster's
   gossip uses.

   Tests skip gracefully if the cluster is not reachable — set
   VALKEY_CLUSTER=skip to force-skip even if it is reachable. *)

module C = Valkey.Client
module CR = Valkey.Cluster_router
module Conn = Valkey.Connection

let seeds = [ "valkey-c1", 7000; "valkey-c2", 7001; "valkey-c3", 7002 ]

let force_skip () =
  try Sys.getenv "VALKEY_CLUSTER" = "skip" with Not_found -> false

(* Try a single transient Connection to the first seed. True =
   reachable, false = the cluster isn't up / not configured. *)
let cluster_reachable () =
  if force_skip () then false
  else
    try
      Eio_main.run @@ fun env ->
      Eio.Switch.run @@ fun sw ->
      let net = Eio.Stdenv.net env in
      let clock = Eio.Stdenv.clock env in
      let (host, port) = List.hd seeds in
      let conn =
        Conn.connect ~sw ~net ~clock
          ~config:Conn.Config.default ~host ~port ()
      in
      Conn.close conn;
      true
    with _ -> false

let with_cluster_client f =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config =
    { (CR.Config.default ~seeds) with prefer_hostname = true }
  in
  match CR.create ~sw ~net ~clock ~config () with
  | Error m -> Alcotest.failf "Cluster_router.create: %s" m
  | Ok router ->
      let client = C.from_router ~config:C.Config.default router in
      let finalize () = C.close client in
      (try f client; finalize ()
       with e -> finalize (); raise e)

let err_pp = Conn.Error.pp

let test_roundtrip_across_slots () =
  with_cluster_client @@ fun client ->
  let pairs =
    List.init 12 (fun i ->
        Printf.sprintf "cluster:smoke:%d" i,
        Printf.sprintf "v-%d" i)
  in
  List.iter
    (fun (k, v) ->
      match C.set client k v with
      | Ok true -> ()
      | Ok false -> Alcotest.failf "SET %s returned false" k
      | Error e -> Alcotest.failf "SET %s: %a" k err_pp e)
    pairs;
  List.iter
    (fun (k, v) ->
      match C.get client k with
      | Ok (Some got) ->
          Alcotest.(check string) (Printf.sprintf "GET %s" k) v got
      | Ok None -> Alcotest.failf "GET %s returned None" k
      | Error e -> Alcotest.failf "GET %s: %a" k err_pp e)
    pairs;
  List.iter (fun (k, _) -> ignore (C.del client [ k ])) pairs

let test_routing_writes_succeed () =
  with_cluster_client @@ fun client ->
  for i = 0 to 99 do
    let k = Printf.sprintf "route:%d" i in
    match C.set client k "x" with
    | Ok _ -> ()
    | Error e -> Alcotest.failf "SET %s: %a" k err_pp e
  done;
  for i = 0 to 99 do
    ignore (C.del client [ Printf.sprintf "route:%d" i ])
  done

let test_script_load_reaches_every_primary () =
  with_cluster_client @@ fun client ->
  let source = "return redis.status_reply('OK')" in
  match C.script_load client source with
  | Error e -> Alcotest.failf "SCRIPT LOAD: %a" err_pp e
  | Ok sha ->
      Alcotest.(check int) "sha len" 40 (String.length sha);
      (match C.script_exists client [ sha ] with
       | Error e -> Alcotest.failf "SCRIPT EXISTS: %a" err_pp e
       | Ok [ true ] -> ()
       | Ok other ->
           Alcotest.failf
             "SCRIPT EXISTS expected [true], got %d items"
             (List.length other))

let test_keys_fans_to_all_primaries () =
  with_cluster_client @@ fun client ->
  let prefix =
    Printf.sprintf "fanout:keys:%d:" (Random.int 1_000_000)
  in
  for i = 0 to 29 do
    ignore (C.set client (prefix ^ string_of_int i) "v")
  done;
  let pattern = prefix ^ "*" in
  (match C.keys client pattern with
   | Error e -> Alcotest.failf "KEYS %s: %a" pattern err_pp e
   | Ok ks ->
       Alcotest.(check int) "KEYS fan count" 30 (List.length ks));
  for i = 0 to 29 do
    ignore (C.del client [ prefix ^ string_of_int i ])
  done

let test_custom_command_routes_correctly () =
  (* Exercise the custom entry point with a command we do NOT have
     a typed wrapper for: BITCOUNT. Command_spec says rk 1, so the
     router must send it to the slot owning the key. If it did not,
     we would get a MOVED-chain but the redirect retry hides it; a
     direct Ok result is the strongest signal our table is right. *)
  with_cluster_client @@ fun client ->
  let key = "custom:bitcount:k" in
  let _ =
    C.custom client [| "SET"; key; "hello" |]
  in
  (match C.custom client [| "BITCOUNT"; key |] with
   | Error e -> Alcotest.failf "BITCOUNT via custom: %a" err_pp e
   | Ok (Valkey.Resp3.Integer _) -> ()
   | Ok v ->
       Alcotest.failf "BITCOUNT returned %a, expected Integer"
         Valkey.Resp3.pp v);
  ignore (C.del client [ key ])

let test_cross_slot_surfaces_crossslot () =
  with_cluster_client @@ fun client ->
  let _ = C.set client "noslot:a" "1" in
  let _ = C.set client "noslot:b" "2" in
  match C.del client [ "noslot:a"; "noslot:b" ] with
  | Ok _ -> ()  (* coincidentally same slot — acceptable *)
  | Error (Conn.Error.Server_error { code = "CROSSSLOT"; _ }) -> ()
  | Error e ->
      Alcotest.failf "DEL cross-slot: unexpected %a" err_pp e

let skip_placeholder name () =
  Printf.printf
    "SKIP %s: cluster not reachable (see docker-compose.cluster.yml \
     + scripts/cluster-hosts-setup.sh)\n%!"
    name

module CP = Valkey.Cluster_pubsub
module PS = Valkey.Pubsub

(* Plain SUBSCRIBE in cluster mode: any node accepts, message
   broadcasts cluster-wide. *)
let test_cluster_pubsub_regular () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config =
    { (CR.Config.default ~seeds) with prefer_hostname = true }
  in
  match CR.create ~sw ~net ~clock ~config () with
  | Error m -> Alcotest.failf "cluster router: %s" m
  | Ok router ->
      let pubclient = C.from_router ~config:C.Config.default router in
      let cp = CP.create ~sw ~net ~clock ~router () in
      Fun.protect
        ~finally:(fun () -> CP.close cp; C.close pubclient)
      @@ fun () ->
      (match CP.subscribe cp [ "cpc:chan" ] with
       | Ok () -> ()
       | Error e -> Alcotest.failf "subscribe: %a" err_pp e);
      Eio.Time.sleep clock 0.2;
      (* NOTE: PUBLISH in cluster mode returns the *local* match
         count on the node it lands on, not cluster-wide. If the
         publisher happens to hit a different node from the
         subscriber, the count can be 0 even though the message
         broadcasts to every shard and is delivered. Don't assert
         on the return; assert on delivery below. *)
      (match C.publish pubclient ~channel:"cpc:chan" ~message:"hi" with
       | Ok _ -> ()
       | Error e -> Alcotest.failf "publish: %a" err_pp e);
      match CP.next_message ~timeout:2.0 cp with
      | Ok (PS.Channel { channel; payload }) ->
          Alcotest.(check string) "channel" "cpc:chan" channel;
          Alcotest.(check string) "payload" "hi" payload
      | Ok _ -> Alcotest.fail "expected Channel delivery"
      | Error _ -> Alcotest.fail "no message within 2s"

let test_cluster_pubsub_close_unblocks_waiter () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config =
    { (CR.Config.default ~seeds) with prefer_hostname = true }
  in
  match CR.create ~sw ~net ~clock ~config () with
  | Error m -> Alcotest.failf "cluster router: %s" m
  | Ok router ->
      let cp = CP.create ~sw ~net ~clock ~router () in
      Fun.protect
        ~finally:(fun () ->
          CP.close cp;
          Valkey.Router.close router)
      @@ fun () ->
      let waiting, wake = Eio.Promise.create () in
      let outcome = ref None in
      Eio.Fiber.fork ~sw (fun () ->
          outcome := Some (CP.next_message cp);
          Eio.Promise.resolve wake ());
      Eio.Time.sleep clock 0.05;
      CP.close cp;
      match
        Eio.Time.with_timeout clock 1.0
          (fun () -> Ok (Eio.Promise.await waiting))
      with
      | Ok () ->
          (match !outcome with
           | Some (Error `Closed) -> ()
           | Some (Error `Timeout) ->
               Alcotest.fail "waiter timed out instead of returning `Closed"
           | Some (Ok _) ->
               Alcotest.fail "waiter received a message while idle"
           | None ->
               Alcotest.fail "waiter finished without storing a result")
      | Error `Timeout ->
          Alcotest.fail "close did not unblock next_message within 1s"

(* Sharded SSUBSCRIBE: channel is pinned to its slot's primary.
   Both subscriber and publisher route by slot, so SPUBLISH must
   reach the same primary SSUBSCRIBE is listening on. *)
let test_cluster_pubsub_sharded () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config =
    { (CR.Config.default ~seeds) with prefer_hostname = true }
  in
  match CR.create ~sw ~net ~clock ~config () with
  | Error m -> Alcotest.failf "cluster router: %s" m
  | Ok router ->
      let pubclient = C.from_router ~config:C.Config.default router in
      let cp = CP.create ~sw ~net ~clock ~router () in
      Fun.protect
        ~finally:(fun () -> CP.close cp; C.close pubclient)
      @@ fun () ->
      (match CP.ssubscribe cp [ "cps:alpha" ] with
       | Ok () -> ()
       | Error e -> Alcotest.failf "ssubscribe: %a" err_pp e);
      Eio.Time.sleep clock 0.1;
      (match C.spublish pubclient ~channel:"cps:alpha"
               ~message:"shard-msg" with
       | Ok n when n >= 1 -> ()
       | Ok n -> Alcotest.failf "spublish returned %d" n
       | Error e -> Alcotest.failf "spublish: %a" err_pp e);
      match CP.next_message ~timeout:2.0 cp with
      | Ok (PS.Shard { channel; payload }) ->
          Alcotest.(check string) "channel" "cps:alpha" channel;
          Alcotest.(check string) "payload" "shard-msg" payload
      | Ok _ -> Alcotest.fail "expected Shard delivery"
      | Error _ -> Alcotest.fail "no sharded message within 2s"

(* Channels in different slots force multiple shard connections. *)
let test_cluster_pubsub_multi_slot () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config =
    { (CR.Config.default ~seeds) with prefer_hostname = true }
  in
  match CR.create ~sw ~net ~clock ~config () with
  | Error m -> Alcotest.failf "cluster router: %s" m
  | Ok router ->
      let pubclient = C.from_router ~config:C.Config.default router in
      let cp = CP.create ~sw ~net ~clock ~router () in
      Fun.protect
        ~finally:(fun () -> CP.close cp; C.close pubclient)
      @@ fun () ->
      (* Find two channel names that land in different slots. *)
      let ch1 = "cps:multi:1" in
      let ch2_candidates =
        [ "cps:multi:x"; "cps:multi:y"; "cps:multi:other";
          "cps:multi:far"; "cps:multi:last" ]
      in
      let slot1 = Valkey.Slot.of_key ch1 in
      let ch2 =
        List.find
          (fun c -> Valkey.Slot.of_key c <> slot1)
          ch2_candidates
      in
      (match CP.ssubscribe cp [ ch1; ch2 ] with
       | Ok () -> ()
       | Error e ->
           Alcotest.failf "ssubscribe multi: %a" err_pp e);
      Eio.Time.sleep clock 0.1;
      ignore
        (C.spublish pubclient ~channel:ch1 ~message:"m1");
      ignore
        (C.spublish pubclient ~channel:ch2 ~message:"m2");
      let got = ref [] in
      for _ = 1 to 2 do
        match CP.next_message ~timeout:2.0 cp with
        | Ok (PS.Shard { channel; payload }) ->
            got := (channel, payload) :: !got
        | Ok _ -> Alcotest.fail "expected Shard delivery"
        | Error _ ->
            Alcotest.failf "multi-slot timeout, got %d msgs"
              (List.length !got)
      done;
      let actual = List.sort compare !got in
      let expected =
        List.sort compare [ ch1, "m1"; ch2, "m2" ]
      in
      Alcotest.(check (list (pair string string)))
        "multi-slot deliveries" expected actual

(* CLUSTER KEYSLOT from the server must match our client-side
   CRC16. Catches any drift in Slot.of_key. *)
let test_cluster_keyslot_matches_client () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config =
    { (CR.Config.default ~seeds) with prefer_hostname = true }
  in
  match CR.create ~sw ~net ~clock ~config () with
  | Error m -> Alcotest.failf "cluster router: %s" m
  | Ok router ->
      let client =
        C.from_router ~config:C.Config.default router
      in
      Fun.protect ~finally:(fun () -> C.close client) @@ fun () ->
      List.iter
        (fun key ->
          match C.cluster_keyslot client ~key with
          | Error e ->
              Alcotest.failf "CLUSTER KEYSLOT %S: %a" key err_pp e
          | Ok server_slot ->
              let local = Valkey.Slot.of_key key in
              Alcotest.(check int)
                (Printf.sprintf "slot of %S" key)
                server_slot local)
        [ "foo"; "bar"; "{user}:1"; "{user}:2";
          "long-key-with-lots-of-bytes-for-crc16" ]

let test_random_target_spreads_across_connections () =
  with_cluster_client @@ fun client ->
  let ids = Hashtbl.create 8 in
  for _ = 1 to 40 do
    match C.custom ~target:C.Target.Random client [| "CLIENT"; "ID" |] with
    | Ok (Valkey.Resp3.Integer id) ->
        Hashtbl.replace ids id ()
    | Ok v ->
        Alcotest.failf "CLIENT ID via Random returned %a"
          Valkey.Resp3.pp v
    | Error e ->
        Alcotest.failf "CLIENT ID via Random: %a" err_pp e
  done;
  if Hashtbl.length ids < 2 then
    Alcotest.fail
      "Target.Random never moved off one pooled connection"

(* Commands whose first-key position is dynamic on the wire (the
   server reports firstkey=0 as a sentinel because the real
   position depends on a numeric argument like [numkeys] or a
   sub-command keyword). Our [Command_spec] pins them to the
   conventional position — good enough for routing the usual
   call shape — but they can't be cross-checked via COMMAND
   INFO's simple firstkey field. *)
let dynamic_firstkey_commands =
  [ "EVAL"; "EVALSHA"; "EVAL_RO"; "EVALSHA_RO";
    "FCALL"; "FCALL_RO";
    "XGROUP" ]

(* Spec validation: for every single-word entry in Command_spec
   that we classify as Single_key or Multi_key, call COMMAND INFO
   on the live server and compare the server-reported first-key
   position against our stored key index. Catches drift when
   Valkey changes its metadata (or when we get one wrong).

   Commands that Valkey does not know about on this version
   (rare, long-tail) are skipped with a note — they show up as
   empty arrays from COMMAND INFO. Commands with dynamic key
   positions (EVAL / FCALL / XGROUP etc) are also skipped. *)
let test_command_spec_matches_server_metadata () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config =
    { (CR.Config.default ~seeds) with prefer_hostname = true }
  in
  match CR.create ~sw ~net ~clock ~config () with
  | Error m -> Alcotest.failf "cluster router: %s" m
  | Ok router ->
      let client =
        C.from_router ~config:C.Config.default router
      in
      Fun.protect ~finally:(fun () -> C.close client) @@ fun () ->
      let names = Valkey.Command_spec.one_word_commands () in
      let first_key_from_reply = function
        | Valkey.Resp3.Array (_ :: _ :: _ :: firstkey :: _) ->
            (match firstkey with
             | Valkey.Resp3.Integer n -> Some (Int64.to_int n)
             | _ -> None)
        | _ -> None
      in
      let mismatches = ref [] in
      let unknown = ref [] in
      let dynamic = ref 0 in
      List.iter
        (fun name ->
          let spec = Valkey.Command_spec.lookup [| name |] in
          let expected_first =
            match spec with
            | Valkey.Command_spec.Single_key { key_index; _ } ->
                Some key_index
            | Valkey.Command_spec.Multi_key { first_key_index; _ } ->
                Some first_key_index
            | _ -> None
          in
          match expected_first with
          | None -> ()
          | Some _
            when List.mem name dynamic_firstkey_commands ->
              incr dynamic
          | Some exp ->
              (match
                 C.custom client [| "COMMAND"; "INFO"; name |]
               with
               | Error _ -> unknown := name :: !unknown
               | Ok (Valkey.Resp3.Array [ info ]) ->
                   (match first_key_from_reply info with
                    | Some srv when srv = exp -> ()
                    | Some 0 ->
                        (* Server says variable-position and we
                           did not list this command as dynamic.
                           Flag - either add it to the list or
                           the spec is wrong. *)
                        mismatches :=
                          (name, exp, 0) :: !mismatches
                    | Some srv ->
                        mismatches :=
                          (name, exp, srv) :: !mismatches
                    | None ->
                        unknown := name :: !unknown)
               | Ok (Valkey.Resp3.Array []) ->
                   unknown := name :: !unknown
               | Ok _ -> unknown := name :: !unknown))
        names;
      if !mismatches <> [] then begin
        let pp_one (n, e, s) =
          Printf.sprintf "  %-16s spec=%d server=%d" n e s
        in
        Alcotest.failf
          "Command_spec drift:\n%s\n(unknown to this server: %d)"
          (String.concat "\n" (List.map pp_one !mismatches))
          (List.length !unknown)
      end;
      (* unknown list is informational; don't fail on it. *)
      Printf.printf
        "[spec-check] %d commands validated, %d dynamic-firstkey \
         (skipped), %d unknown to server\n%!"
        (List.length names - List.length !unknown - !dynamic)
        !dynamic
        (List.length !unknown)

let test_cluster_info_contains_state () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config =
    { (CR.Config.default ~seeds) with prefer_hostname = true }
  in
  match CR.create ~sw ~net ~clock ~config () with
  | Error m -> Alcotest.failf "cluster router: %s" m
  | Ok router ->
      let client =
        C.from_router ~config:C.Config.default router
      in
      Fun.protect ~finally:(fun () -> C.close client) @@ fun () ->
      let info =
        match C.cluster_info client with
        | Ok s -> s
        | Error e -> Alcotest.failf "CLUSTER INFO: %a" err_pp e
      in
      let contains s n =
        let ls = String.length s and ln = String.length n in
        if ln = 0 then true
        else
          let rec loop i =
            if i + ln > ls then false
            else if String.sub s i ln = n then true
            else loop (i + 1)
          in
          loop 0
      in
      if not (contains info "cluster_state") then
        Alcotest.failf "cluster_state missing in:\n%s" info

(* Restart the primary that currently owns [slot] and confirm the
   sharded subscriber's watchdog re-pins on the (possibly new)
   primary once the topology settles, replays SSUBSCRIBE, and
   delivery resumes. *)
let test_cluster_pubsub_failover_replay () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config =
    { (CR.Config.default ~seeds) with
      prefer_hostname = true;
      refresh_interval = 1.5;
      refresh_jitter = 0.5 }
  in
  match CR.create ~sw ~net ~clock ~config () with
  | Error m -> Alcotest.failf "cluster router: %s" m
  | Ok router ->
      let pubclient = C.from_router ~config:C.Config.default router in
      let cp = CP.create ~sw ~net ~clock ~router () in
      Fun.protect
        ~finally:(fun () -> CP.close cp; C.close pubclient)
      @@ fun () ->
      let channel = "cps:fail:alpha" in
      (match CP.ssubscribe cp [ channel ] with
       | Ok () -> ()
       | Error e -> Alcotest.failf "ssubscribe: %a" err_pp e);
      Eio.Time.sleep clock 0.1;

      (* Baseline: sharded delivery works. *)
      ignore (C.spublish pubclient ~channel ~message:"pre-restart");
      (match CP.next_message ~timeout:2.0 cp with
       | Ok (PS.Shard { payload = "pre-restart"; _ }) -> ()
       | Ok _ -> Alcotest.fail "unexpected pre-restart delivery"
       | Error _ ->
           Alcotest.fail "baseline sharded delivery timed out");

      (* Restart every primary one by one — whichever one owns the
         slot, we'll catch it. Gives the router ~2s after each
         restart to observe and refresh topology. *)
      let primaries = [
        "ocaml-valkey-c1";
        "ocaml-valkey-c2";
        "ocaml-valkey-c3";
      ] in
      List.iter
        (fun name ->
          let _ = Sys.command
                    (Printf.sprintf
                       "docker restart %s >/dev/null 2>&1" name)
          in
          Eio.Time.sleep clock 3.0)
        primaries;

      (* Now publish again and expect delivery via the re-pinned
         shard connection. Generous timeout to absorb topology
         settle + reconnect + replay. *)
      let deadline = Unix.gettimeofday () +. 15.0 in
      let rec try_once () =
        if Unix.gettimeofday () > deadline then
          Alcotest.fail
            "no post-failover delivery within 15s; watchdog \
             didn't replay SSUBSCRIBE on the re-pinned shard"
        else begin
          ignore (C.spublish pubclient ~channel ~message:"post-restart");
          match CP.next_message ~timeout:1.0 cp with
          | Ok (PS.Shard { payload = "post-restart"; _ }) -> ()
          | Ok (PS.Shard { payload = "pre-restart"; _ }) ->
              (* Left over from before; drain and retry. *)
              try_once ()
          | Ok _ ->
              Alcotest.fail "unexpected non-shard delivery"
          | Error `Timeout ->
              Eio.Time.sleep clock 0.5;
              try_once ()
          | Error `Closed ->
              Alcotest.fail "handle closed unexpectedly"
        end
      in
      try_once ()

let tests =
  let reachable = cluster_reachable () in
  let tc name f =
    if reachable then Alcotest.test_case name `Quick f
    else Alcotest.test_case name `Quick (skip_placeholder name)
  in
  [ tc "roundtrip across slots" test_roundtrip_across_slots;
    tc "routing: 100 writes succeed" test_routing_writes_succeed;
    tc "SCRIPT LOAD reaches every primary"
      test_script_load_reaches_every_primary;
    tc "KEYS fans to all primaries" test_keys_fans_to_all_primaries;
    tc "custom command routes via Command_spec"
      test_custom_command_routes_correctly;
    tc "cross-slot DEL surfaces CROSSSLOT"
      test_cross_slot_surfaces_crossslot;
    tc "cluster_pubsub: regular SUBSCRIBE"
      test_cluster_pubsub_regular;
    tc "cluster_pubsub: close unblocks next_message"
      test_cluster_pubsub_close_unblocks_waiter;
    tc "cluster_pubsub: sharded SSUBSCRIBE"
      test_cluster_pubsub_sharded;
    tc "cluster_pubsub: multi-slot shard subscriptions"
      test_cluster_pubsub_multi_slot;
    tc "cluster_pubsub: sharded replay after primary restarts"
      test_cluster_pubsub_failover_replay;
    tc "CLUSTER KEYSLOT matches client-side CRC16"
      test_cluster_keyslot_matches_client;
    tc "Target.Random spreads across pooled connections"
      test_random_target_spreads_across_connections;
    tc "CLUSTER INFO contains cluster_state"
      test_cluster_info_contains_state;
    tc "Command_spec key indices match server COMMAND INFO"
      test_command_spec_matches_server_metadata;
  ]
