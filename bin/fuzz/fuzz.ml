(* Stability fuzzer for the Valkey client.

   Drives a wide mix of commands through N concurrent fibers for a
   bounded duration, optionally restarting cluster nodes in the
   background to exercise reconnect / redirect / topology-refresh /
   fan-out paths. At the end, prints per-command counts, a bucketed
   latency histogram, and an outcome tally.

   What we are watching for:
     - stuck fibers (contract B: no dropped commands)
     - blown timeouts (contract A)
     - unexpected error categories (Protocol_violation, Terminal)
     - latency-tail divergence under load or during chaos

   Not a parser fuzzer — for that we plan a separate tool that feeds
   the RESP3 parser arbitrary bytes. *)

module VC = Valkey.Client
module CR = Valkey.Cluster_router
module VConn = Valkey.Connection
module VE = VConn.Error

(* ---------- CLI ---------- *)

type target =
  | Standalone of { host : string; port : int }
  | Cluster of { seeds : (string * int) list; prefer_hostname : bool }

type args = {
  target : target;
  seconds : int;
  workers : int;
  keys : int;
  value_sizes : int list;
  op_timeout : float;
  report_every : int;
  chaos_node_restart : string list;
  chaos_interval : int;
  (* If [Some n], exit 1 when the total error count exceeds n. Used
     by the pre-push hook to gate on zero errors in no-chaos mode. *)
  max_errors : int option;
  quiet : bool;  (* suppress per-command + outcome tables *)
}

let default_args = {
  target = Standalone { host = "localhost"; port = 6379 };
  seconds = 60;
  workers = 16;
  keys = 1024;
  value_sizes = [ 100; 1024; 16 * 1024 ];
  op_timeout = 2.0;
  report_every = 10;
  chaos_node_restart = [];
  chaos_interval = 30;
  max_errors = None;
  quiet = false;
}

let parse_seeds s =
  String.split_on_char ',' s
  |> List.filter (fun x -> x <> "")
  |> List.map (fun pair ->
         match String.split_on_char ':' pair with
         | [ host; port ] -> host, int_of_string port
         | _ ->
             failwith (Printf.sprintf "bad --seeds entry: %s" pair))

let parse_int_list s =
  String.split_on_char ',' s
  |> List.filter (fun x -> x <> "")
  |> List.map int_of_string

let parse_string_list s =
  String.split_on_char ',' s
  |> List.filter (fun x -> x <> "")

let parse_args () =
  let a = ref default_args in
  let usage = "valkey-fuzz [OPTIONS]" in
  let specs =
    [ "--host", Arg.String (fun h ->
          match !a.target with
          | Standalone s -> a := { !a with target = Standalone { s with host = h } }
          | _ -> a := { !a with target = Standalone { host = h; port = 6379 } }),
      "HOST standalone host (default localhost)";
      "--port", Arg.Int (fun p ->
          match !a.target with
          | Standalone s -> a := { !a with target = Standalone { s with port = p } }
          | _ -> a := { !a with target = Standalone { host = "localhost"; port = p } }),
      "PORT standalone port (default 6379)";
      "--seeds", Arg.String (fun s ->
          a := { !a with target = Cluster { seeds = parse_seeds s; prefer_hostname = true } }),
      "h:p,h:p,... use cluster router with these seeds";
      "--seconds", Arg.Int (fun n -> a := { !a with seconds = n }),
      "N run duration (default 60)";
      "--workers", Arg.Int (fun n -> a := { !a with workers = n }),
      "N concurrent fibers (default 16)";
      "--keys", Arg.Int (fun n -> a := { !a with keys = n }),
      "N keyspace size per partition (default 1024)";
      "--sizes", Arg.String (fun s -> a := { !a with value_sizes = parse_int_list s }),
      "B1,B2,... value sizes in bytes (default 100,1024,16384)";
      "--op-timeout", Arg.Float (fun f -> a := { !a with op_timeout = f }),
      "F per-command timeout seconds (default 2.0)";
      "--report-every", Arg.Int (fun n -> a := { !a with report_every = n }),
      "N progress report interval seconds (default 10)";
      "--chaos-restart", Arg.String (fun s ->
          a := { !a with chaos_node_restart = parse_string_list s }),
      "name1,name2 docker container names to restart mid-run";
      "--chaos-interval", Arg.Int (fun n -> a := { !a with chaos_interval = n }),
      "N seconds between chaos actions (default 30)";
      "--max-errors", Arg.Int (fun n -> a := { !a with max_errors = Some n }),
      "N exit 1 if total errors exceed N (default: no exit check)";
      "--quiet", Arg.Unit (fun () -> a := { !a with quiet = true }),
      " suppress per-command and outcome tables in the final report";
    ]
  in
  Arg.parse specs (fun _ -> ()) usage;
  !a

(* ---------- workload ---------- *)

(* Every op builds its command from a partitioned key so that we
   never trigger WRONGTYPE by accident. Each partition has its own
   prefix; --keys controls the range within each prefix. *)
type partition =
  | P_string    (* "fuzz:s:" *)
  | P_counter   (* "fuzz:c:" — only INCR/INCRBY/DECR touch these *)
  | P_hash      (* "fuzz:h:" *)
  | P_set       (* "fuzz:se:" *)
  | P_zset      (* "fuzz:z:" *)
  | P_list      (* "fuzz:l:" *)
  | P_stream    (* "fuzz:x:" *)
  | P_bitmap    (* "fuzz:b:" *)
  | P_hll       (* "fuzz:hl:" *)

let prefix_of = function
  | P_string -> "fuzz:s:"
  | P_counter -> "fuzz:c:"
  | P_hash -> "fuzz:h:"
  | P_set -> "fuzz:se:"
  | P_zset -> "fuzz:z:"
  | P_list -> "fuzz:l:"
  | P_stream -> "fuzz:x:"
  | P_bitmap -> "fuzz:b:"
  | P_hll -> "fuzz:hl:"

let pick_key rng ~keys part =
  Printf.sprintf "%s%d" (prefix_of part) (Random.State.int rng keys)

let pick_value rng sizes =
  let n = List.nth sizes (Random.State.int rng (List.length sizes)) in
  String.init n (fun _ ->
      Char.chr (33 + Random.State.int rng 94))

let pick_int rng ~range =
  Random.State.int rng range

(* An op spec: name (for stats), partition, weight, build. *)
type op = {
  name : string;
  partition : partition;
  weight : int;
  build : Random.State.t -> string -> args -> string array;
}

let op_table : op list =
  let open Printf in
  let v rng a = pick_value rng a.value_sizes in
  let small_num rng = string_of_int (pick_int rng ~range:1_000_000) in
  [
    (* ---------- strings (60%) ---------- *)
    { name = "GET"; partition = P_string; weight = 30;
      build = fun _ k _ -> [| "GET"; k |] };
    { name = "SET"; partition = P_string; weight = 15;
      build = fun rng k a -> [| "SET"; k; v rng a |] };
    { name = "SETEX"; partition = P_string; weight = 2;
      build = fun rng k a ->
        [| "SETEX"; k; "60"; v rng a |] };
    { name = "APPEND"; partition = P_string; weight = 2;
      build = fun rng k a -> [| "APPEND"; k; v rng a |] };
    { name = "STRLEN"; partition = P_string; weight = 2;
      build = fun _ k _ -> [| "STRLEN"; k |] };
    { name = "EXISTS"; partition = P_string; weight = 2;
      build = fun _ k _ -> [| "EXISTS"; k |] };
    { name = "TYPE"; partition = P_string; weight = 2;
      build = fun _ k _ -> [| "TYPE"; k |] };
    { name = "DEL"; partition = P_string; weight = 3;
      build = fun _ k _ -> [| "DEL"; k |] };
    { name = "EXPIRE"; partition = P_string; weight = 2;
      build = fun _ k _ -> [| "EXPIRE"; k; "300" |] };
    { name = "TTL"; partition = P_string; weight = 2;
      build = fun _ k _ -> [| "TTL"; k |] };
    { name = "PERSIST"; partition = P_string; weight = 1;
      build = fun _ k _ -> [| "PERSIST"; k |] };
    { name = "OBJECT.ENCODING"; partition = P_string; weight = 1;
      build = fun _ k _ -> [| "OBJECT"; "ENCODING"; k |] };

    (* ---------- counters ---------- *)
    { name = "INCR"; partition = P_counter; weight = 4;
      build = fun _ k _ -> [| "INCR"; k |] };
    { name = "INCRBY"; partition = P_counter; weight = 2;
      build = fun rng k _ ->
        [| "INCRBY"; k; string_of_int (pick_int rng ~range:1000) |] };
    { name = "DECR"; partition = P_counter; weight = 1;
      build = fun _ k _ -> [| "DECR"; k |] };

    (* ---------- hashes ---------- *)
    { name = "HSET"; partition = P_hash; weight = 3;
      build = fun rng k a ->
        let f = sprintf "f%d" (pick_int rng ~range:16) in
        [| "HSET"; k; f; v rng a |] };
    { name = "HGET"; partition = P_hash; weight = 4;
      build = fun rng k _ ->
        let f = sprintf "f%d" (pick_int rng ~range:16) in
        [| "HGET"; k; f |] };
    { name = "HDEL"; partition = P_hash; weight = 1;
      build = fun rng k _ ->
        let f = sprintf "f%d" (pick_int rng ~range:16) in
        [| "HDEL"; k; f |] };
    { name = "HGETALL"; partition = P_hash; weight = 2;
      build = fun _ k _ -> [| "HGETALL"; k |] };
    { name = "HLEN"; partition = P_hash; weight = 1;
      build = fun _ k _ -> [| "HLEN"; k |] };
    { name = "HINCRBY"; partition = P_hash; weight = 1;
      build = fun rng k _ ->
        let f = sprintf "cnt%d" (pick_int rng ~range:4) in
        [| "HINCRBY"; k; f; small_num rng |] };

    (* ---------- sets ---------- *)
    { name = "SADD"; partition = P_set; weight = 3;
      build = fun rng k _ ->
        [| "SADD"; k; sprintf "m%d" (pick_int rng ~range:64) |] };
    { name = "SREM"; partition = P_set; weight = 1;
      build = fun rng k _ ->
        [| "SREM"; k; sprintf "m%d" (pick_int rng ~range:64) |] };
    { name = "SISMEMBER"; partition = P_set; weight = 2;
      build = fun rng k _ ->
        [| "SISMEMBER"; k; sprintf "m%d" (pick_int rng ~range:64) |] };
    { name = "SCARD"; partition = P_set; weight = 2;
      build = fun _ k _ -> [| "SCARD"; k |] };
    { name = "SMEMBERS"; partition = P_set; weight = 1;
      build = fun _ k _ -> [| "SMEMBERS"; k |] };

    (* ---------- sorted sets ---------- *)
    { name = "ZADD"; partition = P_zset; weight = 3;
      build = fun rng k _ ->
        [| "ZADD"; k;
           string_of_int (pick_int rng ~range:1000);
           sprintf "m%d" (pick_int rng ~range:64) |] };
    { name = "ZRANGE"; partition = P_zset; weight = 2;
      build = fun _ k _ -> [| "ZRANGE"; k; "0"; "9" |] };
    { name = "ZRANGEBYSCORE"; partition = P_zset; weight = 1;
      build = fun _ k _ ->
        [| "ZRANGEBYSCORE"; k; "-inf"; "+inf"; "LIMIT"; "0"; "10" |] };
    { name = "ZCARD"; partition = P_zset; weight = 2;
      build = fun _ k _ -> [| "ZCARD"; k |] };
    { name = "ZSCORE"; partition = P_zset; weight = 2;
      build = fun rng k _ ->
        [| "ZSCORE"; k; sprintf "m%d" (pick_int rng ~range:64) |] };
    { name = "ZINCRBY"; partition = P_zset; weight = 1;
      build = fun rng k _ ->
        [| "ZINCRBY"; k; "1";
           sprintf "m%d" (pick_int rng ~range:64) |] };
    { name = "ZREM"; partition = P_zset; weight = 1;
      build = fun rng k _ ->
        [| "ZREM"; k; sprintf "m%d" (pick_int rng ~range:64) |] };

    (* ---------- lists ---------- *)
    { name = "LPUSH"; partition = P_list; weight = 2;
      build = fun rng k a -> [| "LPUSH"; k; v rng a |] };
    { name = "RPUSH"; partition = P_list; weight = 2;
      build = fun rng k a -> [| "RPUSH"; k; v rng a |] };
    { name = "LRANGE"; partition = P_list; weight = 2;
      build = fun _ k _ -> [| "LRANGE"; k; "0"; "9" |] };
    { name = "LLEN"; partition = P_list; weight = 2;
      build = fun _ k _ -> [| "LLEN"; k |] };
    { name = "LPOP"; partition = P_list; weight = 1;
      build = fun _ k _ -> [| "LPOP"; k |] };
    { name = "RPOP"; partition = P_list; weight = 1;
      build = fun _ k _ -> [| "RPOP"; k |] };

    (* ---------- streams ---------- *)
    { name = "XADD"; partition = P_stream; weight = 2;
      build = fun rng k a ->
        [| "XADD"; k; "*"; "payload"; v rng a |] };
    { name = "XLEN"; partition = P_stream; weight = 1;
      build = fun _ k _ -> [| "XLEN"; k |] };
    { name = "XRANGE"; partition = P_stream; weight = 1;
      build = fun _ k _ -> [| "XRANGE"; k; "-"; "+"; "COUNT"; "5" |] };
    { name = "XTRIM"; partition = P_stream; weight = 1;
      build = fun _ k _ -> [| "XTRIM"; k; "MAXLEN"; "~"; "100" |] };

    (* ---------- bitmaps ---------- *)
    { name = "SETBIT"; partition = P_bitmap; weight = 1;
      build = fun rng k _ ->
        [| "SETBIT"; k;
           string_of_int (pick_int rng ~range:1024);
           (if pick_int rng ~range:2 = 0 then "0" else "1") |] };
    { name = "GETBIT"; partition = P_bitmap; weight = 1;
      build = fun rng k _ ->
        [| "GETBIT"; k;
           string_of_int (pick_int rng ~range:1024) |] };
    { name = "BITCOUNT"; partition = P_bitmap; weight = 1;
      build = fun _ k _ -> [| "BITCOUNT"; k |] };

    (* ---------- HyperLogLog ---------- *)
    { name = "PFADD"; partition = P_hll; weight = 1;
      build = fun rng k _ ->
        [| "PFADD"; k; sprintf "e%d" (pick_int rng ~range:1_000_000) |] };
    { name = "PFCOUNT"; partition = P_hll; weight = 1;
      build = fun _ k _ -> [| "PFCOUNT"; k |] };

    (* ---------- scripting (single-key EVAL is cluster-safe) ---------- *)
    { name = "EVAL.GET"; partition = P_string; weight = 1;
      build = fun _ k _ ->
        [| "EVAL";
           "return redis.call('GET', KEYS[1])";
           "1"; k |] };
  ]

let op_array = Array.of_list op_table
let op_total_weight =
  Array.fold_left (fun acc o -> acc + o.weight) 0 op_array

let pick_op rng =
  let r = Random.State.int rng op_total_weight in
  let rec loop acc i =
    if i >= Array.length op_array then op_array.(0)
    else
      let acc = acc + op_array.(i).weight in
      if r < acc then op_array.(i) else loop acc (i + 1)
  in
  loop 0 0

(* ---------- outcome buckets + latency histogram ---------- *)

type outcome =
  | Ok_simple
  | Ok_bulk
  | Ok_integer
  | Ok_nil
  | Ok_other
  | E_server of string
  | E_protocol
  | E_terminal
  | E_tcp_refused
  | E_dns
  | E_tls
  | E_handshake
  | E_auth
  | E_timeout
  | E_interrupted
  | E_queue_full
  | E_circuit_open
  | E_closed

let outcome_of_reply = function
  | Valkey.Resp3.Simple_string _ -> Ok_simple
  | Valkey.Resp3.Bulk_string _ -> Ok_bulk
  | Valkey.Resp3.Integer _ -> Ok_integer
  | Valkey.Resp3.Null -> Ok_nil
  | _ -> Ok_other

let outcome_of_error = function
  | VE.Server_error { code; _ } -> E_server code
  | VE.Protocol_violation _ -> E_protocol
  | VE.Terminal _ -> E_terminal
  | VE.Tcp_refused _ -> E_tcp_refused
  | VE.Dns_failed _ -> E_dns
  | VE.Tls_failed _ -> E_tls
  | VE.Handshake_rejected _ -> E_handshake
  | VE.Auth_failed _ -> E_auth
  | VE.Timeout -> E_timeout
  | VE.Interrupted -> E_interrupted
  | VE.Queue_full -> E_queue_full
  | VE.Circuit_open -> E_circuit_open
  | VE.Closed -> E_closed

let outcome_name = function
  | Ok_simple -> "ok/simple"
  | Ok_bulk -> "ok/bulk"
  | Ok_integer -> "ok/integer"
  | Ok_nil -> "ok/nil"
  | Ok_other -> "ok/other"
  | E_server code -> Printf.sprintf "err/server:%s" code
  | E_protocol -> "err/protocol"
  | E_terminal -> "err/terminal"
  | E_tcp_refused -> "err/tcp_refused"
  | E_dns -> "err/dns"
  | E_tls -> "err/tls"
  | E_handshake -> "err/handshake"
  | E_auth -> "err/auth"
  | E_timeout -> "err/timeout"
  | E_interrupted -> "err/interrupted"
  | E_queue_full -> "err/queue_full"
  | E_circuit_open -> "err/circuit_open"
  | E_closed -> "err/closed"

(* Bucket boundaries in seconds. *)
let latency_buckets = [|
  "<1ms",   0.001;
  "<10ms",  0.010;
  "<100ms", 0.100;
  "<1s",    1.0;
  "<10s",   10.0;
|]

let num_buckets = Array.length latency_buckets + 1

let bucket_of_latency sec =
  let rec loop i =
    if i >= Array.length latency_buckets then Array.length latency_buckets
    else if sec <= snd latency_buckets.(i) then i
    else loop (i + 1)
  in
  loop 0

let bucket_label i =
  if i < Array.length latency_buckets then fst latency_buckets.(i)
  else ">=10s"

type per_op_stats = {
  mutable attempted : int;
  mutable ok_count : int;
  mutable error_count : int;
  latencies : int array;
}

let make_per_op_stats () = {
  attempted = 0; ok_count = 0; error_count = 0;
  latencies = Array.make num_buckets 0;
}

type stats = {
  per_op : (string, per_op_stats) Hashtbl.t;
  outcomes : (string, int ref) Hashtbl.t;
  mutex : Mutex.t;
  mutable max_latency : float;
}

let make_stats () = {
  per_op = Hashtbl.create 64;
  outcomes = Hashtbl.create 16;
  mutex = Mutex.create ();
  max_latency = 0.0;
}

let record stats op_name outcome latency_sec =
  Mutex.lock stats.mutex;
  let po =
    match Hashtbl.find_opt stats.per_op op_name with
    | Some v -> v
    | None ->
        let v = make_per_op_stats () in
        Hashtbl.replace stats.per_op op_name v;
        v
  in
  po.attempted <- po.attempted + 1;
  (match outcome with
   | Ok_simple | Ok_bulk | Ok_integer | Ok_nil | Ok_other ->
       po.ok_count <- po.ok_count + 1
   | _ -> po.error_count <- po.error_count + 1);
  let b = bucket_of_latency latency_sec in
  po.latencies.(b) <- po.latencies.(b) + 1;
  if latency_sec > stats.max_latency then
    stats.max_latency <- latency_sec;
  let oname = outcome_name outcome in
  (match Hashtbl.find_opt stats.outcomes oname with
   | Some r -> incr r
   | None -> Hashtbl.replace stats.outcomes oname (ref 1));
  Mutex.unlock stats.mutex

(* ---------- running a single op ---------- *)

let run_op ~timeout client rng args_cfg =
  let op = pick_op rng in
  let key = pick_key rng ~keys:args_cfg.keys op.partition in
  let cmd = op.build rng key args_cfg in
  let t0 = Unix.gettimeofday () in
  let outcome =
    try
      match VC.custom ~timeout client cmd with
      | Ok r -> outcome_of_reply r
      | Error e -> outcome_of_error e
    with
    (* Never let a stray exception terminate a worker; surface it as
       a synthetic outcome so it lands in the report. *)
    | exn ->
        E_terminal |> fun x ->
        Printf.eprintf "[worker] unexpected exn: %s (op=%s)\n%!"
          (Printexc.to_string exn) op.name;
        x
  in
  let dt = Unix.gettimeofday () -. t0 in
  op.name, outcome, dt

(* ---------- worker ---------- *)

let worker ~stats ~args ~deadline ~clock client rng =
  let rec loop () =
    if Eio.Time.now clock >= deadline then ()
    else begin
      let name, outcome, dt =
        run_op ~timeout:args.op_timeout client rng args
      in
      record stats name outcome dt;
      loop ()
    end
  in
  loop ()

(* ---------- progress reporter ---------- *)

let totals stats =
  let ok = ref 0 and err = ref 0 in
  Hashtbl.iter
    (fun _ po -> ok := !ok + po.ok_count; err := !err + po.error_count)
    stats.per_op;
  !ok, !err

let reporter ~stats ~clock ~interval ~deadline ~t_start =
  let rec loop () =
    if Eio.Time.now clock >= deadline then ()
    else begin
      Eio.Time.sleep clock (float_of_int interval);
      let elapsed = Unix.gettimeofday () -. t_start in
      let ok, err = totals stats in
      let rate =
        if elapsed > 0.0 then float_of_int (ok + err) /. elapsed
        else 0.0
      in
      let remaining =
        max 0.0 (deadline -. Eio.Time.now clock)
      in
      Printf.printf
        "[%5.0fs] ops=%d (%.0f/s) ok=%d err=%d max-latency=%.3fs \
         remaining=%.0fs\n%!"
        elapsed (ok + err) rate ok err stats.max_latency remaining;
      loop ()
    end
  in
  loop ()

(* ---------- chaos fiber ---------- *)

let docker_restart container =
  let cmd = Printf.sprintf "docker restart %s >/dev/null 2>&1" container in
  let exit_code = Sys.command cmd in
  if exit_code <> 0 then
    Printf.eprintf "[chaos] docker restart %s FAILED (exit %d)\n%!"
      container exit_code
  else
    Printf.printf "[chaos] restarted %s\n%!" container

let chaos ~clock ~containers ~interval ~deadline =
  if containers = [] then ()
  else
    let arr = Array.of_list containers in
    let rng = Random.State.make [| int_of_float (Unix.time ()) |] in
    let rec loop () =
      if Eio.Time.now clock >= deadline then ()
      else begin
        Eio.Time.sleep clock (float_of_int interval);
        if Eio.Time.now clock < deadline then begin
          let target = arr.(Random.State.int rng (Array.length arr)) in
          docker_restart target;
          loop ()
        end
      end
    in
    loop ()

(* ---------- final report ---------- *)

let print_report stats ~elapsed ~quiet =
  let ok, err = totals stats in
  let total = ok + err in
  Printf.printf "\n===== fuzz report =====\n";
  Printf.printf "elapsed      : %.1fs\n" elapsed;
  Printf.printf "total ops    : %d (%.0f ops/s)\n" total
    (if elapsed > 0.0 then float_of_int total /. elapsed else 0.0);
  Printf.printf "ok           : %d\n" ok;
  Printf.printf "err          : %d (%.3f%% of total)\n" err
    (if total > 0 then 100.0 *. float_of_int err /. float_of_int total
     else 0.0);
  Printf.printf "max latency  : %.3fs\n" stats.max_latency;

  if quiet then begin
    (* Quiet mode: always show which outcome categories fired so
       failures are still diagnosable, but skip the full per-command
       table. *)
    if err > 0 then begin
      Printf.printf "\n-- error outcomes --\n";
      let err_outcomes =
        Hashtbl.fold
          (fun k v acc ->
            if String.length k >= 4 && String.sub k 0 4 = "err/"
            then (k, !v) :: acc
            else acc)
          stats.outcomes []
        |> List.sort (fun (_, a) (_, b) -> compare b a)
      in
      List.iter
        (fun (name, count) ->
          Printf.printf "  %-30s %d\n" name count)
        err_outcomes
    end;
    print_newline ()
  end else begin
    (* per-op table, sorted by attempts desc *)
    Printf.printf "\n-- per command (sorted by attempts) --\n";
    let rows =
      Hashtbl.fold (fun k v acc -> (k, v) :: acc) stats.per_op []
      |> List.sort (fun (_, a) (_, b) -> compare b.attempted a.attempted)
    in
    List.iter
      (fun (name, po) ->
        Printf.printf
          "%-17s  att=%-7d ok=%-7d err=%-5d  latency:"
          name po.attempted po.ok_count po.error_count;
        for i = 0 to num_buckets - 1 do
          if po.latencies.(i) > 0 then
            Printf.printf " %s=%d" (bucket_label i) po.latencies.(i)
        done;
        print_newline ())
      rows;

    (* outcomes, sorted by count desc *)
    Printf.printf "\n-- outcomes --\n";
    let outcomes =
      Hashtbl.fold (fun k v acc -> (k, !v) :: acc) stats.outcomes []
      |> List.sort (fun (_, a) (_, b) -> compare b a)
    in
    List.iter
      (fun (name, count) -> Printf.printf "  %-30s %d\n" name count)
      outcomes;
    print_newline ()
  end

(* ---------- main ---------- *)

let connect_target ~sw ~net ~clock args =
  match args.target with
  | Standalone { host; port } ->
      VC.connect ~sw ~net ~clock ~host ~port ()
  | Cluster { seeds; prefer_hostname } ->
      let cfg =
        { (CR.Config.default ~seeds) with
          prefer_hostname;
          refresh_interval = 3.0; refresh_jitter = 2.0 }
      in
      (match CR.create ~sw ~net ~clock ~config:cfg () with
       | Error m -> failwith m
       | Ok router ->
           VC.from_router ~config:VC.Config.default router)

let () =
  let args = parse_args () in
  Random.self_init ();
  Eio_main.run @@ fun env ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  Eio.Switch.run @@ fun sw ->
  let client = connect_target ~sw ~net ~clock args in
  let stats = make_stats () in
  let deadline =
    Eio.Time.now clock +. float_of_int args.seconds
  in
  let worker_rngs =
    Array.init args.workers (fun i ->
        Random.State.make [| i; int_of_float (Unix.time ()) |])
  in
  Printf.printf
    "starting fuzz: workers=%d keys=%d seconds=%ds ops=%d chaos=%s\n%!"
    args.workers args.keys args.seconds
    (Array.length op_array)
    (match args.chaos_node_restart with
     | [] -> "none"
     | xs -> String.concat "," xs);
  let t_start = Unix.gettimeofday () in
  Eio.Fiber.all
    (List.concat
       [ [ (fun () ->
             reporter ~stats ~clock
               ~interval:args.report_every ~deadline ~t_start) ];
         [ (fun () ->
             chaos ~clock
               ~containers:args.chaos_node_restart
               ~interval:args.chaos_interval ~deadline) ];
         Array.to_list
           (Array.mapi
              (fun _ rng () ->
                worker ~stats ~args ~deadline ~clock client rng)
              worker_rngs);
       ]);
  let elapsed = Unix.gettimeofday () -. t_start in
  VC.close client;
  print_report stats ~elapsed ~quiet:args.quiet;
  let _, err = totals stats in
  match args.max_errors with
  | Some n when err > n ->
      Printf.eprintf
        "\nFAIL: %d errors observed, threshold was %d\n%!" err n;
      exit 1
  | _ -> ()
