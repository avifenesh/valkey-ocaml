(* Same-shape benchmark harness as bin/bench/, but driving the
   ocaml-redis blocking client (redis-sync / RESP2) instead of ours.
   Same CLI flags, same scenario matrix, same output format, so the
   two can be diffed side by side.

   Concurrency model: one OS thread per concurrent connection, each
   thread holds its own [Redis_sync.Client.connection] and issues
   commands in a loop. This is the natural shape for a blocking
   client and matches how applications using redis-sync would scale. *)

module RC = Redis_sync.Client

(* ---------- CLI ---------- *)

type args = {
  host : string;
  port : int;
  ops_per_scenario : int;
  warmup : int;
  key_space : int;
  filter : string option;
}

let default_args = {
  host = "localhost";
  port = 6379;
  ops_per_scenario = 50_000;
  warmup = 1_000;
  key_space = 10_000;
  filter = None;
}

let parse_args () =
  let a = ref default_args in
  let specs =
    [ "--host", Arg.String (fun s -> a := { !a with host = s }),
      "HOST";
      "--port", Arg.Int (fun n -> a := { !a with port = n }),
      "PORT";
      "--ops", Arg.Int (fun n -> a := { !a with ops_per_scenario = n }),
      "N";
      "--warmup", Arg.Int (fun n -> a := { !a with warmup = n }),
      "N";
      "--keys", Arg.Int (fun n -> a := { !a with key_space = n }),
      "N";
      "--filter", Arg.String (fun s -> a := { !a with filter = Some s }),
      "SUBSTR";
    ]
  in
  Arg.parse specs (fun _ -> ()) "valkey-bench-redis [OPTIONS]";
  !a

(* ---------- scenarios (mirror bin/bench/) ---------- *)

type op_kind = Op_set | Op_get | Op_mix_80_20

let op_name = function
  | Op_set -> "SET"
  | Op_get -> "GET"
  | Op_mix_80_20 -> "MIX(80G/20S)"

type scenario = {
  kind : op_kind;
  value_size : int;
  concurrency : int;
}

let scenario_name s =
  Printf.sprintf "%-13s size=%-5d conc=%-3d"
    (op_name s.kind) s.value_size s.concurrency

let contains s sub =
  let ls = String.length s and lsub = String.length sub in
  if lsub = 0 then true
  else
    let rec loop i =
      if i + lsub > ls then false
      else if String.sub s i lsub = sub then true
      else loop (i + 1)
    in
    loop 0

let scenarios = [
  { kind = Op_set; value_size = 100; concurrency = 1 };
  { kind = Op_get; value_size = 100; concurrency = 1 };
  { kind = Op_set; value_size = 100; concurrency = 10 };
  { kind = Op_get; value_size = 100; concurrency = 10 };
  { kind = Op_mix_80_20; value_size = 100; concurrency = 10 };
  { kind = Op_set; value_size = 100; concurrency = 100 };
  { kind = Op_get; value_size = 100; concurrency = 100 };
  { kind = Op_mix_80_20; value_size = 100; concurrency = 100 };
  { kind = Op_set; value_size = 16 * 1024; concurrency = 10 };
  { kind = Op_get; value_size = 16 * 1024; concurrency = 10 };
  { kind = Op_mix_80_20; value_size = 1024; concurrency = 100 };
]

(* ---------- latency tracking ---------- *)

type sample_buf = {
  mutable next : int;
  data : float array;
}

let make_buf n = { next = 0; data = Array.make n 0.0 }

let push buf lat_sec =
  let i = buf.next in
  if i < Array.length buf.data then begin
    buf.data.(i) <- lat_sec;
    buf.next <- i + 1
  end

let percentile sorted_data ~count p =
  if count = 0 then 0.0
  else
    let idx = int_of_float (p *. float_of_int (count - 1)) in
    sorted_data.(idx)

let summarise buf ~elapsed =
  let count = buf.next in
  let data = Array.sub buf.data 0 count in
  Array.sort compare data;
  let p50 = percentile data ~count 0.50 in
  let p90 = percentile data ~count 0.90 in
  let p99 = percentile data ~count 0.99 in
  let p999 = percentile data ~count 0.999 in
  let max_ = if count = 0 then 0.0 else data.(count - 1) in
  let sum = Array.fold_left (+.) 0.0 data in
  let avg = if count = 0 then 0.0 else sum /. float_of_int count in
  let throughput =
    if elapsed > 0.0 then float_of_int count /. elapsed else 0.0
  in
  (count, throughput, p50, p90, p99, p999, max_, avg)

(* ---------- workload ---------- *)

let pick_key rng ~key_space =
  Printf.sprintf "bench:k:%d" (Random.State.int rng key_space)

let make_value size = String.make size 'x'

let run_op conn rng ~key_space kind ~value =
  let key = pick_key rng ~key_space in
  match kind with
  | Op_set -> ignore (RC.set conn key value : bool)
  | Op_get -> ignore (RC.get conn key : string option)
  | Op_mix_80_20 ->
      if Random.State.int rng 100 < 80 then
        ignore (RC.get conn key : string option)
      else
        ignore (RC.set conn key value : bool)

(* ---------- driver ---------- *)

let connect_to (args : args) : RC.connection =
  let spec : RC.connection_spec =
    { host = args.host; port = args.port }
  in
  RC.connect spec

let run_scenario ~args scenario =
  let value = make_value scenario.value_size in
  let total_ops = args.ops_per_scenario in

  (* Warmup on a single connection. *)
  let warmup_conn = connect_to args in
  let warmup_rng = Random.State.make [| 1 |] in
  for _ = 1 to args.warmup do
    run_op warmup_conn warmup_rng ~key_space:args.key_space scenario.kind
      ~value
  done;
  RC.disconnect warmup_conn;

  let buf = make_buf total_ops in
  let buf_mutex = Mutex.create () in

  let remaining = Atomic.make total_ops in
  let t_start = Unix.gettimeofday () in

  let worker i () =
    let rng =
      Random.State.make [| i + 7; i * 1337 |]
    in
    let conn = connect_to args in
    let rec loop () =
      let taken = Atomic.fetch_and_add remaining (-1) in
      if taken <= 0 then ()
      else begin
        let t0 = Unix.gettimeofday () in
        run_op conn rng ~key_space:args.key_space scenario.kind ~value;
        let dt = Unix.gettimeofday () -. t0 in
        Mutex.lock buf_mutex;
        push buf dt;
        Mutex.unlock buf_mutex;
        loop ()
      end
    in
    loop ();
    RC.disconnect conn
  in

  let threads =
    List.init scenario.concurrency
      (fun i -> Thread.create (worker i) ())
  in
  List.iter Thread.join threads;

  let elapsed = Unix.gettimeofday () -. t_start in
  let (count, tput, p50, p90, p99, p999, max_, avg) =
    summarise buf ~elapsed
  in
  Printf.printf
    "%-32s  n=%-6d  %-11s  avg=%-9s p50=%-9s p90=%-9s p99=%-9s p999=%-9s max=%s\n%!"
    (scenario_name scenario)
    count
    (Printf.sprintf "%.0f ops/s" tput)
    (Printf.sprintf "%.3fms" (avg *. 1000.0))
    (Printf.sprintf "%.3fms" (p50 *. 1000.0))
    (Printf.sprintf "%.3fms" (p90 *. 1000.0))
    (Printf.sprintf "%.3fms" (p99 *. 1000.0))
    (Printf.sprintf "%.3fms" (p999 *. 1000.0))
    (Printf.sprintf "%.3fms" (max_ *. 1000.0))

let () =
  let args = parse_args () in
  Random.self_init ();
  let scenarios_to_run =
    match args.filter with
    | None -> scenarios
    | Some sub ->
        List.filter (fun s -> contains (scenario_name s) sub)
          scenarios
  in
  Printf.printf
    "ocaml-redis (redis-sync) | host=%s port=%d ops=%d warmup=%d keys=%d\n%!"
    args.host args.port args.ops_per_scenario args.warmup args.key_space;
  Printf.printf "%s\n" (String.make 165 '=');
  List.iter (run_scenario ~args) scenarios_to_run
