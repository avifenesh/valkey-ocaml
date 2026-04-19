(* Benchmark harness for the Valkey client.

   Runs a matrix of scenarios (SET / GET / MIX at varied payload
   sizes and concurrency levels) against a single Valkey server and
   reports per-scenario throughput and latency percentiles.

   Paired with bin/bench_redis/ (ocaml-redis blocking & Lwt) and
   scripts/run-bench.sh (which also runs valkey-benchmark for the
   C-client reference ceiling). Keep the scenario list in sync
   across the three to make comparisons meaningful. *)

module VC = Valkey.Client

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
      "HOST (default localhost)";
      "--port", Arg.Int (fun n -> a := { !a with port = n }),
      "PORT (default 6379)";
      "--ops", Arg.Int (fun n -> a := { !a with ops_per_scenario = n }),
      "N ops per scenario (default 50000)";
      "--warmup", Arg.Int (fun n -> a := { !a with warmup = n }),
      "N warmup ops per scenario (default 1000)";
      "--keys", Arg.Int (fun n -> a := { !a with key_space = n }),
      "N keyspace size (default 10000)";
      "--filter", Arg.String (fun s -> a := { !a with filter = Some s }),
      "SUBSTR run only scenarios whose name contains SUBSTR";
    ]
  in
  Arg.parse specs (fun _ -> ()) "valkey-bench [OPTIONS]";
  !a

(* ---------- scenario model ---------- *)

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

(* Matrix of scenarios. Ordered by concurrency then op kind so the
   table reads top-to-bottom as "increasing parallelism pressure". *)
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
  data : float array;  (* seconds *)
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

let run_op client rng ~key_space kind ~value =
  let key = pick_key rng ~key_space in
  match kind with
  | Op_set -> ignore (VC.set client key value : _ result)
  | Op_get -> ignore (VC.get client key : _ result)
  | Op_mix_80_20 ->
      if Random.State.int rng 100 < 80 then
        ignore (VC.get client key : _ result)
      else
        ignore (VC.set client key value : _ result)

(* ---------- driver ---------- *)

let run_scenario ~env ~args scenario =
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let client =
    VC.connect ~sw ~net ~clock ~host:args.host ~port:args.port ()
  in
  Fun.protect ~finally:(fun () -> VC.close client) @@ fun () ->

  let value = make_value scenario.value_size in
  let total_ops = args.ops_per_scenario in

  (* Warmup on a single fiber. *)
  let warmup_rng = Random.State.make [| 1 |] in
  for _ = 1 to args.warmup do
    run_op client warmup_rng ~key_space:args.key_space scenario.kind
      ~value
  done;

  let buf = make_buf total_ops in
  let buf_mutex = Mutex.create () in

  let remaining = Atomic.make total_ops in
  let t_start = Unix.gettimeofday () in

  let worker rng () =
    let rec loop () =
      let taken = Atomic.fetch_and_add remaining (-1) in
      if taken <= 0 then ()
      else begin
        let t0 = Unix.gettimeofday () in
        run_op client rng ~key_space:args.key_space scenario.kind
          ~value;
        let dt = Unix.gettimeofday () -. t0 in
        Mutex.lock buf_mutex;
        push buf dt;
        Mutex.unlock buf_mutex;
        loop ()
      end
    in
    loop ()
  in

  let fibers =
    List.init scenario.concurrency (fun i ->
        let rng = Random.State.make [| i + 7; i * 1337 |] in
        worker rng)
  in
  Eio.Fiber.all fibers;

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

(* ---------- main ---------- *)

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
    "valkey-bench | host=%s port=%d ops=%d warmup=%d keys=%d\n%!"
    args.host args.port args.ops_per_scenario args.warmup args.key_space;
  Printf.printf "%s\n" (String.make 165 '=');

  Eio_main.run @@ fun env ->
  List.iter (run_scenario ~env ~args) scenarios_to_run
