(* Benchmark harness for the Valkey client.

   Runs a matrix of scenarios (SET / GET / MIX at varied payload
   sizes and concurrency levels) against a single Valkey server and
   reports per-scenario throughput and latency percentiles.

   Paired with bin/bench_redis/ (ocaml-redis blocking & Lwt) and
   scripts/run-bench.sh (which also runs valkey-benchmark for the
   C-client reference ceiling). Keep the scenario list in sync
   across the three to make comparisons meaningful. *)

module VC = Valkey.Client
module VTls = Valkey.Tls_config
module VCC = Valkey.Connection.Config

(* ---------- CLI ---------- *)

type args = {
  host : string;
  port : int;
  ops_per_scenario : int;
  warmup : int;
  key_space : int;
  filter : string option;
  json_out : string option;
  conns : int;
  tls : bool;
  scenario_set : string;
}

let default_args = {
  host = "localhost";
  port = 6379;
  ops_per_scenario = 50_000;
  warmup = 1_000;
  key_space = 10_000;
  filter = None;
  json_out = None;
  conns = 1;
  tls = false;
  scenario_set = "core";
}

let parse_args () =
  let a = ref default_args in
  let specs =
    [ "--host", Arg.String (fun s -> a := { !a with host = s }),
      "HOST (default localhost)";
      "--port", Arg.Int (fun n -> a := { !a with port = n }),
      "PORT (default 6379 for plain, 6390 if --tls without explicit --port)";
      "--ops", Arg.Int (fun n -> a := { !a with ops_per_scenario = n }),
      "N ops per scenario (default 50000)";
      "--warmup", Arg.Int (fun n -> a := { !a with warmup = n }),
      "N warmup ops per scenario (default 1000)";
      "--keys", Arg.Int (fun n -> a := { !a with key_space = n }),
      "N keyspace size (default 10000)";
      "--filter", Arg.String (fun s -> a := { !a with filter = Some s }),
      "SUBSTR run only scenarios whose name contains SUBSTR";
      "--json", Arg.String (fun s -> a := { !a with json_out = Some s }),
      "PATH emit JSON results to PATH (for bench_compare)";
      "--conns", Arg.Int (fun n -> a := { !a with conns = n }),
      "N connections_per_node (default 1)";
      "--tls", Arg.Unit (fun () -> a := { !a with tls = true }),
      " Use TLS (insecure verification; defaults port to 6390)";
      "--scenario-set", Arg.String (fun s ->
          a := { !a with scenario_set = s }),
      "NAME core | large | server-cpu | all (default core)";
    ]
  in
  Arg.parse specs (fun _ -> ()) "valkey-bench [OPTIONS]";
  (* If --tls was set without an explicit --port, flip to the TLS
     port exposed by docker-compose.yml. Users with a non-default
     setup pass --port explicitly. *)
  (if !a.tls && !a.port = default_args.port then
     a := { !a with port = 6390 });
  !a

(* ---------- scenario model ---------- *)

type op_kind =
  | Op_set
  | Op_get
  | Op_mix_80_20
  | Op_eval_spin of int
  (** EVAL a pure-Lua counting loop with [N] iterations. No keys, no
      args, no side effects — gives the server measurable CPU that
      a single multiplexed connection can't parallelise against
      itself. Concrete Lua:
      [return 0] after a for-loop of [N] iterations. *)
  | Op_sort_ro
  (** SORT_RO on a pre-populated list. Server-side CPU-bound
      response (sort + slice). Single-conn can't parallelise
      multiple in-flight SORT_ROs against one server command
      loop. *)
  | Op_mix_small_large of int
  (** One large GET per [N] small GETs against the same client.
      Exercises head-of-line blocking: a 256 KiB bulk-reply on a
      single multiplexed conn stalls every small reply queued
      behind it. *)

let op_name = function
  | Op_set -> "SET"
  | Op_get -> "GET"
  | Op_mix_80_20 -> "MIX(80G/20S)"
  | Op_eval_spin n -> Printf.sprintf "EVAL_SPIN(%d)" n
  | Op_sort_ro -> "SORT_RO"
  | Op_mix_small_large n -> Printf.sprintf "MIX_S_L(1L/%dS)" n

type scenario = {
  kind : op_kind;
  value_size : int;
  concurrency : int;
}

let scenario_name s =
  Printf.sprintf "%-17s size=%-7d conc=%-3d"
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

(* The canonical "core" matrix: SET/GET/MIX with small values at
   varied concurrency levels. Kept stable for cross-tool
   comparisons with bench_redis + valkey-benchmark. *)
let scenarios_core = [
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

(* Large-value matrix: exercises TLS single-stream CPU (each frame
   must be encrypted/decrypted on one core per conn) and
   head-of-line blocking on one multiplexed socket. Expected N>1
   hypothesis zone. *)
let scenarios_large = [
  { kind = Op_get; value_size =   1 * 1024; concurrency = 50 };
  { kind = Op_get; value_size =  64 * 1024; concurrency = 50 };
  { kind = Op_get; value_size = 256 * 1024; concurrency = 50 };
  { kind = Op_get; value_size = 1024 * 1024; concurrency = 20 };
  { kind = Op_mix_small_large 16; value_size = 256 * 1024; concurrency = 50 };
  { kind = Op_mix_small_large 32; value_size = 1024 * 1024; concurrency = 50 };
]

(* Server-CPU matrix: commands that spend real time inside the
   server's command loop. One multiplexed conn serialises all
   these through a single server-side processing slot, so N>1
   should unlock genuine parallelism here. *)
let scenarios_server_cpu = [
  { kind = Op_eval_spin 100;    value_size = 0; concurrency = 50 };
  { kind = Op_eval_spin 1_000;  value_size = 0; concurrency = 50 };
  { kind = Op_eval_spin 10_000; value_size = 0; concurrency = 50 };
  { kind = Op_sort_ro; value_size = 0; concurrency = 50 };
]

let pick_scenarios name =
  match String.lowercase_ascii name with
  | "core" -> scenarios_core
  | "large" -> scenarios_large
  | "server-cpu" | "server_cpu" | "cpu" -> scenarios_server_cpu
  | "all" -> scenarios_core @ scenarios_large @ scenarios_server_cpu
  | other ->
      Printf.eprintf
        "unknown --scenario-set %S (valid: core | large | server-cpu | all)\n"
        other;
      exit 2

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

(* Shared fixtures prepared once per [run_scenario]:
   - sort_ro_list_key: populated with 1000 elements
   - eval_spin_scripts: Sha1 -> script text used by EVAL_SPIN
*)
let sort_ro_list_key = "bench:sort_ro_list"
let mix_small_large_large_key = "bench:mix_small_large:L"
let mix_small_large_small_key_prefix = "bench:mix_small_large:S:"

let prepare_sort_ro_list client =
  (* 1000-element list. Sort on the server, response is the whole
     sorted list — CPU-bound, large-ish reply. *)
  (match VC.del client [ sort_ro_list_key ] with _ -> ());
  let args =
    Array.append [| "RPUSH"; sort_ro_list_key |]
      (Array.init 1000 (fun i -> string_of_int (999 - i)))
  in
  ignore (VC.exec client args : _ result)

let prepare_mix_small_large client ~small_key_space ~large_size =
  let large_val = make_value large_size in
  ignore (VC.set client mix_small_large_large_key large_val : _ result);
  let small_val = make_value 100 in
  for i = 0 to small_key_space - 1 do
    let k = mix_small_large_small_key_prefix ^ string_of_int i in
    ignore (VC.set client k small_val : _ result)
  done

(* Lua snippet that does nothing but burn [N] iterations of a
   tight loop server-side. [return 0] keeps the response minimal
   so the measured cost is server CPU, not wire transfer. *)
let eval_spin_script n =
  Printf.sprintf "local x=0 for i=1,%d do x=x+1 end return 0" n

(* Returns [true] on Ok, [false] on Error. Callers aggregate for
   the error counter. *)
let run_op client rng ~key_space kind ~value =
  let key = pick_key rng ~key_space in
  let ok_of_bool = function Ok _ -> true | Error _ -> false in
  match kind with
  | Op_set -> ok_of_bool (VC.set client key value)
  | Op_get -> ok_of_bool (VC.get client key)
  | Op_mix_80_20 ->
      if Random.State.int rng 100 < 80 then
        ok_of_bool (VC.get client key)
      else
        ok_of_bool (VC.set client key value)
  | Op_eval_spin n ->
      let script = eval_spin_script n in
      ok_of_bool (VC.exec client [| "EVAL"; script; "0" |])
  | Op_sort_ro ->
      ok_of_bool
        (VC.exec client
           [| "SORT_RO"; sort_ro_list_key; "LIMIT"; "0"; "1000" |])
  | Op_mix_small_large every_n_small ->
      if Random.State.int rng (every_n_small + 1) = 0 then
        ok_of_bool (VC.get client mix_small_large_large_key)
      else
        let i = Random.State.int rng (max 1 key_space) in
        let k =
          mix_small_large_small_key_prefix ^ string_of_int i
        in
        ok_of_bool (VC.get client k)

(* ---------- driver ---------- *)

(* Accumulates per-scenario summaries for the optional JSON output. *)
let results : (string * int * float * float * float * float * float * float * float) list ref =
  ref []

let build_config ~args =
  let tls =
    if args.tls then
      (* Insecure verification keeps the bench harness self-contained
         against the docker-compose.yml certs without extra flags. *)
      Some (VTls.insecure ())
    else None
  in
  let connection = { VCC.default with tls } in
  { VC.Config.default with
    connection;
    connections_per_node = args.conns }

let run_scenario ~env ~args scenario =
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config = build_config ~args in
  let client =
    VC.connect ~sw ~net ~clock ~config
      ~host:args.host ~port:args.port ()
  in
  Fun.protect ~finally:(fun () -> VC.close client) @@ fun () ->

  (* Fixture setup for scenarios that need pre-populated state. *)
  (match scenario.kind with
   | Op_sort_ro -> prepare_sort_ro_list client
   | Op_mix_small_large _ ->
       prepare_mix_small_large client
         ~small_key_space:args.key_space
         ~large_size:scenario.value_size
   | _ -> ());

  let value = make_value scenario.value_size in
  let total_ops = args.ops_per_scenario in

  (* Warmup on a single fiber. *)
  let warmup_rng = Random.State.make [| 1 |] in
  for _ = 1 to args.warmup do
    let _ok : bool =
      run_op client warmup_rng ~key_space:args.key_space scenario.kind
        ~value
    in
    ()
  done;

  let buf = make_buf total_ops in
  let buf_mutex = Mutex.create () in
  let errors = Atomic.make 0 in

  let remaining = Atomic.make total_ops in
  let t_start = Unix.gettimeofday () in

  let worker rng () =
    let rec loop () =
      let taken = Atomic.fetch_and_add remaining (-1) in
      if taken <= 0 then ()
      else begin
        let t0 = Unix.gettimeofday () in
        let ok =
          run_op client rng ~key_space:args.key_space scenario.kind
            ~value
        in
        let dt = Unix.gettimeofday () -. t0 in
        if not ok then ignore (Atomic.fetch_and_add errors 1 : int);
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
  let err_count = Atomic.get errors in
  let err_suffix =
    if err_count = 0 then ""
    else Printf.sprintf " err=%d" err_count
  in
  Printf.printf
    "%-38s  n=%-6d  %-11s  avg=%-9s p50=%-9s p90=%-9s p99=%-9s p999=%-9s max=%s%s\n%!"
    (scenario_name scenario)
    count
    (Printf.sprintf "%.0f ops/s" tput)
    (Printf.sprintf "%.3fms" (avg *. 1000.0))
    (Printf.sprintf "%.3fms" (p50 *. 1000.0))
    (Printf.sprintf "%.3fms" (p90 *. 1000.0))
    (Printf.sprintf "%.3fms" (p99 *. 1000.0))
    (Printf.sprintf "%.3fms" (p999 *. 1000.0))
    (Printf.sprintf "%.3fms" (max_ *. 1000.0))
    err_suffix;
  results :=
    (scenario_name scenario, count, tput, avg, p50, p90, p99, p999, max_)
    :: !results

(* ---------- main ---------- *)

let () =
  let args = parse_args () in
  Random.self_init ();

  let base_scenarios = pick_scenarios args.scenario_set in
  let scenarios_to_run =
    match args.filter with
    | None -> base_scenarios
    | Some sub ->
        List.filter (fun s -> contains (scenario_name s) sub)
          base_scenarios
  in

  Printf.printf
    "valkey-bench | host=%s port=%d tls=%b conns=%d set=%s ops=%d warmup=%d keys=%d\n%!"
    args.host args.port args.tls args.conns args.scenario_set
    args.ops_per_scenario args.warmup args.key_space;
  Printf.printf "%s\n" (String.make 170 '=');

  Eio_main.run @@ fun env ->
  List.iter (run_scenario ~env ~args) scenarios_to_run;

  (* Optional JSON emit. Manual encoding keeps the dep footprint
     zero (no yojson). Shape:
       { "scenarios": [
           { "name": "...",
             "count": N,
             "ops_per_sec": 12345.6,
             "avg_ms": 0.123,
             "p50_ms": ..., "p90_ms": ..., "p99_ms": ...,
             "p999_ms": ..., "max_ms": ... }, ...
         ] } *)
  (match args.json_out with
   | None -> ()
   | Some path ->
       let buf = Buffer.create 1024 in
       Buffer.add_string buf "{\n";
       Buffer.add_string buf
         (Printf.sprintf
            "  \"conns\": %d,\n\
            \  \"tls\": %b,\n\
            \  \"scenario_set\": %S,\n"
            args.conns args.tls args.scenario_set);
       Buffer.add_string buf "  \"scenarios\": [\n";
       let items = List.rev !results in
       List.iteri
         (fun i (name, count, tput, avg, p50, p90, p99, p999, max_) ->
           if i > 0 then Buffer.add_string buf ",\n";
           Buffer.add_string buf
             (Printf.sprintf
                "    { \"name\": %S, \"count\": %d, \
                 \"ops_per_sec\": %.2f, \"avg_ms\": %.3f, \
                 \"p50_ms\": %.3f, \"p90_ms\": %.3f, \
                 \"p99_ms\": %.3f, \"p999_ms\": %.3f, \
                 \"max_ms\": %.3f }"
                (String.trim name) count tput
                (avg *. 1000.0) (p50 *. 1000.0) (p90 *. 1000.0)
                (p99 *. 1000.0) (p999 *. 1000.0)
                (max_ *. 1000.0)))
         items;
       Buffer.add_string buf "\n  ]\n}\n";
       let oc = open_out path in
       Buffer.output_buffer oc buf;
       close_out oc;
       Printf.printf "wrote %s\n%!" path)
