# Performance

## TL;DR numbers

Against `valkey-benchmark` (the C client) as the reference
ceiling, on loopback, single-instance Valkey 9:

| Payload | Concurrency | ocaml-valkey vs C | ocaml-valkey vs ocaml-redis |
|---------|-------------|-------------------|------------------------------|
| 100 B   | 1           | 67 %              | 1.4× faster                  |
| 100 B   | 100         | 94 %              | 3.3× faster                  |
| 1 KiB   | 100         | 93 %              | 3.4× faster                  |
| 16 KiB  | 100         | 91 %              | 3.2× faster                  |

Full matrix + methodology: [`BENCHMARKS.md`](../BENCHMARKS.md).

The short version: **at concurrency ≥ 10, we are 3× faster than
the existing OCaml client and within 5–10 % of the C reference
ceiling.**

## Why

A few design choices account for most of the gap:

- **Eio fiber multiplexing.** One socket, many fibers. All
  commands flow through a single in-flight queue that the
  connection writes out back-to-back; replies come back
  pipelined. No threadpool hop, no context switch per request.
- **Single-allocation send path.** `Resp3_writer.command_to_cstruct`
  allocates exactly one `Cstruct.t` of the final wire size and
  blits each argument in once. Zero intermediate `Buffer`, zero
  extra copy at `Buffer.contents`. This optimisation alone took
  SET 16 KiB from 47 % to 91 % of C.
- **No per-request heap churn for the hot path.** Parse results
  are produced by a reusable `Byte_reader.t` and dropped into
  typed wrappers that return `result` values, avoiding long-
  lived closures.
- **Cross-domain I/O split (optional).** Pass `?domain_mgr` to
  `Connection.connect` and the socket read/write fibers live on
  a dedicated OS thread. Useful when the application's Eio
  domain is CPU-bound and can't starve the parser. Not enabled
  by default — loopback doesn't need it.

## Tuning knobs

### One `Client.t` or many?

A single `Client.t` already multiplexes all the concurrency you
want — internally it's one socket + one reader fiber + many
writer fibers. You don't need a pool.

**Open more than one `Client.t` only when you need:**

- Dedicated connections for blocking commands (`BLPOP`, `BRPOP`,
  `XREAD BLOCK`). A blocked command owns the connection until it
  returns. If other commands share that client, they wait.
- Dedicated connections for transactions (`MULTI`/`EXEC` on a
  pinned connection). See [transactions.md](transactions.md).
- Dedicated connections for pub/sub. The subscribe-mode
  connection can't be multiplexed; open a separate `Pubsub.t` or
  `Cluster_pubsub.t`.
- Isolating latency classes. E.g., an interactive-traffic client
  vs. a background-batch client.

For regular `GET`/`SET`/`HSET`/etc., one client is enough.

### Concurrency

Throughput plateaus around concurrency 50–100 on a single Valkey
instance (loopback). More fibers don't help — the server's event
loop is the bottleneck past that point. For higher throughput,
scale to a cluster or run multiple Valkey replicas behind
`Read_from.Prefer_replica` for readonly traffic.

### Timeouts

Default: no per-call timeout. Pass `?timeout` to any command:

```ocaml
Valkey.Client.get client "k" ~timeout:0.5
```

Timeouts compose with cluster retries — each retry gets the full
timeout. So `~timeout:0.5` on a command that hits one `MOVED`
before succeeding can take up to 1 s. If that matters, use a
smaller per-call timeout.

### `max_redirects`

Cluster routing retries up to `max_redirects` (default 5) times
on MOVED/ASK/CLUSTERDOWN/TRYAGAIN/Interrupted. Raising to 8–10
helps during large failovers. Lowering to 2–3 fails fast if
your app has its own retry policy.

### Connection-level rate limiting

`Connection.Config.bucket_size` (bytes in flight allowed before
new requests block) exists for memory-pressure protection. The
default is generous — 64 MiB. Lower it if you run multiple
clients on a small VM.

## Common perf pitfalls

### Accidentally single-threaded access

```ocaml
(* This is sequential, not pipelined. Each call awaits its reply. *)
for i = 0 to 999 do
  ignore (Valkey.Client.set client (Printf.sprintf "k%d" i) "v")
done
```

To pipeline, issue in parallel fibers:

```ocaml
Eio.Fiber.List.iter
  (fun i ->
     ignore (Valkey.Client.set client (Printf.sprintf "k%d" i) "v"))
  (List.init 1000 Fun.id)
```

`Fiber.List.iter` runs each in its own fiber; they queue on the
connection in parallel.

### Using Lwt primitives

Don't. This library is Eio-only; any `Lwt.*` call runs outside
the Eio scheduler and either blocks the Eio domain or just
doesn't work. If you're coming from Lwt code, wrap at the
boundary (e.g., with `Eio_unix.run_in_systhread`).

### Writing a big command with `String.concat`

`Resp3_writer` already has a fast path. Don't build a giant
command as a `string`; pass it as a `string array`:

```ocaml
Valkey.Client.custom client
  [| "HSET"; key;
     "field1"; v1;
     "field2"; v2;
     (* ... *)
  |]
```

The writer blits each element in at its final offset without
rebuilding.

### Large `HGETALL` into a small client

`HGETALL` returns the whole hash. For a 100k-field hash this is
hundreds of MiB of reply. Either use `HSCAN` to iterate, or use
`HMGET` with a known field list.

## Profiling

We keep a local profile build via `dune build --profile=bench`.
For flame graphs:

```bash
dune build --profile=bench
perf record -F 500 -g --call-graph=dwarf \
  _build/bench/bin/bench/bench.exe --ops 100000
perf script | stackcollapse-perf.pl | flamegraph.pl > flame.svg
```

Most hot paths are in `lib/resp3_parser.ml` (server→client) and
`lib/resp3_writer.ml` (client→server). If you see anything
outside those dominate under a realistic workload, open an issue.

## See also

- [`BENCHMARKS.md`](../BENCHMARKS.md) — full scenario matrix +
  methodology + optimisation history.
- [`bin/bench/`](../../bin/bench/) — run locally.
- [`bin/bench_compare/`](../../bin/bench_compare/) — diff two
  bench JSON files.
