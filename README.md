# valkey-ocaml

A modern Valkey client for OCaml 5 + [Eio](https://github.com/ocaml-multicore/eio).

**Status: early development.** Foundations, core typed command surface, and the cluster router are in; transactions and pub/sub are still to come.

## Why

Existing OCaml Redis clients predate Valkey, target RESP2, and use Lwt or Async. This project targets the current era of both stacks:

- **OCaml 5.3+**, Eio-native (effects-based, direct style)
- **RESP3 only** — no RESP2 fallback
- **Valkey 7.2+**, with first-class support for Valkey 8/9 features (HELLO `availability_zone`, `SET IFEQ`, `DELIFEQ`, hash field TTL)

No Lwt compat layer. No legacy Redis support.

## Features

**Connection layer (production-shaped):**
- Auto-reconnect with configurable backoff + jitter
- Byte-budget backpressure (not count-based)
- Circuit breaker (always on, conservative default)
- App-level keepalive fiber
- Full TLS support (self-signed or system CA bundle)
- HELLO / AUTH / SETNAME / SELECT replayed on every reconnect
- Cross-domain split (optional): parser stays on the user's domain; socket I/O runs on a dedicated `Eio.Domain_manager` thread so long parses can't stall the pipeline
- Contracts: user command timeouts honored; commands never silently dropped

**Typed commands (~71):**
Strings, counters, TTL, hashes, hash field TTL (Valkey 9), sets, lists, sorted sets, scripting (including `Script.t` with automatic `EVALSHA` fallback on `NOSCRIPT`), iteration (`SCAN`, `KEYS`), streams (non-blocking + consumer groups + admin: XPENDING / XCLAIM / XAUTOCLAIM / XINFO), blocking commands (BLPOP / BRPOP / BLMOVE / XREAD BLOCK / WAIT).

**Cluster router:**
- CRC16-XMODEM slot hashing with hashtag support
- `CLUSTER SHARDS` parser (Valkey 9 rich format: hostname, TLS port, availability zone, replication offset)
- Quorum-based topology discovery from seed addresses (parallel, configurable agreement ratio)
- Canonical-SHA change detection — diffs the pool on every refresh
- MOVED / ASK redirect retry (bounded, `ASKING` sent before the retried command on `ASK`)
- CLUSTERDOWN / TRYAGAIN retry with short back-off
- Periodic background refresh fiber (15 s base + jitter, wakes early when a redirect points at an unknown address)
- Seed fallback when the live pool can no longer reach quorum
- Typed `Read_from` strategies: `Primary`, `Prefer_replica`, AZ-affinity, AZ-affinity-with-primary
- Routing `Target` types for `by_slot` / `by_node` / `random`; `Fan_target` for `All_nodes` / `All_primaries` / `All_replicas`
- **Per-command default routing** (`Command_spec`) — a table of ~90 commands maps each name to a key position + readonly flag, so `Client.set "k" "v"` routes to `slot(k)`'s primary and `Client.get "k"` respects the caller's `Read_from`. Writes are forced to `Primary` regardless of user preference.
- **Cluster-aware admin commands**: `script_load`, `script_flush`, `script_exists`, `keys` fan to every primary and aggregate (unanimous SHA, AND-reduced existence, flattened key lists) so they work identically in standalone and cluster.
- **Explicit fan-out aggregation for `WAIT`**: `wait_replicas` (single primary), `wait_replicas_all` (per-node list), `wait_replicas_min`, `wait_replicas_sum`.
- **Standalone = one-shard cluster** — single-node connections go through the same router behind a synthetic topology, so the dispatch path is unified.

**Not yet shipped:**
- Transactions (MULTI / EXEC / WATCH / DISCARD)
- Pub/sub (SUBSCRIBE / SSUBSCRIBE family)
- Cluster-wide SCAN helper (current `scan` iterates one primary)
- "First primary to hit target wins" style fan-out (requires fiber cancellation)
- Benchmarks, fuzzer

See [ROADMAP](#roadmap) below.

## Installation

Requires OCaml 5.3+ and opam 2.2+.

```sh
opam install . --deps-only --with-test
dune build
```

Not yet published to the opam repository.

## Quick start

```ocaml
let () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let client =
    Valkey.Client.connect
      ~sw ~net ~clock
      ~host:"localhost" ~port:6379 ()
  in

  (* Typed commands *)
  let _ = Valkey.Client.set client "greeting" "hello" in
  (match Valkey.Client.get client "greeting" with
   | Ok (Some v) -> Printf.printf "got: %s\n" v
   | Ok None     -> print_endline "no value"
   | Error e     ->
       Format.printf "error: %a\n" Valkey.Connection.Error.pp e);

  Valkey.Client.close client
```

### With a dedicated IO domain

Move parser / supervisor to the caller's domain and socket I/O to a separate OS thread, so a CPU-heavy parse on a 16 KiB reply can't freeze the pipeline:

```ocaml
let domain_mgr = Eio.Stdenv.domain_mgr env in
let client = Valkey.Client.connect ~sw ~net ~clock ~domain_mgr
               ~host ~port ()
in
...
```

### Connecting to a cluster

```ocaml
let config =
  Valkey.Cluster_router.Config.default
    ~seeds:[ "node-a.example.com", 6379;
             "node-b.example.com", 6379;
             "node-c.example.com", 6379 ]
in
match Valkey.Cluster_router.create ~sw ~net ~clock ~config () with
| Error m -> failwith m
| Ok router ->
    let client =
      Valkey.Client.from_router
        ~config:Valkey.Client.Config.default router
    in
    let _ = Valkey.Client.set client "user:42" "ada" in
    ...
```

The router discovers topology from the seeds via quorum, opens a connection per node, handles `MOVED` / `ASK` redirects, and refreshes periodically in the background. If every pool node becomes unreachable it falls back to re-resolving from the seed list.

### With TLS against a managed service

```ocaml
let tls =
  match Valkey.Tls_config.with_system_cas ~server_name:"your.redis.amazonaws.com" () with
  | Ok t -> t
  | Error m -> failwith m
in
let config =
  { Valkey.Client.Config.default with
    connection = { Valkey.Connection.Config.default with tls = Some tls } }
in
let client = Valkey.Client.connect ~sw ~net ~clock ~config
               ~host:"your.redis.amazonaws.com" ~port:6379 () in
...
```

## Development setup

Requires: Docker, OCaml 5.3+, opam 2.2+.

```sh
# One-time: generate self-signed certs for the TLS integration tests
bash scripts/gen-tls-certs.sh

# Start a local Valkey 9 on :6379 (plain) and :6390 (TLS)
docker compose up -d

# Build and run tests
dune build
dune runtest
```

## Architecture

Three layers: `Connection` owns the socket + protocol state machine, `Cluster_router` owns the fleet (topology, pool, slot dispatch, redirect retry, refresh), `Client` is typed commands sitting on top of any router.

### Connection states

`Connecting → Alive → (Recovering ⇄ Alive) → Dead`

Recovery replays the full handshake (HELLO + AUTH + SETNAME + SELECT) on every reconnection. In-flight requests stay pending until reconnection succeeds or their command timeout fires. Terminal errors (auth failure, protocol violation, user close) move to `Dead`; everything else is recoverable.

### Cross-domain I/O (optional)

Pass `?domain_mgr` to `Client.connect` to route:
- **Domain A (dedicated)**: `in_pump` reads socket chunks into a `Cstruct.t Eio.Stream.t`; `out_pump` drains the command queue to the socket. Pure syscalls.
- **Domain B (caller)**: parser, supervisor, keepalive, user request fibers. CPU-heavy.

This avoids the cooperative-scheduler pathology where a long RESP3 parse stalls I/O and cascades into a pipeline freeze.

### Blocking commands policy

This client is **multiplexed**. Sending a blocking command (BLPOP, BRPOP, BLMOVE, XREAD BLOCK, WAIT, …) on the Client you use for normal traffic stalls every other fiber sharing that socket.

**Open a dedicated `Client.t` for blocking commands.** The typed blocking API does not auto-switch to an exclusive connection. This is deliberate; matches GLIDE and StackExchange.Redis conventions.

### Standalone is cluster-of-one

`Client.connect ~host ~port` builds a synthetic single-shard topology, puts the one connection in a `Node_pool`, and wraps it with the same `Cluster_router` used for real clusters. The only code path that differs is the absence of a refresh fiber — there is nothing to refresh against a single seed. Everything else (slot dispatch, MOVED handling, `Read_from`, typed commands) runs identically.

## Pre-push gate

The repo ships a tracked pre-push hook (`scripts/git-hooks/pre-push`) that runs `dune build`, the full test suite, and a 30-second stability fuzz against a standalone Valkey (and the docker-compose cluster if it is up) with a **zero-error threshold**. Set it up once:

```sh
bash scripts/install-git-hooks.sh
```

Override knobs:
- `SKIP_FUZZ=1 git push` — skip the fuzz step (still runs build + tests).
- `SKIP_PRE_PUSH=1 git push` — emergency escape; skips everything.
- `FUZZ_SECONDS=60 git push` — longer fuzz window.

## Roadmap

Next major pieces, roughly in order:

1. **Live cluster integration tests** — docker compose cluster, exercise MOVED / ASK / refresh / seed-fallback / fan-out paths
2. **Transactions** — MULTI / EXEC / WATCH / DISCARD with connection pinning under multiplex
3. **Pub/sub** — SUBSCRIBE lifecycle, sharded variants, push dispatch
4. **Cluster-wide SCAN** helper that walks every primary
5. **Benchmarks** — 80/20 GET/SET across 100 B / 1 KiB / 16 KiB at 1 / 10 / 100 concurrency
6. **Fuzzer** — RESP3 parser against arbitrary bytes

## License

MIT. See [LICENSE](LICENSE).
