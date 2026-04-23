# ocaml-valkey

A modern Valkey client for OCaml 5 + [Eio](https://github.com/ocaml-multicore/eio).

**Status: alpha.** v0.2.0 is live on opam (0.1.0 was superseded
after `@runtest` hit a server that doesn't exist in the opam
sandbox). Full core + cluster + batch (incl. WATCH guards +
cross-slot `pfcount_cluster`) + fuzz + CI + docs. Phases 0–7
closed; Phase 8 (client-side caching) next.

## Why

Existing OCaml Redis clients predate Valkey, target RESP2, and use
Lwt or Async. This project targets the current era of both stacks:

- **OCaml 5.3+**, Eio-native (effects-based, direct-style concurrency)
- **RESP3 only** — no RESP2 fallback
- **Valkey 7.2+**, with first-class support for Valkey 8/9 features
  (HELLO `availability_zone`, `SET IFEQ`, `DELIFEQ`, hash field
  TTL, sharded pub/sub)

No Lwt compat layer. No legacy Redis support.

## Docs

- **New to the library?** Start with [docs/getting-started.md](docs/getting-started.md).
- **API reference** (odoc) — built by the docs workflow and
  deployed to GitHub Pages. Run locally with `dune build @doc`;
  open `_build/default/_doc/_html/valkey/Valkey/index.html`.
- **Guides** for deeper topics:
  - [docs/cluster.md](docs/cluster.md) — topology, MOVED/ASK,
    Read_from, hashtags, failover walkthrough.
  - [docs/batch.md](docs/batch.md) — scatter-gather and atomic
    batches, `mget_cluster` / `mset_cluster` / etc., timeout
    semantics.
  - [docs/transactions.md](docs/transactions.md) — MULTI/EXEC,
    WATCH, when *not* to use transactions.
  - [docs/pubsub.md](docs/pubsub.md) — regular + sharded pub/sub,
    auto-resubscribe, delivery guarantees.
  - [docs/tls.md](docs/tls.md) — system CAs, dev certs,
    observed overhead.
  - [docs/performance.md](docs/performance.md) — one-client model,
    tuning knobs, profiling.
  - [docs/troubleshooting.md](docs/troubleshooting.md) — every
    `Connection.Error.t` case, failover symptoms.
  - [docs/security.md](docs/security.md) — AUTH, ACLs, TLS, audit
    of what goes on the wire.
  - [docs/migration-from-ocaml-redis.md](docs/migration-from-ocaml-redis.md) —
    side-by-side port guide.

## What you get

### Connection spine

- Auto-reconnect with configurable backoff + jitter; HELLO / AUTH
  / SETNAME / SELECT replayed on every reconnect.
- Byte-budget backpressure (not count-based).
- Always-on circuit breaker with a conservative default.
- App-level keepalive fibre.
- Full TLS support (self-signed or system CA bundle).
- Optional cross-domain split: parser stays on the user's domain;
  socket I/O runs on a dedicated `Eio.Domain_manager` thread so
  long parses can't stall the pipeline.
- Contracts: user command timeouts honoured; commands never
  silently dropped.
- `?on_connected` hook — fires after every successful handshake
  (used by `Pubsub` to replay subscriptions).

### Cluster router

- CRC16-XMODEM slot hashing with hashtag support.
- `CLUSTER SHARDS` parser (Valkey 9 rich format).
- Quorum-based topology discovery from seed addresses;
  canonical-SHA change detection.
- MOVED / ASK redirect retry (bounded, `ASKING` sent before the
  retried command on `ASK`).
- CLUSTERDOWN / TRYAGAIN retry with exponential back-off (up to
  ~3 s).
- Interrupted / Closed retry so callers don't see transient
  tear-downs.
- Periodic background refresh fibre, wakes early on
  unknown-address redirects.
- Seed fallback when the live pool can no longer reach quorum.
- Typed `Read_from` — Primary, Prefer_replica (random), AZ-affinity
  with 3-tier fallback (in-AZ replica → any replica → primary).
- `Target` types for `By_slot` / `By_node` / `Random`;
  `Fan_target` for `All_nodes` / `All_primaries` / `All_replicas`.
- **Per-primary atomic mutex** (`Router.atomic_lock_for_slot`) —
  serialises concurrent MULTI/EXEC blocks (from `Batch ~atomic` or
  `Transaction`) on the shared pinned connection. Non-atomic
  traffic bypasses the lock and multiplexes freely.
- **Standalone = one-shard cluster** — single-node connections go
  through the same router behind a synthetic topology; dispatch
  is unified.

### Typed commands

- ~140 typed helpers across strings, counters, TTL, hashes
  (incl. field TTL), sets, lists, **sorted sets** (ZADD with 6
  mode variants / ZINCRBY / ZRANK / ZSCORE / ZPOPMIN/MAX /
  WITHSCORES variants), **bitmaps**, **HLL**, **geo**, **generic
  keyspace**, **CLIENT admin**, **FUNCTION + FCALL**,
  **CLUSTER introspection**, **LATENCY**, **MEMORY**, streams
  (non-blocking + consumer groups + admin), scripting (with
  automatic `EVALSHA → EVAL` fallback on `NOSCRIPT`), blocking
  commands.
- **Per-command default routing** (`Command_spec`): ~230 command
  + sub-command entries, cross-checked against live
  `COMMAND INFO` in a test.
- **Cluster-aware admin**: `SCRIPT LOAD/FLUSH/EXISTS`, `KEYS`,
  `FUNCTION LOAD/DELETE/FLUSH/LIST` fan to every primary and
  aggregate so they behave identically in standalone and cluster.
- **Custom commands** via `Client.custom` / `custom_multi` — any
  Valkey command (including ones we don't wrap typed-side) routes
  correctly.
- **Named / registered commands** via `Named_commands`: register
  a template once (`[| "HSET"; "$1"; "$2"; "$3" |]`) and invoke
  by name; same for named transactions.

### Batch (scatter-gather + atomic)

- **`Valkey.Batch.t`** — one primitive, two modes:
  - **Non-atomic** (default): queue heterogeneous commands,
    `Batch.run` splits by slot, runs a per-slot pipeline in
    parallel, merges replies in input order. Partial success is
    the norm; each command gets its own `result`.
  - **Atomic** (`~atomic:true`): in cluster mode, all keys must
    hash to one slot
    (client-side CROSSSLOT validation). Single
    `WATCH`/`MULTI`/cmds/`EXEC` burst on the slot's primary;
    returns `Ok (Some results)` on commit, `Ok None` on WATCH
    abort.
- **Fan-out commands** (`SCRIPT LOAD`, `FLUSHALL`, `CLUSTER
  NODES`, …) route through `exec_multi` in non-atomic mode and
  return `Many (node_id, reply) list` entries; they're rejected
  at `queue` time in atomic mode.
- **Wall-clock `?timeout`** applies to the whole batch; completed
  commands keep their replies, stragglers come back as
  `One (Error Timeout)`.
- **Typed cluster helpers**: `Batch.mget_cluster`,
  `mset_cluster`, `del_cluster`, `unlink_cluster`,
  `exists_cluster`, `touch_cluster`, `pfcount_cluster` (HLL union
  across slots via `DUMP` / `RESTORE` / `PFMERGE`).
- **WATCH guards** for read-modify-write CAS —
  `Batch.with_watch client ["k"] (fun guard -> …)` holds `WATCH`
  across the closure (and the watched primary's atomic mutex),
  commits via `Batch.run_with_guard`, guarantees `UNWATCH` on any
  exit. Matches the classic "read, decide, maybe commit" pattern.
- **Concurrent atomic batches are safe** — `Router.atomic_lock_for_slot`
  serialises MULTI/EXEC blocks on the shared connection; ops on
  different primaries run in parallel.

See [docs/batch.md](docs/batch.md).

### Transactions

- `Valkey.Transaction.begin_ / queue / exec / discard` +
  `with_transaction` scope helper — thin façade over atomic
  `Batch` as of 0.2.0 (one primitive, one mental model).
- Buffered model: bad-arity / unknown-command errors surface
  inside `exec`'s reply array, not at `queue` time. Fan-out
  commands are still rejected at `queue` with a `Terminal` error.
- `~watch` opens a `Batch.guard`, so the watched primary's atomic
  mutex is held for the whole read-modify-exec window.
- `exec` returns `(Resp3.t list option, Error.t) result` —
  `Ok None` on WATCH abort; a fresh retry loop is the expected
  response.

### Pub/sub

Two handles that cover the whole pub/sub surface:

- **`Pubsub.t`** — dedicated subscriber connection. Typed
  `message` variants (Channel / Pattern / Shard). Tracks the
  subscription set under a mutex; on every reconnect, the
  `on_connected` hook replays `SUBSCRIBE` / `PSUBSCRIBE` /
  `SSUBSCRIBE`.
- **`Cluster_pubsub.t`** — cluster-aware. One handle covers
  regular pub/sub (global connection, broadcast across shards on
  Valkey 7+) and sharded pub/sub (one pinned connection per
  subscribed slot). A watchdog fibre polls
  `Router.endpoint_for_slot` every second; when a primary
  changes (failover), the shard connection is closed, reopened at
  the new address, and `on_connected` replays the slot's
  `SSUBSCRIBE` set. Verified by an integration test that
  `docker restart`s every primary in sequence and asserts
  post-failover delivery.

Publish side has typed `Client.publish` (cluster-wide broadcast)
and `Client.spublish` (slot-pinned).

### Testing, fuzzing, chaos

- **~240 tests** across two targets:
  - `dune runtest` → 89 pure-unit tests (RESP3, retry state
    machine, slot / topology / discovery / redirect parsers,
    command-spec table). No server needed — this is what ships
    through opam CI.
  - `dune exec test/run_tests.exe` → the full integration suite
    (everything that talks to a live Valkey / cluster). Takes
    ~20 s end-to-end against `docker compose up -d`.
- **Parser fuzzer** (`bin/fuzz_parser/`) — byte-level + tree
  mutation + length-field poisoning + shrinker. 10 M strict
  clean at ~145 k inputs/s.
- **Stability fuzzer** (`bin/fuzz/`) — live-server soak with 48
  commands, optional docker-restart chaos, zero-error thresholds.
- **Soak tool** (`bin/soak/`) — long-running stability with
  `Gc.quick_stat` + `/proc/self/fd` sampling + OLS slope
  detection.
- **TCP chaos** via toxiproxy (`docker-compose.toxiproxy.yml` +
  `scripts/chaos/chaos.sh`) — latency / loss / bandwidth / reset
  / close toxics on demand.
- **Retry state-machine tests** — every branch of
  `handle_retries` covered with scripted dispatch + wall-clock
  verification of the CLUSTERDOWN exponential back-off schedule.
- **Round-trip proptest** — `∀ v, parse (encode v) = v` over 10 k
  random leaves + 10 k random nested trees + edge cases.

### CI / CD

- **`ci.yml`** — Ubuntu × OCaml {5.3, 5.4} full integration
  (docker cluster + tests + parser fuzz + 30 s standalone fuzz +
  30 s cluster fuzz); macOS × {5.3, 5.4} docker-free portability
  subset.
- **`coverage.yml`** — bisect_ppx instrumented tests, HTML
  artifact, 60 % floor (baseline 63 %), gh-pages deploy on main.
- **`fuzz-nightly.yml`** — 02:00 UTC: 200 M parser fuzz strict +
  15 min cluster stability with docker-restart chaos. Auto-files
  an issue on failure.
- **`bench.yml`** — per-PR delta vs `main` with 10 % regression
  gate; main pushes stash the baseline on `bench-history` branch.
- **`docs.yml`** — odoc HTML + guides under `/guides/`; gh-pages
  deploy on main.

### Benchmarks

Apples-to-apples with [ocaml-redis](https://github.com/0xffea/ocaml-redis)
(RESP2, blocking) and `valkey-benchmark` (the C client, as a
reference ceiling):

| Scenario            |       Ours | ocaml-redis |        C | Ours/C | Ours/ocaml-redis |
|---------------------|-----------:|------------:|---------:|-------:|-----------------:|
| SET 100 B conc=1    |  7.3 k r/s |   8.5 k r/s |  8.8 k   |  83 %  |           0.86x  |
| GET 100 B conc=100  |  199 k r/s |    60 k r/s |  202 k   |  99 %  |         **3.3x** |
| MIX 1 KiB conc=100  |  110 k r/s |    47 k r/s |    —     |   —    |         **2.3x** |
| SET 16 KiB conc=10  |   49 k r/s |    26 k r/s |   55 k   |  91 %  |           1.9x   |

At concurrency ≥ 10 we are **3–3.5× faster than ocaml-redis and
within 85–96 % of the C reference**. Full matrix + methodology +
optimisation history in [BENCHMARKS.md](BENCHMARKS.md). Run
locally with `bash scripts/run-bench.sh`. Batch paths add a
further ≈20× speedup vs per-key loops on 1000-key bulk operations
in cluster mode (`examples/10-batch/bulk.ml`).

## Installation

Requires OCaml 5.3+ and opam 2.2+.

```sh
opam update
opam install valkey eio_main
```

Or to build from a checkout:

```sh
opam install . --deps-only --with-test
dune build
```

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

  let _ = Valkey.Client.set client "greeting" "hello" in
  (match Valkey.Client.get client "greeting" with
   | Ok (Some v) -> Printf.printf "got: %s\n" v
   | Ok None     -> print_endline "no value"
   | Error e     ->
       Format.printf "error: %a\n" Valkey.Connection.Error.pp e);

  Valkey.Client.close client
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
      Valkey.Client.from_router ~config:Valkey.Client.Config.default router
    in
    let _ = Valkey.Client.set client "user:42" "ada" in
    ...
```

See [docs/cluster.md](docs/cluster.md).

### Bulk ops across cluster slots

```ocaml
(* MGET that spans slots — splits by slot, parallel pipelines,
   merges in input order. *)
match Valkey.Batch.mget_cluster client
        [ "user:1"; "user:2"; "user:3"; (* ...1000 more... *) ]
with
| Ok pairs -> List.iter (fun (k, v_opt) -> ...) pairs
| Error e  -> ...
```

Or a heterogeneous batch:

```ocaml
let b = Valkey.Batch.create () in
let _ = Valkey.Batch.queue b [| "SET"; "a"; "1" |] in
let _ = Valkey.Batch.queue b [| "INCR"; "ctr" |] in
let _ = Valkey.Batch.queue b [| "HSET"; "h"; "k"; "v" |] in
let _ = Valkey.Batch.queue b [| "GET"; "a" |] in
match Valkey.Batch.run ~timeout:2.0 client b with
| Ok (Some results) -> Array.iter decode results
| _ -> ...
```

See [docs/batch.md](docs/batch.md).

### Transactions

```ocaml
match
  Valkey.Transaction.with_transaction client ~hint_key:"user:42" @@ fun tx ->
  let _ = Valkey.Transaction.queue tx [| "HSET"; "user:42"; "seen"; "now" |] in
  let _ = Valkey.Transaction.queue tx [| "EXPIRE"; "user:42"; "3600" |] in
  ()
with
| Ok (Some replies) -> (* committed; replies.[i] = i-th queued reply *)
| Ok None           -> (* WATCH abort — caller decides whether to retry *)
| Error e           -> (* transport / protocol failure *)
```

See [docs/transactions.md](docs/transactions.md).

### Pub/sub (cluster-aware)

```ocaml
let cp =
  Valkey.Cluster_pubsub.create ~sw ~net ~clock ~router ()
in
let _ = Valkey.Cluster_pubsub.ssubscribe cp [ "orders:created" ] in

let rec loop () =
  match Valkey.Cluster_pubsub.next_message ~timeout:30.0 cp with
  | Ok (Shard { channel; payload }) ->
      Printf.printf "[%s] %s\n%!" channel payload;
      loop ()
  | Error `Timeout -> loop ()
  | Error `Closed  -> ()
in
loop ()
```

On primary failover the watchdog re-pins the slot's connection
and replays `SSUBSCRIBE` automatically. See
[docs/pubsub.md](docs/pubsub.md).

### With TLS against a managed service

```ocaml
let tls =
  match Valkey.Tls_config.with_system_cas
          ~server_name:"your.redis.amazonaws.com" () with
  | Ok t -> t | Error m -> failwith m
in
let config =
  { Valkey.Client.Config.default with
    connection = { Valkey.Connection.Config.default with tls = Some tls } }
in
let client = Valkey.Client.connect ~sw ~net ~clock ~config
               ~host:"your.redis.amazonaws.com" ~port:6379 () in
...
```

See [docs/tls.md](docs/tls.md).

## Development setup

Requires: Docker, OCaml 5.3+, opam 2.2+.

```sh
# One-time: generate self-signed certs for the TLS integration tests
bash scripts/gen-tls-certs.sh

# Start a local Valkey 9 on :6379 (plain) and :6390 (TLS)
docker compose up -d

# Optional: spin up a 3-primary / 3-replica cluster for integration tests
sudo bash scripts/cluster-hosts-setup.sh     # one-time: /etc/hosts entries
docker compose -f docker-compose.cluster.yml up -d

# Build everything + pure-unit tests (no server needed)
dune build
dune runtest

# Full integration suite (needs the docker services above)
dune exec test/run_tests.exe
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full developer
workflow — fuzzers, bench, coverage, pre-push gate, style rules,
PR checklist.

## Architecture

Four layers, bottom up:

- **`Connection`** owns one socket and the protocol state
  machine (`Connecting → Alive → Recovering → Dead`). Pipelines
  commands through a write fibre and drains replies through a
  parser fibre; optionally splits I/O across domains. Provides
  `request` (reply-matched) and `send_fire_and_forget` (no reply
  expected, used by `Pubsub`).
- **`Cluster_router`** owns the fleet: topology discovery, node
  pool, slot dispatch, redirect retry, periodic refresh, seed
  fallback, typed `Read_from` / `Target` / `Fan_target`,
  per-primary atomic mutex. Standalone is wrapped as a synthetic
  single-shard cluster through the same dispatch path.
- **`Client`** is the typed command surface built on any
  `Router.t`. Handles `Command_spec`-driven routing, fan-out
  aggregation, and the `Client.custom` escape hatch.
- **`Batch`** / **`Transaction`** / **`Pubsub`** /
  **`Cluster_pubsub`** / **`Named_commands`** sit beside `Client`
  and use its primitives. Each is a small, focused module with
  its own integration tests.

## Pre-push gate

`scripts/git-hooks/pre-push` runs `dune build`, the full test
suite, the parser fuzz at 100 k iterations (strict), and a
30-second stability fuzz (both standalone and, if up, the
cluster) with a **zero-error threshold**. Set it up once:

```sh
bash scripts/install-git-hooks.sh
```

Override knobs:
- `SKIP_FUZZ=1 git push` — skip fuzz steps (still runs build +
  tests + parser fuzz).
- `SKIP_PRE_PUSH=1 git push` — emergency escape.
- `FUZZ_SECONDS=60 git push` — longer fuzz window.

## Roadmap

See [ROADMAP.md](ROADMAP.md) for the full 12-phase plan. Current
state:

- ✅ Phase 0 — core (connection, RESP3, typed client, routing)
- ✅ Phase 1 — command surface completion
- ✅ Phase 2 — testing rigour + internal audit ([AUDIT.md](AUDIT.md))
- ✅ Phase 3 — CI / CD + coverage + bench + nightly fuzz + docs
- ✅ Phase 4 — documentation (9 guides + CONTRIBUTING + CHANGELOG)
- ✅ Phase 5 — initial 9 examples + standing rule "ship features with their example"
- ✅ Phase 6 — publishing (v0.2.0 live on opam)
- ✅ Phase 7 — Batch primitive (atomic + scatter-gather + WATCH
  guards + cross-slot `pfcount_cluster`) + cluster typed helpers
- ⏳ Phase 8 — client-side caching (`CLIENT TRACKING` + LRU)
- ⏳ Phase 9 — connection pool + blocking pool
- ⏳ Phase 10 — IAM + mTLS + secret-hygiene audit
- ⏳ Phase 11 — module support (valkey-json / -search / -bloom)
- ⏳ Phase 12 — deep audit → 1.0.0 stable

## License

MIT. See [LICENSE](LICENSE).
