# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added — Phase 8: client-side caching (CSC)

Full server-invalidated client-side caching, on standalone **and**
cluster, in all three tracking modes Valkey supports. Configured
via the new `Client_cache.mode` variant — type-level mutual
exclusion replaces the old `optin : bool + mode = Default | Bcast`
shape so OPTIN/BCAST mismatches stop compiling.

- **Bounded LRU cache primitive** (`Valkey.Cache`) — byte budget,
  per-entry overhead accounting, optional TTL safety net,
  atomic-counter metrics (`hits`, `misses`, `evicts_budget`,
  `evicts_ttl`, `invalidations`, `puts`).
- **`CLIENT TRACKING` handshake** on every (re)connect, fail-closed
  if the server rejects the mode (no silent drop to unconfigured
  cache).
- **Invalidation parser + invalidator fiber** — RESP3
  `>2 ["invalidate", [keys...]]` push frames are routed onto a
  dedicated stream so the invalidator drains them without racing
  pubsub consumers.
- **Read-path coverage**: `Client.get`, `Client.mget` (scatter-
  gather over hit/batch/joining groups), `Client.hgetall`,
  `Client.smembers`. `Null` (missing-key) responses are
  intentionally not cached.
- **Single-flight + invalidation-race safety** — `Inflight`
  table dedups concurrent fetches; an invalidation that arrives
  during the fetch flips a dirty flag so the post-fetch put is
  skipped.
- **Cluster integration** — one shared `Cache.t` across every
  shard connection; per-shard tracking happens automatically;
  flush on every per-connection reconnect AND on topology refresh.
- **`mode = Bcast { prefixes }`** — server-side prefix-broadcast
  tracking. `TRACKINGINFO` flag visibility, in-prefix evict, and
  out-of-prefix isolation all verified end-to-end.
- **`mode = Optin`** — pipelined per-read tracking via the new
  internal `Connection.request_pair` (two-frame indivisible
  submit) and `Router.pair` dispatch closure. On cluster, MOVED
  on the read frame triggers a redirect-aware retry that re-
  submits the **whole** pair on the new owner so `CACHING YES`
  stays adjacent to the read across the redirect. ASK on the
  read frame goes through `Connection.request_triple`, which
  sends `[CACHING YES; ASKING; read]` as one wire-adjacent
  submit on the importing primary. Frame ordering is
  load-bearing: `ASKING` is consumed by the very next command
  on the connection regardless of what it is, so it must sit
  immediately before the slot-keyed read; putting `ASKING`
  first would let `CACHING YES` eat the flag and the read would
  bounce as MOVED. Verified against a live `CLUSTER SETSLOT
  MIGRATING/IMPORTING` window in
  `test_csc_optin_migration.ml`. Empirically validated against
  Valkey 9.0.3: the OPTIN flag is consumed by exactly the next
  single command on the wire (so a pipelined `CACHING YES + GET
  k1 + GET k2` tracks `k1` only); a write before the read
  consumes the flag too; MULTI/EXEC counts as one logical
  command for CACHING purposes.

### Fixed — cluster routing under topology change

- **`Batch.run_atomic` / `run_with_guard` no longer leak
  EXECABORT or MOVED on slot-move.** A slot ownership change
  between WATCH/MULTI and EXEC was surfacing to callers as
  `Server_error EXECABORT` (or `MOVED` directly on the EXEC
  frame). Both paths now map to `Ok None` — the same WATCH-abort
  outcome the caller's retry loop already handles. Only
  topology-induced redirects map this way; bad-arity / WRONGTYPE
  EXECABORTs still flow through the per-command-result array
  unchanged.
- **`Cluster_router` MOVED triggers topology refresh + eager
  cache clear when the cached slot owner disagrees.** Previously
  `handle_retries` only fired `trigger_refresh` on unknown
  redirect targets; a MOVED to a known node (the common case
  during failover) silently retried with no refresh, leaving the
  CSC cache stale until the next periodic refresh (15 s). Now
  refreshes whenever the cached topology disagrees with the
  redirect's destination, and `trigger_refresh` synchronously
  clears the CSC cache (closes the window between MOVED and
  async refresh-completion).
- **Pool-race fix on MOVED.** When the redirect target is in the
  topology but not yet in `Node_pool` (race between refresh
  completing and pool-diff applying), `handle_retries` previously
  short-circuited and returned the original error without
  triggering refresh. Now triggers refresh so the next attempt
  finds the connection.
- **Topology stale-ref bug in retry loops.** `topology_ref` was
  only re-synced from `topology_atomic` at the outer call
  boundary; CLUSTERDOWN/TRYAGAIN backoffs (100–1600 ms) could
  let the refresh fiber commit a fresh topology while the retry
  loop kept dispatching against the stale value. Both
  `handle_retries` and `make_pair` now accept an optional
  `?sync_ref` callback that runs before each (re-)dispatch.
- **`handle_retries` ASK arm: atomic `[ASKING; cmd]` submit.**
  Previously sent `ASKING` and the actual command as two
  sequential `Connection.request` calls. The Valkey server
  consumes the one-shot `ASKING` flag from the very next command
  on the connection regardless of which fiber sent it, so under
  concurrent fibers sharing a node connection, another fiber's
  frame could interleave between `ASKING` and the retry on the
  pool's connection — eating the flag and bouncing the actual
  command as MOVED. Both wires are now pipelined through
  `Connection.request_pair` as one indivisible submit. The
  matching CSC OPTIN variant (`make_pair` ASK arm) is fixed in
  the same shape via `Connection.request_triple`. Surfaced by a
  new live-migration concurrent stress test.

### Added — OpenTelemetry tracing

- Library emits OpenTelemetry spans for the bounded operations
  worth knowing latency and failure-mode of: `valkey.connect`
  (TCP + TLS + `HELLO`/`SELECT`, one per (re)connect),
  `valkey.cluster.discover`, `valkey.cluster.refresh`. Outcomes
  (`agreed` / `agreed_fallback` / `no_agreement`) recorded as span
  attributes. With no exporter configured the cost is near-zero.
  Per-command spans intentionally not emitted (~150 k req/s would
  dominate trace volume — apps that need them wrap their own call
  sites). See `docs/observability.md` for setup, attribute schema,
  and the redaction invariants enforced in `lib/observability.ml`
  (no auth credentials, no command keys, no command values, no
  server error message bodies inside spans).
- New dependency: `opentelemetry >= 0.90`.

### Security audit pass (Phase 2.5)

Reviewed against threat model: trusted Valkey cluster (operators
own all members), TLS chain validated when enabled. Three items
fixed in this pass; the rest were either out-of-scope under that
model or features Valkey itself doesn't support (e.g. per-node
SNI). Items addressed:

- **Narrowed `Connection.wrap_with_tls` catch.** Previously any
  exception inside `Tls_eio.client_of_flow` was wrapped as
  `Tls_failed`, masking internal bugs. Now only the actual
  handshake-time exceptions map to that variant
  (`Tls_eio.Tls_alert`, `Tls_eio.Tls_failure`, `End_of_file`,
  `Eio.Io _`); anything else propagates.
- **Non-leaking error payloads.** `Tcp_refused`/`Dns_failed`/
  `Tls_failed` no longer carry `Printexc.to_string` of the
  underlying exception (which prints constructor args, internal
  paths, raw cert bytes). Replaced with a small classifier
  producing stable short kinds (`peer_closed`, `tls_alert`,
  `tls_failure`, `io_error`, errno text via
  `Unix.error_message`). Programmatic handling stays on the
  `Error.t` variant.
- **Observability gap closed** (see "Added" above). Auth events,
  redirect outcomes, and refresh results are now visible in
  traces.

## [0.2.0] — 2026-04-21

### Changed — opam packaging

- Split test suite into pure-unit and integration targets.
  Pure-unit tests (resp3, resp3 round-trip, retry state machine,
  byte_reader, valkey_error, slot, topology, discovery, redirect,
  command_spec) run under `dune build @runtest` and ship green
  through opam CI. Integration suites (everything that talks to a
  live Valkey or cluster) are a plain `executable` stanza now —
  invoke with `dune exec test/run_tests.exe` locally. Fixes the
  opam-repository CI failure on valkey.0.1.0 where @runtest tried
  to contact a server that does not exist in the opam sandbox.

### Added — Batch.pfcount_cluster

- `Valkey.Batch.pfcount_cluster` — HLL union cardinality across
  keys that may live on different cluster slots. Single-slot
  inputs go straight to server-side multi-key `PFCOUNT`.
  Cross-slot inputs are materialised under a hashtag-controlled
  slot via `DUMP` + `RESTORE`, `PFMERGE`d into a destination HLL,
  `PFCOUNT`ed, and cleaned up before returning. Missing input
  keys are treated as empty HLLs (no contribution). Closes the
  long-standing "pfcount_cluster is intentionally missing" note
  in the CHANGELOG.

### Changed — Transaction folded into Batch

- `Valkey.Transaction` is now a thin wrapper over atomic
  {!Batch}: `begin_` with `~watch:` opens a `Batch.guard` (so
  the watched primary's atomic mutex is held across the user's
  code); `queue` appends to a buffered `Batch.t ~atomic:true`;
  `exec` runs the whole block via `Batch.run_with_guard` (or
  `Batch.run` when there's no watch). One primitive, one mental
  model.
- Behaviour shift: bad-arity / unknown-command errors surface
  inside the per-command replies returned by `exec` (as
  `Resp3.Simple_error _`) or via EXECABORT, not at `queue`
  time. Fan-out commands are still rejected at `queue` with a
  `Terminal` error.
- Concurrent transactions on the same primary now serialise at
  `exec` time (via the router's per-primary atomic mutex). Local
  `queue` calls on different handles never contend. Non-atomic
  traffic continues to multiplex as before.
- `Batch.validate_same_slot` now compares *connections*, not
  slot numbers: standalone (one conn for every slot) and
  co-located keys within one primary both pass through, only
  true cross-primary batches fail CROSSSLOT.

### Added — Batch.watch (read-modify-write CAS)

- `Valkey.Batch.with_watch` — scoped WATCH guard for the classic
  optimistic-concurrency pattern. Sends `WATCH` immediately, holds
  the watched primary's atomic mutex across the closure, and
  guarantees `UNWATCH` + mutex release on any exit (commit,
  abort, or exception).
- `Valkey.Batch.watch` / `run_with_guard` / `release_guard` —
  lower-level pieces for callers that need explicit lifetime
  control. `run_with_guard` issues `MULTI`/queued/`EXEC` on the
  guard's connection, returning `Ok (Some _)` on commit,
  `Ok None` if a watched key was modified, `Error _` on
  transport failure. Empty batch under guard sends `UNWATCH` and
  returns `Ok (Some [||])` — clean abort path for "no write
  needed" decisions.
- Watched keys must hash to one slot (client-side CROSSSLOT
  validation before any I/O). Pass `~hint_key` to override.
- Closes the long-standing limitation noted in the previous entry
  ("`Batch.create ~watch:` is effectively useless"). Legacy
  `~watch:` on `Batch.create` still exists but is documented as
  paranoia padding; real CAS goes through the guard API.

### Added — Phase 7 (batch + cluster-aware commands)

- `Valkey.Batch` — scatter-gather batch primitive. One module,
  two modes selected by `~atomic:bool` at `create` time:
  - **Non-atomic** (default): commands bucketed by slot, each
    slot's bucket runs as a parallel pipeline on that slot's
    connection, results merged back into input order. Per-command
    results; partial success is the norm.
  - **Atomic** (`~atomic:true`): all keys must hash to one slot
    (client-side CROSSSLOT validation). Sends a single
    `WATCH` / `MULTI` / commands / `EXEC` burst on the slot's
    primary. Returns `Ok (Some results)` on commit, `Ok None` on
    WATCH abort.
- Per-entry result variant:
  - `One of (Resp3.t, Error.t) result` — single-target commands.
  - `Many of (node_id * result) list` — fan-out commands
    (SCRIPT LOAD, FLUSHALL, CLUSTER NODES, …), matching
    `Client.exec_multi`.
- Fan-out commands are rejected at `Batch.queue` time in atomic
  mode (`Batch.Fan_out_in_atomic_batch` structured error). They
  route through `exec_multi` in non-atomic mode.
- Wall-clock `?timeout` applies to the whole batch; commands that
  don't complete in the window come back as `One (Error Timeout)`;
  completed ones keep their real reply. Typed helpers collapse
  any timeout into a whole-call error.
- Typed cluster helpers under `Valkey.Batch`:
  - `mget_cluster` — `(key, value option) list` in input order.
  - `mset_cluster` — per-slot atomic, cross-slot interleaved.
  - `del_cluster` / `unlink_cluster` — sum of per-slot removal counts.
  - `exists_cluster` — sum of existence hits (duplicates count
    separately, matching server semantics).
  - `touch_cluster` — sum of keys whose last-access was bumped.
- `examples/10-batch/` — three runnable programs: `bulk.ml`
  (1000-key mset/mget/del + perf comparison with per-key loop),
  `scatter.ml` (heterogeneous non-atomic batch), `atomic_counters.ml`
  (SET NX + INCR + INCR + GET pinned via hashtag in atomic mode).
- `docs/batch.md` — concept, atomic vs non-atomic semantics,
  timeout, ordering, typed helpers, WATCH caveat.

### Changed — router

- `Router.t` gained `atomic_lock_for_slot : int -> Eio.Mutex.t`.
  Serialises concurrent atomic operations (both
  `Batch ~atomic:true` and `Transaction`) on the same primary
  connection so their MULTI/EXEC blocks no longer interleave.
  Non-atomic traffic bypasses the lock and continues to
  multiplex at full speed. Per-primary (not per-slot): ops on
  different primaries run in parallel; ops on slots sharing a
  primary queue behind each other. Standalone uses a single
  mutex across all slots.
- `Transaction.begin_` now acquires the mutex before
  `WATCH`/`MULTI`; `exec` / `discard` release it. Leaking a
  transaction without `exec`/`discard` now also leaks the lock
  — same failure-mode class as the pre-existing leak of
  server-side MULTI state, so no user-visible regression for
  correctly-written callers.

## [0.1.0] — 2026-04-20

First public release. The repo's been incubating for a few weeks
across Phases 0–5; this tag freezes a working surface and ships it
to opam under the `0.x` umbrella (API may evolve through `0.x`;
`1.0` waits for the deeper audit in ROADMAP Phase 12).

Highlights of what's in 0.1.0 (full per-phase detail below):

- Connection layer: auto-reconnect, byte-budget backpressure,
  circuit breaker, keepalive, TLS, optional cross-domain split.
- RESP3 parser + writer; all 14 wire types.
- ~140 typed commands across strings / counters / TTL / hashes
  (incl. field TTL on Valkey 9+) / sets / lists / sorted sets /
  streams (incl. consumer groups + XAUTOCLAIM) / scripting /
  Functions / pub/sub / blocking / bitmap / HLL / geo / generic
  keyspace / CLIENT admin / CLUSTER introspection / LATENCY /
  MEMORY.
- Cluster router: quorum-based topology discovery, periodic
  refresh, MOVED/ASK/CLUSTERDOWN/TRYAGAIN/Interrupted retry,
  Read_from with 3-tier AZ-affinity fallback, fan-out helpers.
- Standalone uses the same router behind a synthetic single-shard
  topology.
- Transaction (MULTI/EXEC/WATCH with hint_key slot pinning).
- Pub/sub: standalone with auto-resubscribe; cluster-aware with
  failover-watchdog re-pinning.
- 9 runnable examples under `examples/`.
- 9 hand-written guides under `docs/` + odoc HTML.
- 5 GitHub Actions workflows: ci, coverage (60% floor), bench
  (10% regression gate), nightly fuzz, docs (gh-pages).
- 221 tests; 10M strict parser-fuzz iterations; bin/soak/ for
  long-running stability with GC + fd slope detection.

### Added — typed sorted-set wrappers

The leaderboard example surfaced this gap; the wrappers below
replace the `Client.custom` calls that example was using.
**docs-first audit done against valkey.io for every command** —
each `.mli` quotes the upstream syntax + version + reply shape.

- `zadd` — `?mode:zadd_mode` is a single sum encoding every
  server-permitted combination of NX/XX/GT/LT (server rejects
  NX+GT, NX+LT, NX+XX, GT+LT — those are now type-impossible).
  Six legal modes: `Z_only_add`, `Z_only_update`,
  `Z_only_update_if_greater`, `Z_only_update_if_less`,
  `Z_add_or_update_if_greater`, `Z_add_or_update_if_less`.
  `?ch` toggles the CH modifier.
- `zadd_incr` — separate function so the return type tracks the
  reply. Single score-member pair enforced by signature (server
  rejects multiple under INCR). Returns `float option`: the new
  score on success, `None` when `~mode` prevented the write.
- Score formatting uses `Printf.sprintf "%.17g"` for full
  IEEE-754 round-trip precision (was `%g`, which truncated to 6
  significant digits and silently lost data on real scores).
- `zincrby` — atomic increment, returns the new score as `float`.
- `zrem` — returns count of members actually removed.
- `zrank` / `zrevrank` — `int option`, `None` when missing.
- `zrank_with_score` / `zrevrank_with_score` — Valkey 7.2+
  WITHSCORE variant, returns `(int * float) option`.
- `zscore` — `float option`, `None` when missing.
- `zmscore` — multi-member; returns `float option list` parallel
  to the input list.
- `zcount` — count members in a score range.
- `zrange_with_scores` / `zrangebyscore_with_scores` — return
  `(string * float) list` instead of just members.
- `zpopmin` / `zpopmax` — atomic pop with optional count;
  returns `(string * float) list`.

Score parsing handles both RESP3 `Double` and `Bulk_string`
shapes (different commands and server versions emit different
encodings).

`examples/09-leaderboard/` is now typed end-to-end (no
`Client.custom` left).

### Added — Phase 5 (examples)

- Initial set of 9 runnable example programs under `examples/`:
  01-hello (strings/counters/hashes/streams/consumer groups),
  02-cluster (Read_from modes + TLS template), 03-pubsub,
  04-transaction (WATCH retry), 05-cache-aside (hash field TTL),
  06-distributed-lock (SET NX EX + CAD release), 07-task-queue
  (streams + consumer groups + XAUTOCLAIM), 08-blocking-commands
  (BRPOP), 09-leaderboard (sorted-set).
- `examples/README.md` documents the set and the convention.
- `CONTRIBUTING.md` updated: new significant features land with
  the example that exercises them.

### Changed

- `Cluster_router.pick_node_by_read_from` now picks a random
  replica for `Prefer_replica` and `Az_affinity` modes (was
  always the first replica, which pinned all reads from one
  client to a single replica). Spreads readonly traffic across
  the replica set.

### Known gaps surfaced by examples

- Sorted-set commands `ZADD`, `ZINCRBY`, `ZRANK`, `ZSCORE`,
  `ZREVRANGE` are not yet typed wrappers — the leaderboard
  example uses `Client.custom` for them. Wrapping them is on the
  pre-1.0 list.

### Added — Phase 4 (documentation)

- `docs/` — 9 hand-written guides covering getting-started, cluster,
  transactions, pub/sub, TLS, performance, troubleshooting,
  security, and migration from `ocaml-redis`.
- `CONTRIBUTING.md` at repo root — build/test/fuzz/bench/coverage
  workflow, style rules, PR checklist.
- `CHANGELOG.md` rewritten to explicitly cover Phases 0 → 3 (was
  stale, reported 82 tests).
- `.github/workflows/docs.yml` — builds odoc HTML on every push/PR,
  stages guides under `/guides/`, deploys to `gh-pages` on main.
- `dune build @doc` is warning-clean on all 20 modules; the
  previously-hidden constructor warning on `Connection.Error.t`
  was resolved during this pass.

### Added — Phase 3 (CI/CD + coverage)

- GitHub Actions workflows:
  - `ci.yml` — Ubuntu × OCaml {5.3, 5.4} integration (docker
    standalone + cluster, full tests, 100k parser fuzz strict, 30s
    standalone + 30s cluster stability fuzz). macOS × OCaml {5.3,
    5.4} portability subset (docker-free tests + 50k parser fuzz).
  - `coverage.yml` — bisect_ppx instrumentation, HTML artifact, 60
    % floor (baseline 63 %), gh-pages deploy of the report on main.
  - `fuzz-nightly.yml` — scheduled 02:00 UTC. 200M parser fuzz
    strict + 15 min cluster stability with docker-restart chaos.
    Auto-opens an issue on non-zero exit.
  - `bench.yml` — per-PR delta table vs `main` with a 10%
    regression gate; pushes to `main` stash the baseline on the
    `bench-history` branch.
- `bin/bench_compare/` — zero-dep bench-JSON diff tool producing
  GitHub-flavoured markdown tables.
- `bin/bench/` gained `--json PATH` output.
- `bisect_ppx` is a new `with-dev-setup` dependency; `lib/dune`
  declares it as the instrumentation backend.

### Added — Phase 2 (testing rigour + audit)

- `test/test_resp3_roundtrip.ml` — randomised round-trip proptest,
  10k random leaves + 10k nested trees + targeted edge cases
  (empty aggregates, bulk-with-CRLF, int64 extremes, inf/NaN,
  exotic map keys). Hand-rolled generator, no new dep.
- `test/test_command_spec_property.ml` — three cluster-level
  properties: 500 random-keyed round-trips, every-slot endpoint
  coverage, Read_from.Prefer_replica actually reaches replicas.
- `test/test_retry_state.ml` — nine focused tests over the retry
  state machine via `Cluster_router.For_testing`: ok /
  non-retryable / TRYAGAIN / CLUSTERDOWN (×2, incl. exponential
  schedule + budget exhaustion) / Interrupted / Closed / mixed /
  backoff schedule.
- Parser fuzzer upgrades: tree-level structural mutation (swap /
  duplicate / reverse sublist / recursively mutate), length-field
  poisoning on every declared aggregate/bulk header, and a
  delta-debug shrinker that prints the minimal reproducer on
  failure. 10 M strict clean at ~145k inputs/s on the new
  six-strategy mix.
- `bin/soak/` — long-running stability soak. Steady SET/GET/DEL
  workload with a sampler recording `Gc.quick_stat` heap + top +
  live, `/proc/self/fd` count, total ops every N seconds. OLS
  slope detection flags heap or fd leaks; `--strict` exits 1 on
  threshold breach.
- `docker-compose.toxiproxy.yml` + `scripts/chaos/chaos.sh` — TCP
  chaos via toxiproxy. Subcommands: setup / latency / loss /
  bandwidth / reset / close / clear / teardown. Point bin/fuzz,
  bin/soak, or bin/bench at the proxy ports for chaos runs.
- `AUDIT.md` — inventory of every `Obj.magic`, `try _ with _ -> ()`,
  `ignore (_ : _ result)`, `mutable` field (+ lock discipline),
  and `Atomic.*` site, each with a disposition.
- `Cluster_router.For_testing` — exposes `handle_retries` and the
  backoff constants so the retry loop can be driven from unit
  tests without a real pool.

### Changed — Phase 2

- Deleted the dead `watchdog` function in `cluster_pubsub.ml` —
  it contained an `Obj.magic ()` landmine (never executed, but
  dangerous to leave around); the correct implementation was
  already inlined in `create`.
- `Connection.close` and `Cluster_router.close` now guard with
  `Atomic.exchange closing true`, making repeat calls a true no-op
  instead of relying on each inner step being re-invokable.
- Added a locking-discipline comment at the top of
  `cluster_pubsub.ml` documenting the `shards_mutex` /
  `subs_mutex` contract.
- Narrowed all 14 drain-path `try ... with _ -> ()` sites to the
  specific exceptions that legitimately arise on teardown
  (`Eio.Io _ | End_of_file | Invalid_argument _ | Unix.Unix_error
  _` for close paths; `Invalid_argument _` for
  `Eio.Promise.resolve`). Anything else now surfaces instead of
  being silently swallowed.

### Added — Phase 1 (command surface completion)

- **Bitmap**: `BITCOUNT`, `BITPOS`, `BITOP`, `SETBIT`, `GETBIT`,
  `BITFIELD`, `BITFIELD_RO`. `bit_range` is a sum type (`From`,
  `From_to`, `From_to_unit`) matching Valkey 8.0+ semantics, and
  `BITOP NOT` is encoded with arity-at-type-level
  (`Bitop_not of string` takes exactly one source).
- **HyperLogLog**: `PFADD`, `PFCOUNT`, `PFMERGE`.
- **Generic keyspace**: `COPY`, `DUMP`, `RESTORE`, `TOUCH`,
  `RANDOMKEY`, `OBJECT ENCODING|REFCOUNT|IDLETIME|FREQ`.
- **Geo**: `GEOADD`, `GEODIST`, `GEOPOS`, `GEOHASH`, `GEOSEARCH`,
  `GEOSEARCHSTORE` with typed `geo_from`, `geo_shape`,
  `geo_search_result`.
- **CLIENT admin**: `CLIENT ID|GETNAME|SETNAME|INFO|LIST|PAUSE|
  UNPAUSE|NO-EVICT|NO-TOUCH|KILL|TRACKING`, with closed-sum
  filters (`client_kill_filter`) and typed tracking options
  (`client_tracking_on` + REDIRECT / PREFIX / BCAST / OPTIN /
  OPTOUT / NOLOOP).
- **Functions + FCALL**: `FUNCTION LOAD|DELETE|FLUSH|LIST`,
  `FCALL`, `FCALL_RO` (fan-out to every primary for LOAD-class
  commands via `fan_primaries_unanimous`).
- **Cluster introspection**: `CLUSTER KEYSLOT`, `CLUSTER INFO`.
- **Observability**: `LATENCY DOCTOR|RESET`, `MEMORY USAGE`,
  `MEMORY PURGE`.
- `Named_commands` — register command and transaction templates
  with `$N` placeholders, run by name later. Thread-safe; shares
  the same routing as `Client.custom`.
- `Pubsub` — standalone client-level pub/sub with auto-resubscribe
  after reconnect via `Connection.on_connected`.
- `Cluster_pubsub` — regular + sharded pub/sub on one handle, with
  a watchdog fiber re-pinning sharded connections on failover
  (integration test forces all 3 primaries to restart and asserts
  delivery resumes).
- `Transaction` — `MULTI`/`EXEC`/`WATCH`/`DISCARD` with a
  `hint_key` to pin to a slot, and a `with_transaction` scope
  helper.
- `Command_spec` — ~230 entries covering all typed wrappers, plus
  a test that cross-checks every entry against live `COMMAND INFO`
  metadata.
- Send-path optimisation — new `Resp3_writer.command_to_cstruct`
  produces a single allocation of the exact wire size with one
  blit per argument. SET 16 KiB went from 47 % to 91 % of the C
  reference.
- Parser hardening — `Resp3_parser` now rejects negative bulk
  and aggregate lengths (regression found by the parser fuzzer on
  its first run).

### Added — Phase 0 (core)

- Connection layer: auto-reconnect with jittered backoff,
  byte-budget backpressure, circuit breaker (always-on generous
  default), app-level keepalive fiber, TLS (self-signed + system
  CAs), optional cross-domain split (`?domain_mgr`) moving socket
  I/O to a dedicated OS thread.
- RESP3 parser + writer covering all 14 wire types. Streamed
  aggregates raise explicitly (not silently mis-decoded).
- Client layer: abstract `Client.t` with typed commands covering
  strings, counters, TTL, hashes, hash field TTL (Valkey 9+),
  sets, lists, sorted sets, scripting with `Script.t` and
  transparent `NOSCRIPT` fallback, iteration, streams
  (non-blocking + consumer groups), blocking commands.
- Typed variants for every wire-level keyword set (`set_cond`,
  `set_ttl`, `hexpire_cond`, `hgetex_ttl`, `hsetex_ttl`,
  `score_bound`, `value_type`, …) and every per-field status code
  (`field_ttl_set`, `field_persist`, `expiry_state`).
- Routing interface (`Read_from`, `Target`) — surfaces the API
  shape the cluster router plugs into without changing callers.
