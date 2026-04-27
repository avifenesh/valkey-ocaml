# Project status and next steps

**Snapshot taken:** 2026-04-27, branch `main` at commit `6cb6efe` (pushed to origin).

This document captures what's shipped on `main`, what's immediately
runnable, the test posture, and what's queued next. Phase 8
(client-side caching) is now fully shipped — Default, BCAST, and
OPTIN modes work on standalone and cluster. No 0.3.0 release yet;
all of §2 lives unreleased on `main`.

Canonical references this complements — not replaces:
- [README.md](README.md) — user-facing surface.
- [ROADMAP.md](ROADMAP.md) — phased plan, preserves the original planning
  text. Use it for intent, not current state.
- [CHANGELOG.md](CHANGELOG.md) — per-release line of what shipped.
- [docs/client-side-caching.md](docs/client-side-caching.md) — ground truth
  for Phase 8's shape; updated at each step.

---

## 1. Released today

**`valkey.0.2.0`** is live on opam. Shipped with commit `f6d3681`.

What's in 0.2.0, from [CHANGELOG.md](CHANGELOG.md):

- Standalone + cluster client, RESP3-only, OCaml 5.3+/Eio-native.
- Atomic + scatter-gather `Batch` with `WATCH` guards, cross-slot helpers
  (`mget_cluster`, `mset_cluster`, `del_cluster`, `unlink_cluster`,
  `exists_cluster`, `touch_cluster`, `pfcount_cluster`).
- `Transaction` is a thin fa&ccedil;ade over atomic `Batch`.
- Pub/Sub standalone + cluster-aware with failover replay.
- Named commands (user-registered command/transaction templates).
- 240+ tests; parser fuzzer; stability fuzzer; pre-push gate;
  Ubuntu × OCaml {5.3, 5.4} + macOS subset CI; 60% coverage floor;
  nightly 200M-input parser fuzz + 15-min cluster chaos.

## 2. Landed since 0.2.0 (unreleased; on `main`)

### Phase 2.5 — security audit pass (commits `f0e771e..2d281db`)

- **`connection.ml`**: `wrap_with_tls` catch narrowed from catch-all to
  `Tls_eio.Tls_alert | Tls_eio.Tls_failure | End_of_file | Eio.Io _`.
  Non-TLS logic bugs propagate instead of being mislabelled.
- **`connection.ml`**: `Tcp_refused` / `Dns_failed` / `Tls_failed` error
  payloads no longer carry `Printexc.to_string` of the underlying
  exception (which prints paths, cert bytes, etc.). Replaced with a
  classifier that emits stable short kinds (`peer_closed`, `tls_alert`,
  `tls_failure`, `io_error`, errno text via `Unix.error_message`).
- **OpenTelemetry tracing** wired for the bounded operations:
  `valkey.connect` (TCP + TLS + `HELLO`/`SELECT`),
  `valkey.cluster.discover`, `valkey.cluster.refresh`. Outcomes recorded
  as span attributes. With no exporter configured, span ops are near-no-op
  cheap. Per-command spans intentionally not emitted (volume). Redaction
  invariants enforced in `lib/observability.ml` — no auth credentials, no
  command keys, no command values, no server error message bodies. See
  [docs/observability.md](docs/observability.md).
- **Dependency**: `opentelemetry >= 0.90`.

### `Batch.run_atomic` / `run_with_guard` — MOVED/ASK on EXEC = WATCH abort

Discovered while writing the WATCH+topology-change integration
test (formerly deferred): a slot move between WATCH and EXEC was
leaking as `Error (Server_error EXECABORT)` to the caller, who
couldn't distinguish "topology moved, please retry" from "you
sent a malformed transaction." Fixed:

- `queue_all` now classifies its outcome (`All_queued |
  Topology_changed | Transport_error`); a MOVED/ASK during the
  fill-in-MULTI phase short-circuits with `Topology_changed`.
- Both `run_atomic` and `run_with_guard` map `Topology_changed`
  (and a MOVED/ASK on the EXEC frame itself) to `Ok None`,
  mirroring the WATCH-abort path. The connection is dropped so
  the supervisor reconnects against the fresh topology.

Caller's contract for `Ok None` is unchanged: "retry the
read-modify-write loop." Bad-arity / WRONGTYPE EXECABORTs still
flow through the existing per-command-result array path.

### Phase 8 Branch B — Client-side caching (commits `a28f8a3..595b84a`)

Full server-invalidated client-side caching, standalone and cluster, with
single-flight, invalidation-race safety, TTL safety net, metrics, and
BCAST mode. Standalone-pattern inspiration came from lettuce / rueidis /
redis-py; GLIDE's initial CSC (TTL-only, no invalidation) was rejected as
too weak for a serious OCaml client.

Shipped pieces:

| Step | What | Commit |
|------|------|--------|
| 1 | `Cache` primitive (LRU + byte budget) | `a28f8a3` |
| B1 | `CLIENT TRACKING` handshake on (re)connect | `d6639f1` |
| B2.1+B2.2 | Invalidation parser + invalidator fiber | `91fc704` |
| B2.3+B2.4 | Read-path caching on `Client.get` | `61b89af` |
| B3 | Single-flight + invalidation-race safety | `6d4f48c` |
| B6a | `HGETALL` + `SMEMBERS` coverage | `6ba4564` |
| B6b | `MGET` scatter-gather over cache state | `6b19f1a` |
| B7 | Cluster integration + flush invariants | `3ae246d` |
| B8 | Lifecycle tests (reconnect + live failover) | `664187e` |
| B9 | TTL safety net + `Flush_all` race-close | `5f4bfce` |
| B10a | Cache metrics (atomic counters) | `04acdc2` |
| B10b | BCAST mode | `595b84a` |
| B2.5 | OPTIN — pipelined per-read tracking | (this commit) |

Behavioural summary, honest:

- **Cacheable**: `GET`, `HGETALL`, `SMEMBERS`, `MGET`.
- **Not cached by design**: `HGET` / `HMGET` (compound key + prefix-evict
  needed; redis-py shipped it with a field-collision bug — #3612),
  `EXISTS` / `STRLEN` / `TYPE` (low value; covered by the GET-shaped
  entry for the same key in practice).
- **`mode = Default`** is the default and recommended mode for
  standalone and cluster. **`mode = Optin`** is supported on
  both standalone and cluster — the read path pipelines `CLIENT
  CACHING YES + read` as one wire-atomic submit via the new
  internal `Connection.request_pair`, threaded through
  `Router.pair` so the cluster path retries the whole pair on
  the new owner if MOVED arrives on the read frame (frame 1
  stays adjacent to frame 2). The previous `optin : bool` field
  is folded into the `mode` variant so OPTIN/BCAST mutual
  exclusion is encoded in the type.
- Per-shard tracking on cluster happens automatically via the single
  `Client_cache.t` threaded into every shard `Connection.Config`.
- **Flush on topology refresh** and **flush on every per-connection
  reconnect** are both wired — coarse but correct.
- **TTL safety net** off by default; caller sets
  `Client_cache.make ~cache ~entry_ttl_ms:60_000 ()` to opt in.
- **`Flush_all` in-flight race** closed via `Inflight.mark_all_dirty`.
- **Metrics**: `hits`, `misses`, `evicts_budget`, `evicts_ttl`,
  `invalidations`, `puts` via `Client.cache_metrics`.
- **BCAST mode**: `Client_cache.mode = Bcast { prefixes }` activates
  prefix-subscription tracking; server-side rejects overlapping prefixes
  and we surface that as a handshake error (no client-side pre-check yet).

See [docs/client-side-caching.md](docs/client-side-caching.md) for the
full step-by-step.

---

## 3. Test posture

Two targets, both green at the current commit:

### Pure-unit — `dune build @runtest` — **143 tests**

Opam-CI clean. No server dependency. Runs in ~5s.

| Suite | Count | Note |
|-------|------:|------|
| `resp3` + round-trip | 56 | Parser + writer |
| `retry state machine` | small | Cluster retry invariants |
| `byte_reader` | small | Stream reader edge cases |
| `valkey_error` | small | Server-error parse |
| `slot` / `topology` / `discovery` | small | Cluster primitives |
| `redirect` | 5 | MOVED/ASK parser |
| `command_spec` | 20 | Per-command routing |
| `cache` | 22 | LRU/byte budget/TTL/metrics |
| `invalidation parser` | 16 | RESP3 push → `Invalidation.t`; `apply` |
| `inflight` | 12 | Single-flight + dirty-flip |
| `csc optin (pure)` | 4 | `map_optin_pair_reply` arms (outer Error, frame-1 transport error, happy path, non-OK CACHING reply -> Protocol_violation) |

### Integration — `dune exec test/run_tests.exe` — **full suite** against live servers

Requires `docker compose up -d` (standalone at `:6379`) and optionally
`docker compose -f docker-compose.cluster.yml up -d` plus
`bash scripts/cluster-hosts-setup.sh` (cluster at `:7000..:7005`).

CSC-specific slice (37 tests, all green at the current commit):

| File | Count | Scope |
|------|------:|-------|
| `test_csc_tracking.ml` | 2 | B1 handshake + reconnect re-issue |
| `test_csc_invalidation.ml` | 7 | External SET/DEL/FLUSHDB, single-flight (2 & 10 concurrent), metrics |
| `test_csc_hash_set.ml` | 4 | `HGETALL` + `SMEMBERS` populate/evict |
| `test_csc_mget.ml` | 5 | All-miss / all-hit / partial-hit / null-not-cached / evict |
| `test_csc_cluster.ml` | 2 | Two-shard invalidation; cluster-wide `FLUSHDB` |
| `test_csc_lifecycle.ml` | 3 | Standalone reconnect flush; live `CLUSTER FAILOVER FORCE`; TTL expiry without invalidation |
| `test_csc_bcast.ml` | 3 | `TRACKINGINFO` flags; in-prefix evict; out-of-prefix isolation |
| `test_csc_optin.ml` | 6 | Populate-then-hit; external SET evicts; 50-fiber concurrent OPTIN tracking; CACHING-error path; read after `Client.close` returns transport error; tiny `max_queued_bytes` returns `Queue_full` |
| `test_csc_optin_cluster.ml` | 3 | Two-shard populate + cross-shard evict; 25-fiber concurrent OPTIN across all 3 shards; MOVED-retry against wrong-target routing |
| `test_csc_optin_migration.ml` | 2 | Live `CLUSTER SETSLOT MIGRATING/IMPORTING` window; ASK redirect retry single-key smoke; 25-fiber ASK retry under contention |

CSC tests run against live Valkey 9.0.3 standalone **and** a
live 6-node cluster with real primary promotion. OPTIN cluster
support travels through `Router.pair`, with MOVED on the read
frame triggering a redirect-aware retry of the whole pair.
Nothing skipped.

---

## 4. Environment (about to change)

**Current (Windows + WSL):**

- Build: `MSYS_NO_PATHCONV=1 wsl bash /mnt/c/Users/avife/AppData/Local/Temp/build_check.sh`
  (analogous scripts for `@runtest` and the integration target live in `AppData/Local/Temp/`).
- Docker Desktop with the WSL integration enabled.
- `/etc/hosts` in WSL has the cluster hostname mappings
  (`127.0.0.1 valkey-c1 valkey-c2 valkey-c3 valkey-c4 valkey-c5 valkey-c6`)
  so that CLUSTER SHARDS' hostname-endpoints resolve.
- `opam` switch at OCaml 5.3.0, all deps installed.

**Migration targets (Ubuntu):**

- Stock `opam init` + OCaml 5.3, then `opam install . --deps-only` from the repo.
- Docker Engine (not Desktop) via `apt install docker.io docker-compose-plugin`.
- `scripts/cluster-hosts-setup.sh` still applies verbatim — it appends to
  `/etc/hosts` with the exact same `127.0.0.1 valkey-cN` lines.
- Delete `AppData/Local/Temp/*.sh` build-wrapper helpers; no longer needed.

First-run sanity steps on Ubuntu:

1. `opam install . --deps-only`
2. `dune build`
3. `dune build @runtest` — expect 141/141.
4. `docker compose up -d` — starts `ocaml-valkey-dev` on `:6379`.
5. `dune exec test/run_tests.exe -- test 'csc tracking'` — 2 quick tests
   confirming wire-level CSC works end-to-end before anything cluster.
6. `docker compose -f docker-compose.cluster.yml up -d` +
   `bash scripts/cluster-hosts-setup.sh` — if cluster CSC is in scope.
7. `dune exec test/run_tests.exe -- test csc` — full 26-test CSC slice.

---

## 5. Open design questions (noted but not acted on)

These are documented here rather than as stub code or dead TODOs:

1. **Per-key TTL refresh on hit.** Right now a cached entry's TTL
   counts from its last `put`, not its last `get`. Some users expect
   sliding-window TTL; we don't do that. If wanted: add an
   `expires_at` refresh inside `Cache.get`'s hit branch.
2. **BCAST prefix-overlap validation.** The server rejects overlapping
   prefixes and we surface the error; a client-side pre-check would
   give a better error at build time but is not load-bearing.
3. **MGET error semantics when a joiner's owner fails.** If the batched
   MGET fails, every batched-key resolver is resolved with the error
   and the overall result is `Error e` — but the hit group's results
   are discarded. That matches the expected `Client.mget` contract
   (all-or-nothing). If someone wants partial-result semantics we'd
   need a new API.
4. **OOM stress harness.** Deliberately long-running, not worth the
   CI budget until we have a user report.

---

## 6. Next work, ordered by standing value

Note: these are possibilities, not commitments. Re-confirm before
implementing.

### Release and stabilise

- [ ] **Release `valkey.0.3.0`** with everything in §2.
  - CHANGELOG entry summarising Phase 2.5 (shipped) and Phase 8 Branch B.
  - Follow [Phase 6 in ROADMAP.md](ROADMAP.md) for the opam-publishing
    steps. Version bump in `dune-project`.
  - Merge docs/client-side-caching.md into the main docs index.

### Roadmap continuations

- [ ] **Phase 9 — Connection pool.** Opt-in, first-class. ROADMAP has
      the full scope. Per project-memory this is the correct shape:
      pool is opt-in, matches jedis/SE.Redis/GLIDE conventions, not a
      default-hidden thing.
- [ ] **Phase 10 — IAM + mTLS.** mTLS client-cert support in
      `Tls_config` (already partially scaffolded; see the stub note in
      `docs/tls.md`). IAM for AWS ElastiCache-type deployments.
- [ ] **Phase 11 — Module support.** Typed wrappers for `valkey-json`,
      `valkey-search`, `valkey-bloom`. Per-module opam package or a
      single `valkey-modules` meta?
- [ ] **Phase 12 — Full audit pass.** Before 1.0.

### CSC follow-ups (optional)

- [x] **OTel bridge for `cache_metrics`.** Shipped — see
      `Valkey.Observability.observe_cache_metrics`.
- [x] **OPTIN ASK redirect retry.** Shipped — `Connection.request_triple`
      drives `CLIENT CACHING YES + ASKING + read` as one wire-adjacent
      submit; `Cluster_router.make_pair`'s ASK arm wires it. Frame
      ordering is load-bearing — the server's `ASKING` flag is
      consumed by the very next command on the connection regardless
      of what it is, so `ASKING` must sit immediately before the
      slot-keyed read.
- [x] **Slot-migration stress test.** Shipped —
      `test/test_csc_optin_migration.ml` drives a real
      `CLUSTER SETSLOT MIGRATING / IMPORTING` window, plants a key on
      the importing target, and exercises the OPTIN ASK retry. The
      25-fiber concurrent variant proves wire-adjacency under
      contention. Cleanup unconditionally clears migration state via
      `CLUSTER SETSLOT … STABLE` so partial runs can't leave the
      cluster broken.

---

## 7. Quick-reference commit landmarks

- Phase 2.5 start: `6086039` (`connection: stop leaking internals via Printexc.to_string`).
- Phase 2.5 close: `2d281db` (`changelog: tighten Phase 2.5 security audit summary`).
- CSC foundation: `a28f8a3` (`cache: bounded LRU+byte-budget primitive`).
- CSC end-to-end working (single-key GET): `61b89af`.
- CSC race-safe: `6d4f48c`.
- CSC all commands: `6b19f1a`.
- CSC cluster + lifecycle: `3ae246d` and `664187e`.
- CSC BCAST landed: `595b84a`.
- CSC OPTIN standalone: `80563ec` (B2.5).
- CSC OPTIN cluster: `d16e0dd` (Router.pair + redirect-aware retry).
- Cluster-router topology-stale-on-MOVED + WATCH+EXEC fixes:
  `a7e8912`, `9cc5c63`.
- Simplify pass on Phase 8: `6cb6efe` (topology stale-ref bug,
  drop dead `Read_from`, tighten OPTIN target fallback,
  `client_caching_yes` constant, comment trim).

Use `git log --grep='Phase 8'` or `git log --grep='csc:'` to walk the
series.
