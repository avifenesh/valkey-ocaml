# Roadmap

Elaborated, phased plan toward a public `valkey` package and beyond.
Each phase is a checkpoint: a self-contained chunk of work with its
own success criteria. Re-assessed after every phase.

Current tree snapshot: **241 tests passing; Phases 0-7 shipped;
`valkey.0.2.0` live on opam** (standalone, cluster, transactions,
batch incl. `with_watch` + cross-slot `pfcount_cluster`, pub/sub
standalone + cluster-aware with failover replay, named commands,
stability fuzzer, parser fuzzer, pre-push gate). The phase bodies
below preserve the original planning text; when the shipped shape
changed later, the `Shipped.` paragraphs are the current source of
truth. See [README.md](README.md) for the user-facing surface.

---

## Phase 1 — Command surface completion ✅ done

**Shipped.** Six batches landed at commits `63dde75..d450897`:
bitmap, HLL + generic keyspace, geo, CLIENT admin, FUNCTION + FCALL
+ CLUSTER introspection + LATENCY + MEMORY, COMMAND INFO
spec-validation test. 191 tests (+48).


**Goal.** Every Valkey 7.2–9.x command callable with correct cluster
routing and (for the common ones) a typed wrapper that doesn't leak
`Resp3.t`. The library stops being a "core + escape hatch"; it
becomes "Valkey-complete."

**Scope.**

- Enumerate the gap between the `Command_spec` table and the
  Valkey 9.x command reference. Two categories:
  1. Commands in the spec but not typed (user falls back to
     `Client.custom`). Add typed wrappers sorted by usage
     frequency from GLIDE fleet data and valkey.io docs.
  2. Commands not in the spec at all — add entries with the correct
     route type (`Single_key` / `Multi_key` / `Keyless_random` /
     `Fan_primaries` / `Fan_all_nodes`).
- Re-validate every `Single_key` entry against `COMMAND INFO`'s
  `firstkey` / `lastkey` metadata from a live Valkey 9 server. If
  they disagree, the spec is wrong and will produce silent MOVED
  churn in cluster.
- Typed wrappers to add (non-exhaustive, ordered by impact):
  - **Bitmap**: `BITCOUNT`, `BITPOS`, `BITOP`, `BITFIELD`, `BITFIELD_RO`
  - **Geo**: `GEOADD` / `GEOSEARCH` / `GEOSEARCHSTORE` / `GEORADIUS_RO` / `GEOPOS` / `GEODIST`
  - **HyperLogLog**: `PFADD` / `PFCOUNT` / `PFMERGE`
  - **Generic**: `COPY`, `DUMP`, `RESTORE`, `OBJECT *`, `TOUCH`, `RANDOMKEY`, `UNLINK`
  - **Server admin**: `DBSIZE`, `LATENCY *`, `MEMORY USAGE`, `INFO` (with sections)
  - **CLIENT**: `CLIENT TRACKING`, `CLIENT NO-EVICT`, `CLIENT PAUSE`, `CLIENT INFO`, `CLIENT KILL` (typed filter args)
  - **FUNCTION**: `FUNCTION LOAD`, `FUNCTION DELETE`, `FUNCTION LIST`, `FCALL`, `FCALL_RO`
  - **Cluster introspection**: `CLUSTER COUNT-FAILURE-REPORTS`, `CLUSTER LINKS`, `CLUSTER MYID`, `CLUSTER RESET`
- Document how to extend: a cookbook entry showing add-a-command in
  ~20 lines (mli + ml + spec + test).

**Deliverables.**

- Typed wrappers for ≥95 % of the commands a real app uses (GLIDE
  frequency list as the benchmark).
- Routing table in the ~230 command / sub-command range, with the
  covered surface validated against live `COMMAND INFO`.
- One test per newly typed command (smoke: arg-building + a success
  reply + a known error reply).

**Success criteria.**

- `grep -c Client.custom` in example programs is near zero.
- `COMMAND INFO <cmd>` for every entry in `Command_spec` matches
  our `key_index` / `first_key_index`.
- Live cluster integration test: pick 100 random command + keyset
  combinations, assert no MOVED on first dispatch.

**Depends on.** Nothing — pure surface work.

---

## Phase 2 — Testing rigour + internal audit ✅ done

**Shipped.** Seven commits (round-trip proptest, Command_spec
property tests over live cluster, retry state-machine tests via
`Cluster_router.For_testing`, parser fuzzer mutation + shrinker at
10 M strict clean, `bin/soak/`, toxiproxy TCP chaos, audit pass
with fixes + narrowed exception handlers). 211 tests. Audit +
deferred items catalogued in [AUDIT.md](AUDIT.md).


**Goal.** Everything the existing tests assert remains true under
hostile inputs, weird key distributions, and long running. No
`Obj.magic`. No silently swallowed exceptions. Every invariant
that the retry state machine depends on has a test.

**Scope.**

- **Property-based tests over `Command_spec`.**
  Random-generate command + keyset tuples, dispatch against a live
  cluster, assert:
  1. No MOVED on first send.
  2. Writes land on the primary (via `Read_from.Primary` forced).
  3. Readonly commands with `Prefer_replica` actually hit a replica
     when one is available.
- **RESP3 round-trip property test.**
  `∀ v in Resp3.t, parse (encode v) = v`. The parser fuzzer already
  has an encoder; formalise it with qcheck. Catches encoder drift.
- **Retry state-machine tests.**
  Inject synthetic errors (mock server or fault-injected socket)
  for every branch of `handle_retries`: MOVED / ASK / CLUSTERDOWN /
  TRYAGAIN / Interrupted / Closed. Assert:
  - Bounded retry count (never more than `max_redirects`).
  - Correct backoff schedule (check timing within tolerance).
  - `trigger_refresh` fires exactly when the spec says.
- **Full audit pass.** Walk every module top-to-bottom and flag:
  - Every `Obj.magic`. There's one in `Cluster_pubsub` that's
    documented; justify or eliminate the rest (I don't think there
    are others but audit anyway).
  - Every `try _ with _ -> ()` — does the swallowed exception hide
    a real bug? Categorise: "drain path, safe" vs "fix this".
  - Every `ignore (... : _ result)` — same question.
  - Every `mutable` field and its lock/atomic discipline.
  - Every `Atomic.*` — memory ordering correct?
- **Soak test infrastructure.**
  `bin/soak/` that runs the stability fuzzer for 6–12 h, samples
  `Gc.stat` + fd count + fiber count every minute, asserts no
  unbounded growth. CI-friendly (smaller window) + local
  long-window variants.
- **Chaos mix-in.**
  Extend `bin/fuzz/` with a `--chaos-tcp` option that uses
  [toxiproxy](https://github.com/Shopify/toxiproxy) or `tc netem`
  to inject packet loss, latency, bandwidth caps, half-closed
  sockets. Exposes race conditions the clean-restart chaos can't.
- **Parser fuzzer upgrades.**
  - Add mutation-based inputs: take valid messages, flip bits,
    duplicate sub-sections, reorder array elements.
  - Structured shrinking: when the fuzzer finds a failing input,
    report the minimal input that reproduces it.

**Deliverables.**

- New `test/test_command_spec_property.ml`, `test/test_resp3_roundtrip.ml`,
  `test/test_retry_state.ml`.
- `bin/soak/` soak-test binary.
- `AUDIT.md` listing every flagged pattern in the codebase and its
  disposition.
- Parser fuzzer runs strict clean at 10 M iterations with mutations.

**Success criteria.**

- Zero `Obj.magic` without a written justification.
- Zero swallowed exception without a comment explaining why.
- 12-hour soak shows flat memory, flat fd count, flat fibre count
  (noise floor only).
- Parser fuzzer CI run at 10 M inputs green.
- Retry state machine: every branch proved to terminate and not
  exceed `max_redirects`.

**Depends on.** Phase 1 (need the full command surface to
property-test it).

---

## Phase 3 — CI / CD + test coverage ✅ done

**Shipped.** Five commits landing `ci.yml` (Ubuntu × 5.3/5.4 full
integration + macOS × 5.3/5.4 portability), `coverage.yml` (bisect_ppx
instrumentation, 60 % floor, gh-pages deploy), `fuzz-nightly.yml`
(200 M parser + 15 min cluster chaos + auto-issue), `bench.yml`
(per-PR delta + 10 % regression gate + baseline to `bench-history`),
and `bin/bench_compare/`.


**Goal.** Every push is validated on every supported OCaml version
and OS before a human reads the PR. Coverage is measured and
enforced. Benchmarks are tracked over time. Fuzzing runs nightly.

**Scope.**

- **GitHub Actions workflows:**
  - `ci.yml` — runs on every push + PR:
    - Matrix: OCaml 5.3 × 5.4, Ubuntu-latest × macOS-latest.
    - Start docker compose (standalone + cluster).
    - `dune build`, `dune runtest`, parser fuzz at 100 k, standalone
      stability fuzz 30 s, cluster stability fuzz 30 s.
    - Upload test + fuzz logs as artifacts.
  - `coverage.yml` — on main pushes:
    - Run tests under `bisect_ppx`, generate HTML report, upload to
      GitHub Pages under `/coverage/`.
    - Fail the build if coverage drops below the committed floor
      (start at whatever it is today, ratchet up).
  - `bench.yml` — on main pushes and release tags:
    - Run the bench harness (shortened ops count for CI),
      post-process into a JSON record, commit to a `bench-history`
      branch (`.github/bench-results.json`).
    - Comment on the PR with the delta vs main.
    - Fail if any scenario regressed > 10 %.
  - `fuzz-nightly.yml` — scheduled 02:00 UTC:
    - Parser fuzzer for 30 min with fresh seeds.
    - Stability fuzzer against a spun-up cluster for 15 min with
      docker-restart chaos.
    - Upload any failing seeds or stack traces as artifacts; open
      an issue automatically if red.
- **Coverage instrumentation:**
  - Add `bisect_ppx` as a dev dependency.
  - `dune runtest --instrument-with bisect_ppx` target.
  - Coverage report via `bisect-ppx-report html`.
  - CI enforces `bisect-ppx-report summary --fail-below N`.
- **Bench result storage:**
  - A simple JSON schema per run: `{ commit, scenarios : [ { name,
    ops, median_ms, p99_ms } ] }`.
  - A small OCaml tool (`bin/bench_compare/`) that diffs two
    result files and outputs markdown.

**Deliverables.**

- `.github/workflows/ci.yml`, `coverage.yml`, `bench.yml`, `fuzz-nightly.yml`.
- `bin/bench_compare/` that produces PR comments.
- Coverage report published from CI (artifact + gh-pages deploy).
- Bench baselines published to the `bench-history` branch.

**Success criteria.**

- Every PR gets automated: build, tests, parser fuzz, coverage
  delta, bench delta posted as a comment in < 10 minutes.
- Nightly fuzz catches any regression within 24 hours.
- Coverage stays above the committed floor (target ≥ 80 %).

**Depends on.** Phase 2 (need the new test suites to include them).

---

## Phase 4 — Documentation ✅ done

**Shipped.** One big commit: 9 hand-written guides under `docs/`
(getting-started, cluster, transactions, pubsub, tls, performance,
troubleshooting, security, migration-from-ocaml-redis),
`CONTRIBUTING.md`, a rewritten `CHANGELOG.md` covering Phase 0-3,
and `docs.yml` building odoc + staging guides under `/guides/` on
gh-pages. The current tree still builds with `dune build @doc`,
but it now emits one odoc warning about hidden constructors in
`Valkey.Connection.Error.t`, so the older "warning-clean"
statement no longer holds.


**Goal.** A new user can go from `opam install valkey` to running
a cluster-aware app in 15 minutes, without reading the source.

**Scope.**

- **API reference** via odoc. One-time setup:
  - Annotate every `.mli` with enough examples in doc-comments.
  - `dune build @doc` produces HTML.
  - `gh-pages.yml` workflow publishes to the `ocaml-valkey.github.io`
    org or a `/docs` subtree on the main repo.
- **`docs/` subtree** with hand-written guides:
  - `getting-started.md` — install + first program.
  - `cluster.md` — how to point at a cluster, what happens under
    the hood, failover semantics.
  - `transactions.md` — MULTI/EXEC + WATCH patterns, CROSSSLOT
    handling, idempotency advice.
  - `pubsub.md` — regular vs sharded, auto-resubscribe, delivery
    guarantees, dead-letter patterns.
  - `tls.md` — self-signed + system CA, ElastiCache, mTLS (once shipped).
  - `performance.md` — concurrency model, connection-per-Client
    vs pool, benchmark methodology, observed ratios vs C/
    ocaml-redis.
  - `troubleshooting.md` — common errors by code, what they mean,
    what to do.
  - `security.md` — secrets handling, TLS, ACL, IAM (once shipped),
    audit of what goes on the wire.
  - `migration-from-ocaml-redis.md` — side-by-side of the common
    idioms, where the APIs diverge.
- **`CONTRIBUTING.md`** at repo root — build, test, fuzz, bench,
  pre-push gate, style conventions, commit-message format.
- **`CHANGELOG.md`** — start keeping it now; every user-visible
  change gets an entry before the commit lands.

**Deliverables.**

- odoc HTML hosted at a stable URL.
- All 9 `docs/*.md` guides above.
- CONTRIBUTING + CHANGELOG at the repo root.
- Inline doc-comment coverage ≥ 95 % for public API (measurable
  via odoc warnings).

**Success criteria.**

- A colleague who has never touched the library can write a
  working cluster + pub/sub demo using only the docs, in ≤ 30 min.
- No "undocumented" public symbol per odoc warnings.

**Depends on.** Phases 1–3 (docs reflect the final surface).

---

## Phase 5 — Examples ✅ done (and ongoing)

**Shipped.** Initial set of 9 runnable example programs landed
under `examples/`. Going forward, new significant features land
together with the example that exercises them — see
[CONTRIBUTING.md](CONTRIBUTING.md). Phase 5 is converted from a
one-shot batch into a standing rule.

| # | Directory | Demonstrates |
|---|---|---|
| 01 | `01-hello/` | strings, counters, hashes, hash field TTL, streams, consumer groups |
| 02 | `02-cluster/` | all four Read_from modes; TLS template (managed + internal CA) |
| 03 | `03-pubsub/` | publisher + 2 subscribers (channel + pattern), graceful shutdown |
| 04 | `04-transaction/` | WATCH retry race on a balance |
| 05 | `05-cache-aside/` | read-through with hash field TTL (Valkey 9+) |
| 06 | `06-distributed-lock/` | SET NX EX + CAD release; not Redlock |
| 07 | `07-task-queue/` | streams + consumer groups + XAUTOCLAIM reclaim |
| 08 | `08-blocking-commands/` | BRPOP worker on a dedicated client |
| 09 | `09-leaderboard/` | sorted-set leaderboard using typed `ZADD` / `ZINCRBY` / `ZRANGE*` helpers |

While building these, two follow-ons fell out:
  - Random replica selection inside `pick_node_by_read_from`
    (was first-only, now properly randomised).
  - The sorted-set wrapper batch landed later in `0.2`, so the
    leaderboard example no longer needs `Client.custom`.



**Goal.** Runnable, real-world-shaped programs in `examples/`. Each
demonstrates one use case end-to-end, is small enough to read in
one sitting, and ties into one or more docs guides.

**Scope.** One subdir per example, each with its own `dune` +
README:

- `examples/01-hello/` — connect, SET, GET, close. The smallest
  possible intro.
- `examples/02-cluster/` — connect to a 3-primary cluster, run a
  loop of SETs across a broad keyspace, print slot distribution.
  Links to `docs/cluster.md`.
- `examples/03-stream-worker/` — `XREADGROUP BLOCK` worker with
  auto-XACK + XCLAIM of stuck entries. Shipping-style harness.
  (Requires Phase 7's stream consumer helper, or do it hand-rolled
  first.)
- `examples/04-pubsub-fanout/` — 1 publisher + N subscribers on
  both regular and sharded channels. Shows `Cluster_pubsub` survive
  a forced primary restart.
- `examples/05-transaction-retry/` — WATCH-based optimistic concurrency
  pattern with a retry loop.
- `examples/06-otel/` — wire the library to `ocaml-opentelemetry`,
  ship spans to a local collector. (Requires Phase where we add the
  metrics hooks — can stage.)
- `examples/07-cache-aside/` — read-through cache layer using our
  client. A realistic pattern for users who came for performance.
- `examples/08-redlock/` — distributed lock demo. (Requires the
  primitive to exist.)

Every example has a `README.md` explaining what it shows, how to
run it, and what to observe.

**Deliverables.**

- 9 initial runnable examples, each buildable with `dune build examples/XX`.
- `examples/README.md` as an index with recommended reading order.
- Links from the main README's Quick Start.

**Success criteria.**

- Each example runs green against a fresh `docker compose up` in
  under a minute.
- Total LOC across examples < 2 000 (they're demos, not products).

**Depends on.** Phases 1 + 4 (stable surface + docs that the
examples link to). Some examples block on later phases and stage
in accordingly.

---

## Phase 6 — Publishing / releasing ✅ done (0.2.0 on opam)

**Goal.** `opam install valkey` works. `0.2.0` is the first
opam-published release (`0.1.0` was superseded after opam CI
rejected its test setup). A public announcement lands.

**Scope.**

- **Opam packaging discipline:**
  - `valkey.opam` has `description`, `maintainer`, `homepage`,
    `bug-reports`, `dev-repo`, `tags`, `depends` with lower bounds
    justified.
  - Decide on a package split:
    - `valkey` — core (Client, Router, Cluster_router, Transaction,
      Pubsub, Cluster_pubsub, Named_commands, Command_spec).
    - `valkey-tls` — optional TLS support via ocaml-tls.
    - `valkey-search`, `valkey-json`, `valkey-bloom` — modules
      (Phase 11). Released alongside or after 1.0.
  - `dune-project` pins lang + package metadata.
  - `dune build @install` + `opam-lint` pass clean.
- **Versioning policy (SemVer, opam style):**
  - Plain `0.x.y` while pre-stable — no `-alpha` / `-beta` suffix
    needed; the leading zero already signals "API may change."
    Matches Eio, Dune, Lwt, Mirage, and the rest of the opam
    ecosystem.
  - If a pre-release marker is ever needed, opam uses the tilde
    syntax (`0.1.0~alpha1` sorts *before* `0.1.0`) — never the
    npm-style `-alpha` hyphen suffix, which sorts the wrong way.
  - `1.0.0` when the public API is declared stable.
  - Deprecation: mark with `[@@deprecated]` for one minor version
    before removal. `CHANGELOG.md` notes it in both the deprecating
    release and the removing release.
- **Release process:**
  - GPG-signed annotated tag `v0.2.0`.
  - GitHub release with generated notes from `CHANGELOG.md`.
  - `opam publish` PR to `ocaml/opam-repository`.
  - Automate most of it via `scripts/release.sh`.
- **Announcement:**
  - Discuss.OCaml thread: "Announcing valkey, a modern Valkey
    client for OCaml 5 + Eio."
  - OCaml Weekly News submission.
  - Reddit /r/ocaml post.
  - Valkey project mailing list — the Valkey org cares.
  - Tweet / blog post linking to BENCHMARKS.md and the chaos story.

**Deliverables.**

- Correct `valkey.opam` accepted into the opam-repository as
  `valkey.0.2.0`.
- Git tag `v0.2.0` + GitHub release.
- Announcement posted in ≥ 3 channels.
- `scripts/release.sh` that handles CHANGELOG bump + tag + push.

**Success criteria.**

- `opam install valkey` works in a fresh switch.
- The announcement thread gets non-zero engagement.
- No regressions reported in the first 48 h that we didn't already
  know about.

**Depends on.** Phases 1–5 (the thing we're announcing needs to
be complete enough to use).

---

## Phase 7 — Batch + cluster-aware commands ✅ done (0.2)

**Shipped.** `Valkey.Batch` primitive with non-atomic (scatter-
gather) and atomic (`WATCH`/`MULTI`/`EXEC`) modes selected by a
flag. Typed cluster helpers (`mget_cluster`, `mset_cluster`,
`del_cluster`, `unlink_cluster`, `exists_cluster`,
`touch_cluster`, `pfcount_cluster`), `Batch.with_watch` /
`run_with_guard` for classic WATCH-style CAS, `Transaction`
rebased as a thin façade over atomic `Batch`, plus
`examples/10-batch/` and `docs/batch.md`.

Concurrent atomic batches on one router are **now safe** — the
router exposes a per-primary mutex (`atomic_lock_for_slot`) that
serialises MULTI/EXEC blocks on the shared pinned connection.
Non-atomic traffic bypasses it. A real connection pool (Phase 9)
will later let atomic ops against the same primary run on
different connections in parallel; the router's API doesn't
change when that lands.



**Goal.** `Client.mget client ["a";"b";"c"]` just works in cluster
mode — the library splits by slot, fans out in parallel, merges
results in input order. Same for `mset`, `del`, `unlink`, `exists`,
`touch`, `pfcount` (merge via count). Users stop needing to
carefully hashtag their keys.

**Scope.**

- **Slot-splitting dispatcher:**
  - Input: `(string list)` of keys.
  - Group by `Slot.of_key`.
  - For each group, dispatch on the primary owning that slot,
    reassemble in original order.
  - Fail-fast vs partial-success mode. Default: partial success
    (return a per-key `result`).
- **Typed wrappers:**
  - `Client.mget_cluster client keys` — returns `(string * string option) list`
    in input order.
  - `Client.mset_cluster client kvs` — returns `(unit, Error.t) result`
    (all or nothing semantically; but we split into per-slot MSETs
    which are each atomic, so documented as "each slot atomically,
    overall not").
  - `Client.del_cluster`, `Client.unlink_cluster`,
    `Client.exists_cluster`, `Client.touch_cluster`.
- **Pipelining primitive** (prerequisite for the split path to be
  fast): `Client.pipeline client` returns a pipeline handle;
  `Pipeline.queue handle args` enqueues without waiting; `Pipeline.flush`
  sends the batch and returns the replies in order. Fire-and-forget
  for the middle of the batch; only the flush blocks.
- **Cluster-aware variant:** same API but splits by slot internally,
  runs one pipeline per slot in parallel, merges. GLIDE-equivalent.
- **Benchmarks:**
  - Add cluster-mget scenarios to `bin/bench/`: 100 keys, 1000
    keys, mixed-slot vs hashtagged.
  - Ours should beat "N individual GETs" by a wide margin in
    cluster mode; target ≥ 3× at 1000 keys.

**Deliverables.**

- `lib/pipeline.ml` + `.mli` (standalone and cluster flavours).
- `Client.{mget,mset,del,unlink,exists,touch,pfcount}_cluster`
  typed helpers.
- Integration tests against the docker cluster for each new
  helper: 100 keys spread across all slots, round-trip.
- Bench scenarios showing ≥ 3× vs per-key loop.

**Success criteria.**

- A user writing an app with large batch ops gets cluster
  correctness by default, no hashtag gymnastics required.
- `mget_cluster` of 1000 keys hits ≥ 3× the per-key-loop ops/s.

**Depends on.** Phases 1 (for the non-typed primitives underneath)
and 6 (we want a 0.x release published before this lands).

---

## Phase 8 — Client-side caching

**Goal.** A read-through local cache that stays consistent via
Valkey's `CLIENT TRACKING` invalidation pushes. Huge read-heavy
perf win for apps that use the same keys repeatedly.

**Scope.**

- **Opt-in via new Client config:**
  `Client.Config.with_client_side_cache ~max_entries ~max_bytes`.
- **Under the hood:**
  - On connect, send `CLIENT TRACKING ON` (and on every reconnect
    via `on_connected`).
  - An LRU keyed by the bulk-string key, value = `(Resp3.t, time)`.
  - On a typed `get` call:
    - Look up in LRU.
    - If hit: return immediately.
    - If miss: send `GET`, store result, return.
  - A dedicated fibre consumes invalidation push frames (`["invalidate", key-list]`),
    evicts those keys from the LRU.
  - On reconnect: flush the whole LRU (can't trust stale state;
    tracking state on the server is gone).
- **Cluster-specific behaviour:**
  - Per-slot cache or shared cache? Shared is fine — the invalidation
    push comes on the connection to the slot's primary, but we key by
    the bulk-string name.
  - In cluster, `CLIENT TRACKING ON` is per-connection. Means every
    pool connection has its own tracking subscription. Push handlers
    need to dispatch across all pool connections.
- **Observability hooks:** `on_cache_hit` / `on_cache_miss` /
  `on_cache_invalidate` callbacks so users can expose Prometheus
  counters.
- **Tests:**
  - Functional: SET via another client → local GET sees new
    value (invalidation arrived) within N ms.
  - Stress: 10 k concurrent GETs with 1 % SET mix from another
    client. Cache hit rate ≥ 98 %, no stale reads past invalidation.

**Deliverables.**

- `lib/client_side_cache.ml` + `.mli`.
- Config knob + typed API that automatically caches reads.
- Functional + stress integration tests.
- Benchmark scenario showing ≥ 5× GET ops/s on a
  20 %-hot-keys workload vs the uncached path.

**Success criteria.**

- Cache correctness under concurrent writes from another client
  (no stale reads past 10 ms in local testing).
- Opt-in with a single config flag; no change to existing user
  code.

**Depends on.** Phase 1 (need `CLIENT TRACKING` as a typed
command) + Phase 2 (the testing rigour to trust the consistency).

---

## Phase 9 — Connection pool

**Goal.** `Client_pool.t` for apps that need more throughput than
one multiplexed socket can deliver (i.e., CPU-bound on parsing or
on one socket's server-side queueing).

**Scope.**

- **`Client_pool.t`:** owns N `Client.t`. Pick strategy is
  configurable:
  - `Round_robin` — cycle.
  - `Sticky_by_key` — hash the first arg to a sub-client; good
    locality, keeps caches warm.
  - `Least_loaded` — track per-client in-flight, pick minimum.
  - Default: `Sticky_by_key`.
- **Typed command wrappers** on the pool mirror `Client.t`'s —
  `Client_pool.set t k v` picks a client then calls `Client.set`.
- **Cluster-aware pool** — pool-of-pools? In cluster, each
  sub-client is itself a cluster router with the full fleet.
  Value comes from duplicating the CPU path, not the fleet.
- **Blocking pool** — separate type `Blocking_pool.t` with
  check-out semantics: `Blocking_pool.with_ blocking_pool
  (fun c -> ... run blocking command on c ...)`. Cap on
  simultaneous checkouts; queue behind cap.
- **Benchmarks:**
  - At what point does `Client_pool` beat single `Client.t`?
  - Pick strategy comparison.

**Deliverables.**

- `lib/client_pool.ml` + `.mli`.
- `lib/blocking_pool.ml` + `.mli`.
- Bench matrix with pool sizes 1 / 4 / 16 / 64.
- Example: `examples/09-pool/`.

**Success criteria.**

- At conc=500 with 16 `Client_pool` sub-clients, our throughput
  beats the single-client ceiling by ≥ 3×.
- Blocking pool handles 1000 concurrent `BLPOP` callers with a
  cap of 100 connections, bounded wait, no leaks.

**Depends on.** Phase 6 (1.0 stability first), Phase 7 (pipelining
primitive we can reuse).

---

## Phase 10 — IAM + authentication

**Goal.** First-class AWS IAM auth for ElastiCache-for-Valkey.
Rotate tokens without app-code awareness. mTLS for self-managed
clusters. No secrets in tests.

**Scope.**

- **IAM token provider:**
  - Compute SigV4 signature for `elasticache:Connect` (the service
    exposes an IAM-auth token endpoint; see AWS docs).
  - Token TTL ~15 minutes; provider refreshes before expiry.
  - Provider passes the token to `Connection` via a new
    `?auth_provider : unit -> (user:string * password:string)`
    hook that replaces static `user` / `password` in Config.
- **Other providers:** generic interface so users can plug:
  - AWS IAM (default implementation).
  - Env var loaded at call time.
  - HashiCorp Vault transit.
  - Custom closure.
- **Mutual TLS:**
  - Extend `Tls_config.t` with `~client_cert` / `~client_key`
    loader.
  - Plumb into the `wrap_with_tls` call path.
- **Hardcoded-secret audit:**
  - Replace the `"pass"` literal in
    `scripts/cluster-hosts-setup.sh` with a prompt / env-var
    reader. Nothing in the repo hardcodes a production secret.
  - `grep -i 'pass\|secret\|token\|key'` review, case by case.
- **Tests:**
  - IAM provider unit tests with a fake SigV4 signer.
  - mTLS integration test with our own CA: generate client cert,
    verify the server accepts.

**Deliverables.**

- `lib/auth/iam_provider.ml` (optional sub-package).
- Connection.Config extended with auth provider.
- `Tls_config` mTLS support.
- `docs/security.md` expanded with IAM + mTLS recipes.

**Success criteria.**

- A 6-hour connection against ElastiCache-for-Valkey via IAM
  survives at least one token rotation with zero user-visible
  errors.
- mTLS connects + handshakes cleanly; bad client cert rejected
  with a typed error.

**Depends on.** Phase 6 (production rollout needs a stable 1.0).

---

## Phase 11 — Module support (JSON / Search / Bloom)

**Goal.** Typed wrappers for the three big Valkey modules so users
don't have to reach for raw `exec` to use them.

**Scope (one optional sub-package per module):**

- **`valkey-json`** — typed wrappers for `JSON.*`:
  - `Json.get`, `set`, `del`, `arrappend`, `arrindex`, `arrlen`,
    `arrtrim`, `numincrby`, `objkeys`, `type`.
  - JSON-path strings passed through.
  - Reply decoding: keep the raw JSON as `string`; let the caller
    parse with whatever JSON lib they prefer (yojson, jsonaf, etc.).
- **`valkey-search`** — `FT.CREATE`, `FT.SEARCH`, `FT.AGGREGATE`,
  `FT.INFO`, `FT.DROPINDEX`:
  - Typed schema DSL for `FT.CREATE` index definitions.
  - Typed search-result decoder.
  - Rank / sort / limit argument builders.
- **`valkey-bloom`** — probabilistic filters:
  - `Bloom.*` (add / exists / mexists / insert).
  - `Cuckoo.*`.
  - `TDigest.*`.
  - `TopK.*`.
  - `Cms.*` (count-min sketch).

Each module:
- Own `lib/module_<name>/`.
- Own `.opam` so users pick only what they need.
- Own integration tests that need a Valkey build with the module
  loaded — CI fixture via docker image with modules preinstalled.

**Deliverables.**

- `valkey-json.opam`, `valkey-search.opam`, `valkey-bloom.opam`.
- Three integration test suites.
- Three example programs (`examples/10-json/`, `11-search/`,
  `12-bloom/`).

**Success criteria.**

- `opam install valkey-json` works; typed wrappers cover the
  JSON module's documented commands.
- Same for Search and Bloom.

**Depends on.** Phase 6 (package-split process established) +
Phase 5 (examples exist to model after).

---

## Phase 12 — Full audit (deep pass)

**Goal.** Before 1.0.0 stable. Go through every module with fresh
eyes now that the surface is complete, look for:

- Abstractions that leak.
- API inconsistencies between modules.
- Cases where we added a feature and forgot to document the
  interaction with another.
- Performance regressions that crept in.
- Opportunities to delete code (a good refactor removes more than
  it adds).

**Scope.**

- **API consistency review.** e.g. `Transaction.with_transaction`
  vs `Cluster_pubsub.create` — do they look like the same
  library? Name conventions, arg order, error types, timeout
  shape.
- **Doc comment pass.** Every `val` in every `.mli`: is the
  comment accurate, complete, example-bearing?
- **Benchmark sweep.** Re-run the full BENCHMARKS.md matrix on
  Linux bare metal (not WSL). Update the doc.
- **Cross-module invariants.** e.g. `Pubsub` and `Client` sharing
  a `Connection` is forbidden — is that actually enforced at a
  type level? (No — it's documented.) Audit similar.
- **Memory snapshot.** Use `memtrace` on a long-running example
  to find any unnoticed allocation hotpath.
- **The "six-month-later" question.** For each module, imagine
  returning in six months and having to extend it. Is it obvious
  how? If not, rewrite the module prelude comment.

**Deliverables.**

- `AUDIT.md` updated with findings + dispositions.
- Any fixes committed as separate small PRs, not one mega commit.
- Updated BENCHMARKS.md on bare-metal hardware.
- 1.0.0 release tag after the audit and any fixes land.

**Success criteria.**

- No inconsistency a reader would flag in a code review.
- 1.0.0 API stable for at least one minor-release cycle before
  needing a breaking change.

**Depends on.** Phases 1–11. This is the "ready for 1.0" gate.

---

## Post-1.0 — re-assess

Once Phase 12 closes and 1.0.0 ships, pause, look at real-user
feedback, and decide among:

- Observability expansion (OTel, metrics hooks, slow-tap, health
  API — most of which were scoped into earlier phases).
- Pipelining primitive exposed as a first-class user-facing API
  (if Phase 7 handled it internally only).
- Keyspace notifications as a typed Pubsub consumer.
- Stream consumer helper (`XREADGROUP` loop + XACK + XCLAIM + DLQ).
- Redlock and rate-limiting primitives.
- RESP3 streaming aggregates.
- Code-generation of the command spec from `COMMAND INFO`.
- Cluster scan (if Valkey 9.1 native isn't enough for some
  reason).
- TLA+ model of the retry state machine.
- Read-your-writes consistency helper (WAIT under the hood).

Re-elaborate this list when we get there; priorities will have
shifted.

---

## Non-goals (permanent)

- **RESP2 fallback.** RESP3-only.
- **Lwt or Async bindings.** Eio only.
- **Redis <7 support.** Valkey 7.2+ is the target.
- **Client-side cluster SCAN (custom).** Valkey 9.1's native
  cluster SCAN is the answer.
- **Re-implementing hiredis.** We're at 85–96 % of C across
  scenarios; closing the last points means giving up the
  Eio-fibre pipeline.
- **Sentinel support.** The Valkey team considers cluster mode
  the replacement.
