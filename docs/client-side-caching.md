# Client-side caching

> **Status: in progress.** This doc grows step by step alongside the
> implementation (ROADMAP Phase 8). Steps already shipped are marked
> ✅; everything else is `…` until it lands.

## Why it exists

Valkey's client-side caching lets a client cache `GET`-style responses
in memory and trust the server to send invalidation pushes whenever a
cached key changes. Two reads of the same hot key cost one network
round-trip + many local hashtable hits.

The library implements this in pieces. Each piece below is a
self-contained step; together they form the full feature.

## Pieces

### ✅ 1. Cache primitive — `Valkey.Cache`

A bounded in-process cache, keyed by string, holding `Resp3.t`
values. LRU eviction under a configurable byte budget. All operations
are mutex-guarded and safe across Eio fibers.

This is just a data structure; it does not talk to Valkey on its own.
The connection-tracking and invalidation pieces (steps 2–4) plug into
it.

API: see [`lib/cache.mli`](../lib/cache.mli). Concretely:

```ocaml
let cache = Valkey.Cache.create ~byte_budget:(64 * 1024 * 1024) in
Valkey.Cache.put cache "user:42" some_resp3_value;
match Valkey.Cache.get cache "user:42" with
| Some v -> use v
| None -> fetch_from_server ()
```

Sizing:

- A `Bulk_string s` accounts as `String.length s + 16` (the 16-byte
  constant is a coarse overhead estimate covering the OCaml block
  header, key copy, and LRU bookkeeping; intentionally a slight
  overestimate so the byte budget stays conservative).
- Aggregates (`Array`, `Map`, `Set`, `Push`) account recursively plus a
  per-aggregate constant.

The byte budget is a *soft* limit: a `put` that fits in budget after
evicting the LRU tail succeeds; a single value strictly larger than
the budget is rejected (`put` is a no-op).

### ✅ 2. `CLIENT TRACKING` handshake on connect / reconnect

Connection config gains a `client_cache : Client_cache.t option`.
When `Some cfg`, `full_handshake` issues `CLIENT TRACKING ON
[OPTIN] [BCAST PREFIX …] [NOLOOP]` after `HELLO`/`SELECT`, per
`cfg.mode` and `cfg.noloop`. The existing `on_connected`
hook re-issues the same sequence after every reconnect, matching
the pubsub-resubscribe pattern.

Fails closed: if the server rejects `CLIENT TRACKING` (older
server, ACL denies the command, invalid prefix, etc.), the whole
connect fails rather than silently dropping to an unconfigured
cache. Users see a typed handshake error.

No reads or writes go through the cache yet; this step just makes
the server aware the connection is tracking so that subsequent
steps can consume invalidation pushes. See
[`lib/client_cache.ml`](../lib/client_cache.ml),
[`lib/connection.ml`](../lib/connection.ml) (`run_client_tracking`,
`full_handshake`).

### ✅ 3. Invalidation parser + invalidator fiber

`lib/invalidation.ml` translates a parsed RESP3 push frame into
`Invalidation.t` (`Keys [...]` or `Flush_all`). Malformed or
not-ours pushes return `None` and are skipped — no partial eviction
on spec violations.

Routing happens at the source. `parse_worker` in `Connection`
classifies each incoming push: invalidations go on a dedicated
`invalidations : Invalidation.t Eio.Stream.t`; everything else
(pubsub deliveries, `tracking-redir-broken`, future pushes) stays
on `pushes` where pubsub consumers already read. Rationale: one
consumer per stream avoids fan-out complexity; pubsub users who
also enable CSC are unaffected.

An invalidator fiber is spawned automatically whenever
`client_cache = Some _`. It loops on `Eio.Stream.take
t.invalidations` and calls `Invalidation.apply cache inv` —
`Keys` evicts each, `Flush_all` clears the whole cache. Exits
cleanly when the connection's `closing` atomic flips or the
supervisor resolves `cancel_signal`.

`Invalidation.apply` is a plain function (no Eio), so its
behaviour is unit-tested directly against `Cache.t`.

Nothing populates the cache yet — that's step 4.

### ✅ 4. Read-path caching (default tracking)

`Client.Config.t` gains a `client_cache : Client_cache.t option`
field. When set, `Client.connect` propagates it into the inner
`Connection.Config` so every shard connection shares one
`Cache.t` and one tracking mode. Atomic batches, pubsub, and raw
`Connection.request` paths bypass the cache by construction —
they don't go through `Client.get`.

`Client.get` consults the cache first. On hit, returns the
cached value with no wire traffic and no replica selection
(the cache is a logical view of server state keyed by the
Valkey key, not by routing target). On miss, `exec` as
before; the returned `Bulk_string` is cached so the next `get`
hits. `Null` (missing key) is intentionally not cached — a
later external SET would have no entry to evict via the
invalidation path, so a negative cache could stay stale forever.

OPTIN-armed reads (one extra wire frame per cached read in
exchange for a smaller server-side tracking table) land in
step B2.5 below.

Single-flight dedup and in-flight/invalidation race handling
land in step 5.

See `lib/client.ml` — `resolve_connection_config`, the
`client_cache` field on `t`, and the `Client.get` branch that
checks cache first.

### ✅ 5. Single-flight + invalidation-race safety

New `lib/inflight.ml`: a per-key table of `{promise; resolver;
dirty}` entries. `Client.get` on miss calls
`Inflight.begin_fetch` — `Fresh resolver` makes the fiber the
owner of the wire fetch; `Joining promise` joins a pending
fetch and wakes on the owner's resolution. On completion the
owner runs `Inflight.complete`, which returns `Clean` (ok to
cache) or `Dirty` (an invalidation raced the fetch; don't
cache). Joined fibers see the fetched value either way — only
the *cache write* is suppressed.

The invalidator fiber calls `Inflight.mark_dirty` **before**
`Cache.evict` for each invalidated key, so a fetch that
started before the invalidation but completes after finds
`dirty = true` and skips the cache write.

Lock-ordering rule: `Cache.mutex` and `Inflight.mutex` are
independent; no path ever holds both. No deadlock possible.

Flush_all known gap: when `FLUSHALL`/`FLUSHDB` arrives we
clear the cache but do not mark every in-flight fetch dirty
(we don't index pending fetches by "all keys"). An owner
whose fetch was in-flight at the moment of flush will cache
the now-stale value. That window closes on the next per-key
invalidation, TTL expiry (step 8), or reconnect-flush
(step 7). Matches lettuce's behaviour.

Refactor: extracted `Connection.Error.t` into `Connection_error`
to break a dep cycle (Connection → Client_cache → Connection).
`Connection.Error` is now an alias — external surface
unchanged.

Also added `Client_cache.make ~cache ()` convenience constructor;
the record form still works but `make` initialises `inflight`.

### ✅ 6a. HGETALL + SMEMBERS cache coverage

`Client.hgetall` and `Client.smembers` now go through the same
cache path as `Client.get`. One logical Valkey key → at most
one cache entry, regardless of which command read it; Valkey's
WRONGTYPE rule keeps this from producing wrong-shape cached
entries in practice, and the read-path treats a wrong-shape
entry as a miss and re-fetches (single-flight dedups).

Refactor: the `cached_read` helper in `lib/client.ml` is now
generic over four parameters (`cmd`, `accepts`, `cache_ok`,
`decode`) so adding the next cacheable command is a few lines.

Deliberately **not** cached in this library:

- `HGET` / `HMGET`: would require a compound `(key, field)`
  cache key and a prefix-evict on invalidation. redis-py
  shipped this and hit a field-collision bug (issue #3612).
  Not worth the complexity until proven needed.
- `EXISTS`: returns a count across multiple keys. Caching
  requires a per-key boolean store separate from the reply
  cache. Low value.
- `STRLEN` / `TYPE`: cacheable in principle but every
  invalidation for the key also evicts the `GET`-shaped
  cache entry these would ride on. Users who ask for these
  pay one round-trip; not worth a specialised path.

### ✅ 6b. MGET scatter-gather over cache state

`Client.mget` splits input keys into three disjoint groups per
call:

- **Hit** — in cache already; no wire.
- **Batch** — not in cache, no in-flight fetch; this fiber owns
  the fresh MGET for these keys.
- **Joining** — not in cache but another fiber's in-flight
  fetch already covers these keys; await the per-key promise.

We issue ONE wire MGET containing only the Batch keys. Reply
elements align with that sub-list; we walk both together,
calling `Inflight.complete` per key (respecting the dirty flag
from a mid-flight invalidation), then `Cache.put` on
`Clean + Bulk_string` elements only (Null is not cached —
matches `Client.get`). The per-key in-flight resolvers are
resolved with the per-key element so any joiners on other
fibers wake. Results from all three groups merge back into the
caller's input order via an indexed array.

All-hit MGET issues zero wire commands. Partial-hit MGET
issues one wire command of length |misses|, not |total|.

If the batched MGET fails at the wire level, every batched
key's inflight entry is `abandon`ed and each resolver is
resolved with the error so joiners propagate it. The call
returns `Error e`; hits and joiners-that-succeeded do not
override the failure.

### ✅ 7. Cluster integration

Per-shard tracking already worked by construction: Client
threads the `Client_cache.t` reference into every per-shard
`Connection.Config` that `Cluster_router.build_pool` creates,
so every shard's handshake issues `CLIENT TRACKING ON` and
every shard's invalidator fiber drains into the same shared
`Cache.t`. This step added the missing flush invariants:

- **Topology refresh** (`apply_new_topology` in
  `cluster_router.ml`) calls `Cache.clear` when the topology
  SHA changes. Slot migrations, failovers, and topology-
  triggered reconnects all funnel through here. Closes the
  slot-migration staleness window (key K migrated A→B; the
  old cached value from A is dropped before the next read
  routes to B).
- **Per-connection reconnect** (`recovery_loop`'s success
  branch in `connection.ml`) calls `Cache.clear` on every
  successful reconnect. The server forgets our tracking
  context while we're disconnected; entries cached from a
  blipped shard would stop getting invalidations. Coarse
  (one shard blip in a 100-shard cluster clears everything),
  but correct; matches redis-py.

MOVED/ASK redirect does not need its own eviction path: the
existing `trigger_refresh ()` call in `handle_retries` wakes
the refresh fiber, and the next `apply_new_topology` with a
different SHA clears the cache.

Integration tests (`test/test_csc_cluster.ml`, needs
`docker-compose.cluster.yml`):
- Two hashtag-distinct keys route to different shards; external
  writes to both shards are caught by each shard's invalidator
  fiber and evict from the shared cache.
- `FLUSHDB` fanned out across all primaries clears the whole
  cache (one null-body invalidation per shard, all evicting
  the same shared cache).

### ✅ 8. Lifecycle failure-mode tests

Prove the flush invariants hold end-to-end against a real
server / cluster:

- **Standalone reconnect**: populate cache, `CLIENT KILL` self,
  assert `Cache.count = 0` after reconnect, then assert a fresh
  read re-populates (tracking is back). Validates B7.2.
- **Cluster failover**: populate cache with keys on two shards,
  pick a replica from `CLUSTER NODES`, force promotion with
  `CLUSTER FAILOVER FORCE`, assert cache cleared within a few
  seconds. Validates B7.1 (topology-refresh flush) + B7.2
  (per-connection reconnect flush) together — whichever path
  fires first does the clear.

Tests live in `test/test_csc_lifecycle.ml`, both marked `Slow`
(multi-second by design). Cluster test skips gracefully when
the compose cluster isn't reachable.

Slot-migration-under-load and OOM are not covered here; the
former needs orchestrating a live `CLUSTER SETSLOT` loop with
concurrent cached reads, the latter needs a stress harness.
Deferred until there's an incident report asking for them.

### ✅ 9. Per-key TTL safety net + Flush_all in-flight-race fix

Two small but correctness-material additions:

1. **Lazy TTL on `Cache` entries.** `Cache.put ?ttl_ms` sets a
   wall-clock deadline per entry (same epoch as
   `Unix.gettimeofday`). `Cache.get` checks the deadline on
   every hit; expired entries are evicted in place and
   reported as a miss. No background sweeper. `Client_cache.t`
   already carried `entry_ttl_ms`; `Client.get` / `hgetall` /
   `smembers` / `mget` all forward it to `Cache.put`.
2. **`Inflight.mark_all_dirty`.** `Invalidation.apply` now
   marks every pending fetch dirty before clearing the cache
   on `Flush_all`, closing the in-flight-race gap documented
   in B3. An owner whose fetch was mid-flight at the moment
   of a server `FLUSHDB`/`FLUSHALL` will now see `Dirty` on
   completion and skip the cache write, instead of re-populating
   with a pre-flush stale value.

TTL is off by default (`entry_ttl_ms = None`). Users who want
defense-in-depth against missed invalidations set e.g.
`Client_cache.make ~cache ~entry_ttl_ms:60_000 ()` for a 60s
cap. Cost: one extra float comparison per hit.

Tests:
- 3 unit tests on `Cache`: no-TTL entries don't expire; TTL
  entries miss after deadline; `put` refreshes the deadline.
- 2 unit tests on `Inflight.mark_all_dirty`: flips every
  pending entry; no-op on empty table.
- 1 unit test on `Invalidation.apply Flush_all`: marks every
  pending fetch dirty.
- 1 integration test: TTL expires a cached entry with no
  server-side activity.

All 136 unit + 22 integration tests green.

### ✅ 10a. Metrics

`Cache` carries six `int Atomic.t` counters: `hits`, `misses`,
`evicts_budget` (byte-budget LRU eviction), `evicts_ttl` (lazy
TTL expiry on read), `invalidations` (`evict` calls; fed by the
invalidator fiber and by user `evict`), and `puts` (every `put`
call including oversize-rejected ones). Read non-locking via
`Cache.metrics : t -> metrics`; `Cache.reset_metrics : t -> unit`
zeroes the counters (for benchmarks — production should use
deltas).

Exposed at the Client layer as `Client.cache_metrics : t ->
Cache.metrics option`. Returns `None` when caching isn't
enabled.

### ✅ 10b. BCAST mode

Prefix-based tracking. Instead of the server maintaining a
per-client table of keys this client has read, the client
declares prefixes; the server broadcasts invalidations for any
write to any key matching any declared prefix. Scales to fleet
deployments where a per-key tracking table on the server would
blow up.

Setup via `Client_cache.t.mode = Bcast { prefixes }`.
`Connection`'s `full_handshake` emits
`CLIENT TRACKING ON BCAST PREFIX <p1> [PREFIX <p2> ...]`; the
same handshake replay on reconnect already covers BCAST.

The wire frame for BCAST invalidations is **identical** to
default-mode invalidation (`> "invalidate" [keys]`) — confirmed
empirically against Valkey 9. Our existing `Invalidation.of_push`
parser handles both. No code changes needed in `lib/invalidation.ml`
or the invalidator fiber.

Caveats:
- **No client-side prefix-overlap validation yet.** Server
  rejects overlapping prefixes (e.g. `"user:"` and
  `"user:admin:"`) with `ERR Tracking prefix <p> already
  specified`. `Connection.run_client_tracking` surfaces that
  server error and fails the handshake cleanly. Could
  pre-check client-side; not done.
- **In BCAST mode, invalidations arrive for keys we never
  read.** If the cache was pre-populated by a matching key's
  read and a sibling key under the same prefix is written,
  only the written key's name is in the push's keys array —
  the sibling cache entries stay put. This is what BCAST
  promises: "invalidate keys matching the prefix," not "flush
  everything under the prefix." Proved by the `out-of-prefix
  writes leave cache alone` test.

Integration tests (`test/test_csc_bcast.ml`):
- BCAST flag + declared prefix show up in `CLIENT
  TRACKINGINFO`.
- External SET on a prefix-match key evicts our cached entry.
- External SET on a key *outside* the prefix does not evict.

### ✅ B2.5. OPTIN — pipelined per-read tracking

Default and BCAST tell the server to track everything we read
(or everything matching a prefix). OPTIN is the third option:
the server tracks **only** the keys we explicitly opt in for,
read by read. Smaller server-side tracking table at the cost
of one extra wire frame per cached read.

Configured via `Client_cache.t.mode = Optin`. The handshake
emits `CLIENT TRACKING ON OPTIN` (instead of plain `ON` for
default or `ON BCAST PREFIX ...` for BCAST). Mutually exclusive
with BCAST — encoded directly in the `mode` variant so
ill-formed combinations don't compile.

#### Wire shape

The server consumes the OPTIN flag from a `CLIENT CACHING YES`
command and applies it to **exactly the next single command on
the wire**. Verified empirically against Valkey 9.0.3:

- A pipelined `CACHING YES + GET k1 + GET k2` tracks `k1` only.
- A `CACHING YES + SET kw + GET kw` tracks nothing (the SET
  consumes the flag).
- A `CACHING YES + PING + GET k` tracks nothing.
- A `CACHING YES + MULTI + GET k + EXEC` does track `k` —
  MULTI/EXEC counts as one logical command.
- `CACHING YES` outside OPTIN/OPTOUT mode returns `ERR`.
- Double-armed `CACHING YES + CACHING YES + GET k` is benign;
  flag stays set.

Implication: each cached OPTIN read must be a wire-atomic pair
of exactly two frames — `CLIENT CACHING YES` then the read.
Two separate `Connection.request` calls are insufficient,
because another fiber's enqueue can land between them.

#### Atomicity primitive — `Connection.request_pair`

`lib/connection.ml` adds an internal `request_pair` that takes
two arg arrays, builds both wire frames, concatenates them
into a single `queued` entry with two ordered FIFO matching
slots, and enqueues as one indivisible unit. The writer
flushes both frames back-to-back; the parser pulls two replies
in order and resolves both promises. No new mutex — the queue
is already the per-connection serialization point. Generalises
the existing per-`queued` `entry option` to an `entry list` so
that `send_fire_and_forget` (`[]`), normal `request`
(`[ entry ]`), and `request_pair` (`[ e1; e2 ]`) all share one
code path.

The outer `Error.t` covers submit failures (`Closed`,
`Circuit_open`, `Queue_full`). The inner pair carries each
frame's reply independently — frame 1 may succeed
(`Simple_string "OK"`) while frame 2 errors (`WRONGTYPE`),
without either failing the other.

#### Read-path integration

`Client.cached_read`'s miss branch now dispatches on
`ccfg.mode`:

```ocaml
match ccfg.Client_cache.mode with
| Client_cache.Optin -> exec_optin_pair ?timeout t cmd
| Client_cache.Default | Client_cache.Bcast _ ->
    exec ?timeout ?read_from t cmd
```

`exec_optin_pair` looks up the standalone primary connection
directly (the cluster gate enforces standalone for OPTIN),
calls `Connection.request_pair conn ["CLIENT";"CACHING";"YES"] cmd`,
and:

- maps the CACHING reply: `Simple_string "OK"` lets the read
  reply through unchanged; anything else surfaces as a
  `Protocol_violation`,
- propagates transport errors from either frame as an `Error`,
- never poisons `Inflight` — the existing single-flight
  machinery treats the result the same way as it would for a
  default-tracking miss.

Cluster + OPTIN with redirect-aware pair retry is a separate
step; today, constructing a `Client.t` from a non-standalone
router with `mode = Optin` raises `Invalid_argument` at
`from_router`.

#### Integration tests (`test/test_csc_optin.ml`)

- **Populates then hits.** Cold OPTIN GET round-trips and
  caches; second GET serves from cache.
- **External SET evicts.** The load-bearing test: an external
  write must produce an invalidation push, which only happens
  if the wire-pair was actually adjacent and the server
  actually started tracking the key.
- **50-fiber concurrency.** N fibers each issue an OPTIN read
  on a distinct key; aux mutates every key; every cache entry
  must be invalidated. Detects any wire-pair atomicity break
  under concurrent enqueue (a broken pair would leave one
  fiber's read untracked, no invalidation, that entry stays
  cached, count > 0).
