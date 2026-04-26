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
`cfg.mode`, `cfg.optin`, `cfg.noloop`. The existing `on_connected`
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

### ✅ 4. Read-path caching (`optin=false`)

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

`optin=true` is planned for a later step (`B2.5`, pipelined
`CLIENT CACHING YES` + read). Setting `optin=true` on the config
today raises `Invalid_argument` at connect time.

Single-flight dedup and in-flight/invalidation race handling
land in step 5.

See `lib/client.ml` — `resolve_connection_config`, the
`client_cache` field on `t`, and the `Client.get` branch that
checks cache first.

### … 5. Per-command coverage

`MGET`, `HGET`, `HMGET`, `HGETALL`, `EXISTS`, `STRLEN`, `TYPE`.

### … 6. Cluster integration

Per-shard tracking, single shared cache, reconnect-flush invariant.

### … 7. Failure-mode tests

Failover mid-cache, slot migration mid-cache, OOM under load.

### … 8. Per-key TTL safety net

Optional defense-in-depth.

### … 9. BCAST mode

Alternate invalidation path with prefix subscription.
