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

### … 3. Invalidator fiber

Drains RESP3 push frames, parses invalidation messages, evicts.

### … 4. Race-safe GET path

In-flight tracking + concurrent-fetch dedup + drop-on-invalidation.

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
