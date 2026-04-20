# Pub/sub

Valkey has three pub/sub primitives:

- `SUBSCRIBE` / `UNSUBSCRIBE` — exact-match channels.
- `PSUBSCRIBE` / `PUNSUBSCRIBE` — pattern-match channels.
- `SSUBSCRIBE` / `SUNSUBSCRIBE` — **sharded** channels (Valkey 7+).
  Messages stay within a single cluster slot; publishers must use
  `SPUBLISH` to the same slot.

This library wraps them with two modules:

- `Pubsub` — standalone or single-node. One connection, tracks
  subscriptions, resubscribes automatically on reconnect.
- `Cluster_pubsub` — cluster-aware. Handles regular + sharded
  subscriptions on one handle. Sharded subscriptions get a
  dedicated connection per slot; a watchdog re-pins on failover.

Use `Cluster_pubsub` if you're in cluster mode, even if you're
only using `SUBSCRIBE` today — it's a superset.

## Basic `Pubsub`

```ocaml
let ps =
  Valkey.Pubsub.create ~sw ~net ~clock
    ~host:"localhost" ~port:6379 ()
in
let _ = Valkey.Pubsub.subscribe ps [ "orders" ] in

(* Another fiber publishes: *)
Eio.Fiber.fork ~sw (fun () ->
    let pub =
      Valkey.Client.connect ~sw ~net ~clock
        ~host:"localhost" ~port:6379 ()
    in
    let _ =
      Valkey.Client.publish pub ~channel:"orders" ~message:"o-42"
    in
    Valkey.Client.close pub);

(* Block for a delivery: *)
match Valkey.Pubsub.next_message ~timeout:5.0 ps with
| Ok (Channel { channel; payload }) ->
    Format.printf "got %s on %s@." payload channel
| Ok (Pattern _) | Ok (Shard _) -> ()  (* not from plain SUBSCRIBE *)
| Error `Timeout -> Format.printf "no delivery@."
| Error `Closed -> ()
```

Messages come through a typed sum — you always know which
subscription they came from.

## Auto-resubscribe

When the underlying connection drops (network blip, server
restart, TLS renegotiate), the client reconnects with jittered
backoff. After a successful reconnect, `Pubsub` replays every
channel and pattern it was subscribed to before the drop. This is
wired through `Connection.on_connected`:

- The Connection layer fires the callback after every successful
  handshake (first connect, every reconnect).
- `Pubsub` installs a callback that re-emits the tracked
  channels and patterns.
- From the caller's point of view, subscriptions are durable
  across restarts.

No action needed in user code. Test this path yourself with
`docker restart valkey-dev` mid-run.

## `Cluster_pubsub`

```ocaml
let cp =
  Valkey.Cluster_pubsub.create ~sw ~net ~clock ~router ()
in

(* Regular cluster-wide channel. *)
let _ = Valkey.Cluster_pubsub.subscribe cp [ "orders" ] in

(* Sharded channel — pinned to its slot's primary. *)
let _ = Valkey.Cluster_pubsub.ssubscribe cp [ "orders:{us-east}" ] in

match Valkey.Cluster_pubsub.next_message ~timeout:5.0 cp with
| Ok (Channel { channel; payload }) -> (* SUBSCRIBE delivery *)
| Ok (Pattern { channel; payload; pattern }) -> (* PSUBSCRIBE *)
| Ok (Shard { channel; payload }) -> (* SSUBSCRIBE *)
| Error _ -> ()
```

Internally:

- **Global connection** — one socket to any live primary.
  Handles `SUBSCRIBE` / `PSUBSCRIBE`. Valkey 7+ broadcasts those
  across the cluster; any primary suffices.
- **Per-slot connections** — one socket per distinct slot your
  `SSUBSCRIBE` set covers. Opened lazily on first sub to that
  slot.
- **Watchdog fiber** — every 1 s, walks the tracked slots and
  asks the router if the primary for each slot has changed. On
  change, closes the old shard connection, opens a new one at
  the new address, lets `on_connected` replay the slot's
  `SSUBSCRIBE` set.

A test in [`test/test_cluster.ml`](../../test/test_cluster.ml)
force-restarts all three primaries in sequence and asserts
sharded delivery resumes within 15 s.

## Delivery guarantees

Valkey pub/sub is **at-most-once**. A message delivered while a
subscriber is temporarily disconnected (reconnecting, mid-
failover, crashed) is lost — Valkey doesn't buffer.

If you need durability:

- Use Streams (`XADD` + `XREADGROUP` + `XACK`). Consumer groups
  remember per-group cursors, so a brief disconnect doesn't lose
  messages.
- Or: publish to both a channel and a stream, and treat the
  stream as the authoritative log.

**Sharded pub/sub has the same semantics.** It's an optimisation
for localised traffic (writes to slot X only reach subscribers
listening on that slot), not a durability improvement.

## Unsubscribe semantics

`unsubscribe cp []` — empty list means "unsubscribe from every
channel", matching the Valkey server's semantics.

`ssubscribe cp [ch1; ch2]` followed by `sunsubscribe cp [ch1]`
leaves `ch2` subscribed. If `ch1` was the only channel on its
slot, the shard connection is kept open (idle) until either
another slot subscription demands it, or `close` tears everything
down. We don't eagerly close idle shard connections — the
round-trip cost of re-opening on the next use is higher than the
socket idle cost.

## Pattern performance note

`PSUBSCRIBE orders.*` is O(N) per publish across all active
patterns cluster-wide, where N is the sum of all subscribers'
pattern counts. Under heavy publish traffic this shows up as
primary CPU. Prefer exact-match `SUBSCRIBE` or sharded
`SSUBSCRIBE` when you can.

## See also

- [cluster.md](cluster.md) — topology and failover concepts.
- [`Pubsub.mli`](../valkey/Valkey/Pubsub/index.html) — standalone
  API reference.
- [`Cluster_pubsub.mli`](../valkey/Valkey/Cluster_pubsub/index.html)
  — cluster API reference.
- [`test/test_cluster.ml`](../../test/test_cluster.ml) — the
  failover-replay test is the canonical example.
