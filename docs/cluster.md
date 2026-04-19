# Cluster

This guide explains how to run `ocaml-valkey` against a Valkey
cluster, what happens under the hood when topology changes, and
how to control the routing knobs.

## Connect

```ocaml
let seeds = [ "node-a.internal", 6379;
              "node-b.internal", 6379;
              "node-c.internal", 6379 ]

let () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let net = Eio.Stdenv.net env in
  let clock = Eio.Stdenv.clock env in
  let config =
    { (Valkey.Cluster_router.Config.default ~seeds) with
      prefer_hostname = true }
  in
  match Valkey.Cluster_router.create ~sw ~net ~clock ~config () with
  | Error msg -> failwith msg
  | Ok router ->
      let client =
        Valkey.Client.from_router
          ~config:Valkey.Client.Config.default router
      in
      let _ = Valkey.Client.set client "k" "v" in
      Valkey.Client.close client
```

Seeds are just a bootstrap list. As soon as `create` has read the
topology from a quorum of seeds, the router uses the real
topology — the seed list is never consulted again under normal
operation (it's re-probed only as a last-resort fallback when the
primary topology refresh can't reach any live node).

`create` raises if it can't reach a quorum of seeds. No error
value; no partially-alive router. Setup cost dominates: you pay
it once at boot, not per call.

## What `create` actually does

1. Opens a transient connection to each seed in parallel.
2. Sends `CLUSTER SHARDS` to each.
3. Hashes the canonical serialisation of each reply (minus
   ever-moving fields like `replication_offset`).
4. Requires `agreement_ratio` of the responding seeds to agree on
   the same SHA — otherwise `create` fails with
   `"cluster_router: no quorum"`.
5. Closes the seed connections, opens a `Node_pool.t` with one
   `Connection.t` per primary + replica in the agreed topology.
6. Forks a refresh fiber that re-runs this dance every
   `refresh_interval + random(0, refresh_jitter)` seconds.

The refresh fiber short-circuits if the SHA hasn't changed — so a
steady cluster pays only the cost of fetching `CLUSTER SHARDS`
from one primary every ~5 s.

## Routing

Every typed command is annotated in `Command_spec` with where it
belongs:

| Spec                            | Meaning                                 |
|---------------------------------|-----------------------------------------|
| `Single_key { key_index; readonly }` | Slot = `CRC16(key)`. Writes go to primary; readonly obeys `Read_from`. |
| `Multi_key { first_key_index; readonly }` | Slot = `CRC16(first key)`. Caller must co-locate via hashtags; any MOVED means the hashtag contract was broken. |
| `Keyless_random { readonly }`   | Any node. `PING`, `TIME`, `CLIENT ID`.  |
| `Fan_primaries`                 | Every primary (e.g. `SCRIPT LOAD`).     |
| `Fan_all_nodes`                 | Every node (e.g. `CLIENT LIST`).        |

When you call `Client.set client "foo" "bar"`:

1. `Command_spec.lookup [| "SET"; ...; "foo"; "bar" |]` returns
   `Single_key { key_index = 1; readonly = false }`.
2. Router computes `slot = CRC16_XMODEM("foo") & 16383`.
3. Router consults the current topology to find the primary
   owning that slot and dispatches through the pooled connection
   to that node.
4. Reply comes back — success, or one of the retryable errors
   below.

## MOVED / ASK / CLUSTERDOWN / TRYAGAIN / Interrupted

Five server-side conditions are retried transparently up to
`max_redirects` (default 5):

| Condition     | What it means                                    | Client response |
|---------------|--------------------------------------------------|-----------------|
| `MOVED host port` | Our topology is stale.                        | Redial via `Topology.find_node_by_address`; re-send on the new node; wake the refresh fiber. |
| `ASK host port`   | Slot is migrating; this request goes there.   | Send `ASKING` then the original command, once. Don't update topology. |
| `CLUSTERDOWN`     | No primary owns some slot (e.g. during failover). | Sleep, wake refresh fiber, retry. Exponential backoff 100ms → 1.6s capped. |
| `TRYAGAIN`        | Multi-key command spans a migrating slot.     | Sleep 50 ms, retry. No refresh. |
| `Interrupted`/`Closed` | Connection died mid-flight.              | Sleep 50 ms, wake refresh, retry. |

These are the only retry conditions. Anything else —
`WRONGTYPE`, `NOSCRIPT`, `WRONGPASS`, a hung server — surfaces to
the caller unchanged.

The exponential `CLUSTERDOWN` backoff exists because primary
failover gossip takes a few seconds to converge. A fixed
`max_redirects × 100ms` would routinely exhaust on real
failovers. See `lib/cluster_router.ml:clusterdown_backoff_for_attempt`
for the curve.

## Read_from

`Client.set/get/...` all accept a `?read_from:Read_from.t`:

```ocaml
(* Readonly GET that may reach a replica: *)
let _ = Valkey.Client.get client "k"
          ~read_from:Valkey.Router.Read_from.Prefer_replica
```

Writes always go to the primary regardless of `Read_from`. For
readonly commands:

| `Read_from`                              | Behaviour                                       |
|------------------------------------------|-------------------------------------------------|
| `Primary`                                | Always the primary. (Default.)                  |
| `Prefer_replica`                         | Pick a random replica; fall back to primary if none. |
| `Az_affinity { az }`                     | Pick a replica in this AZ; else any replica; else primary. |
| `Az_affinity_replicas_and_primary { az }`| Same, but include the primary in the AZ pool.   |

Set the default on the client:

```ocaml
let config =
  { Valkey.Client.Config.default with
    read_from = Valkey.Router.Read_from.Prefer_replica }
in
let client = Valkey.Client.from_router ~config router
```

## Multi-key commands + hashtags

Cluster mode rejects any multi-key command whose keys live in
different slots with `CROSSSLOT`. To co-locate, use hashtags:

```
user:42:profile    -> slot CRC16("user:42:profile")
user:{42}:profile  -> slot CRC16("42")
user:{42}:settings -> slot CRC16("42")   (same)
```

`MSET`, `MGET`, `DEL [k1;k2;k3]`, `SUNION`, transactions — all
require the keys to hash to the same slot.

If you control the schema, use `{...}` to pin related keys into
the same slot. If the keys are truly unrelated,
you have three options:

1. Call per key in a loop (each lands in the right slot).
2. Use `Client.exec_multi` / `custom_multi` for fan-out
   aggregation commands.
3. Wait for the Phase 7 batch layer — it will split multi-key
   commands by slot and fan them out in parallel, then stitch the
   results.

## Failover: what actually happens

You restart primary `c1`:

1. In-flight commands on `c1` surface `Interrupted` / `Closed`.
2. The router sleeps 50 ms, wakes the refresh fiber, re-dispatches.
3. Refresh sees `c1`'s replica has taken over → SHA differs →
   topology updated, pool adds the new primary.
4. The re-dispatched command now routes to the new primary, succeeds.
5. Caller sees a single `Ok _` — the transient failure was absorbed.

End-to-end latency during a failover is dominated by cluster
gossip convergence, not the client. Our chaos tests force all 3
primaries to restart and observe zero user-visible errors at 12k
ops/s.

## `Cluster_pubsub` — special case

Pub/sub is not dispatch-by-slot in the same way. Regular
`SUBSCRIBE` broadcasts cluster-wide, so a single connection to
any live primary is enough. Sharded `SSUBSCRIBE` is slot-pinned
and needs one connection per distinct slot subscribed on. See
[pubsub.md](pubsub.md).

## Configuration

```ocaml
type Cluster_router.Config.t = {
  seeds : (string * int) list;
  connection : Connection.Config.t;
  agreement_ratio : float;         (* 0.75: 3-seed quorum needs 3, 4-seed needs 3, ... *)
  min_nodes_for_quorum : int;      (* 2: single-node cluster needs adjustment *)
  max_redirects : int;             (* 5: MOVED/ASK/Interrupted budget per command *)
  prefer_hostname : bool;          (* true in docker-compose; false in raw IP deploys *)
  refresh_interval : float;        (* 5.0: base refresh cadence (s) *)
  refresh_jitter : float;          (* 2.0: up to 2s random offset *)
}
```

Knobs you may need to tune:

- **`prefer_hostname`** — set `true` when the cluster announces
  hostnames (docker-compose, kube service). Set `false` when it
  announces raw IPs and your client resolves them directly.
- **`max_redirects`** — raise to 8-10 if you see spurious
  `CLUSTERDOWN` errors during large failovers; budget is
  exponential so 10 retries ≈ 6 s total wait.
- **`refresh_interval`** — lower to 1-2 s under frequent topology
  changes (active resharding), keep at 5 s in steady state.

Everything else has sensible defaults.
