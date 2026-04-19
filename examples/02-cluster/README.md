# 02-cluster — cluster routing + TLS

Two programs:

| File | Demonstrates |
|---|---|
| [routing.ml](routing.ml) | All four `Read_from` modes side by side, with per-node tally |
| [tls.ml](tls.ml) | TLS template — managed-service (system CAs) + internal CA paths |

## routing.ml

Run against the included docker-compose cluster:

```bash
sudo bash scripts/cluster-hosts-setup.sh   # one-time
docker compose -f docker-compose.cluster.yml up -d
dune exec examples/02-cluster/routing.exe
```

Probes `CLUSTER MYID` 12 times under each routing mode and tallies
which node actually served the request. Expected output for a
3-primary / 3-replica cluster:

```
[Primary] for slot 0, 12 probes:
  12  <primary id>

[Prefer_replica] for slot 0, 12 probes:
  ~6  <replica A id>     ← split across replicas because pick_node_by_read_from
  ~6  <replica B id>        randomises across the candidate set
```

`Prefer_replica` is randomised per call (see
[`lib/cluster_router.ml`](../../lib/cluster_router.ml)
`pick_random`), so over many calls the distribution spreads
across replicas.

### AZ-affinity fallback chain

`Az_affinity { az }` and `Az_affinity_replicas_and_primary { az }`
have a 3-tier fallback:

1. Pick a replica in the requested AZ.
2. If none, pick any other replica.
3. If no replicas at all, the primary.

The docker-compose cluster doesn't set `availability_zone` on its
nodes (no `--availability-zone` flag in `docker-compose.cluster.yml`),
so tier 1 always returns empty and the example demonstrates tier
2 — the distribution should match `Prefer_replica`.

To exercise tier 1 against a real multi-AZ cluster (e.g. AWS
ElastiCache), pass the AZ string the cluster reports — visible
via `CLUSTER SHARDS` on the `availability-zone` field of each
node.

## tls.ml

Template only — won't connect anywhere unless you fill in the
host / port. Two patterns:

```ocaml
(* Hosted service: system trust store. *)
let tls = TLS.with_system_cas ~server_name:"my.cache.amazonaws.com" ()

(* Internal CA: load a PEM file. *)
let tls = TLS.with_ca_cert ~server_name:"valkey-c1" ~ca_pem ()
```

For dev TLS, generate a self-signed CA with
`scripts/gen-tls-certs.sh` and point `with_ca_cert` at the
resulting `.dev-certs/ca.pem`. See [docs/tls.md](../../docs/tls.md)
for the full picture.
