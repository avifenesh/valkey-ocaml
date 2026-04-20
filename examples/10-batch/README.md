# 10-batch — cluster-aware batches (atomic + scatter)

Three programs demonstrating the `Batch` primitive end-to-end.

| File | Demonstrates |
|---|---|
| [bulk.ml](bulk.ml) | `mset_cluster` / `mget_cluster` / `del_cluster` over 1000 keys; side-by-side with a per-key loop so the speedup is visible |
| [scatter.ml](scatter.ml) | heterogeneous non-atomic batch (SET / INCR / HSET / GET) across slots, showing the `batch_entry_result` sum |
| [atomic_counters.ml](atomic_counters.ml) | `~atomic:true` commit: SET NX + INCR + INCR + GET pinned to one slot via hashtag |

## Run

Requires the docker-compose cluster:

```bash
sudo bash scripts/cluster-hosts-setup.sh   # one-time
docker compose -f docker-compose.cluster.yml up -d

dune exec examples/10-batch/bulk.exe
dune exec examples/10-batch/scatter.exe
dune exec examples/10-batch/atomic_counters.exe
```

## Notes

- **`bulk.ml`** prints wall-clock time for each phase. Expected
  output on a local cluster:
  ```
  mset_cluster 1000 keys: 0.008s
  mget_cluster 1000 keys: 0.006s
    hits: 1000 / 1000
  per-key loop 1000 gets: 0.900s  (hits 1000)
  ```
  The per-key loop is ~100× slower because each call is its own
  round-trip. The batch path pipelines within each slot and runs
  all slot-pipelines in parallel.

- **`scatter.ml`** queues both single-target (GET/SET/INCR/HSET)
  and unwraps each `One` entry. Fan-out commands (e.g.
  `SCRIPT LOAD`) would surface as `Many`; they're rejected at
  queue time in atomic mode only.

- **`atomic_counters.ml`** uses the `{demo}` hashtag to pin the
  key to one slot — required for atomic mode. It demonstrates
  the commit path only; WATCH-based optimistic concurrency needs
  the eager `Transaction` module today — see
  [docs/batch.md](../../docs/batch.md#watch-caveat) for why.

## See also

- [docs/batch.md](../../docs/batch.md) — the conceptual model,
  atomic vs non-atomic, timeout semantics, fan-out handling.
- [docs/transactions.md](../../docs/transactions.md) — the eager
  `Transaction.t` API, which remains supported alongside atomic
  `Batch`.
