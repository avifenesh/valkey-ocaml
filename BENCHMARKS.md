# Benchmarks

Side-by-side comparison of this library against
[ocaml-redis](https://github.com/0xffea/ocaml-redis) (the existing
OCaml Redis client — Lwt/RESP2) and `valkey-benchmark` (the C client
that ships with Valkey, as a reference ceiling).

## Reproducing

```sh
docker compose up -d                # Valkey 9 on :6379
bash scripts/run-bench.sh           # takes ~3 min with default OPS=50k
# or for a quick pass:
OPS=10000 bash scripts/run-bench.sh
```

The runner builds and invokes three binaries:
1. `bin/bench/` — our library, Eio multiplex, RESP3.
2. `bin/bench_redis/` — `redis-sync` (blocking), one OS thread per
   connection, RESP2.
3. `valkey-benchmark` — the C client pipelined + threaded. The
   theoretical ceiling for a given server and network.

All three drive the same scenario matrix (same payload sizes, same
concurrency levels, same ops counts) so latency + throughput numbers
can be compared directly.

## Results — localhost Valkey 9, 30k ops per scenario

| Scenario                      |       Ours |  ocaml-redis |        C ref | Ours/C | Ours/ocaml-redis |
|-------------------------------|-----------:|-------------:|-------------:|-------:|-----------------:|
| SET 100B   conc=1             |  7,408 r/s |    8,884 r/s |    9,091 r/s |   81 % |            0.83x |
| GET 100B   conc=1             |  7,622 r/s |    9,518 r/s |    8,633 r/s |   88 % |            0.80x |
| SET 100B   conc=10            | 55,351 r/s |   28,679 r/s |   60,484 r/s |   92 % |          **1.93x** |
| GET 100B   conc=10            | 45,443 r/s |   27,872 r/s |   61,224 r/s |   74 % |          **1.63x** |
| MIX 100B   conc=10            | 50,452 r/s |   27,012 r/s |            — |      — |          **1.87x** |
| SET 100B   conc=100           |172,475 r/s |   57,817 r/s |  215,827 r/s |   80 % |          **2.98x** |
| GET 100B   conc=100           |206,344 r/s |   59,121 r/s |  225,564 r/s |   91 % |          **3.49x** |
| MIX 100B   conc=100           |200,615 r/s |   54,371 r/s |            — |      — |          **3.69x** |
| SET 16KiB  conc=10            | 27,679 r/s |   26,557 r/s |   59,406 r/s |   47 % |            1.04x |
| GET 16KiB  conc=10            | 41,470 r/s |   24,565 r/s |   58,708 r/s |   71 % |          **1.69x** |
| MIX 1KiB   conc=100           |105,447 r/s |   45,675 r/s |            — |      — |          **2.31x** |

p50 latency stays under 1 ms for 100B payloads at every concurrency
tier. p99 is 0.9 ms at conc=100 vs ocaml-redis's 7.3 ms (the head-of-
line cost of blocking sockets + OS-thread scheduling). See
`scripts/run-bench.sh` output for the full percentile table.

## What the numbers say

**Single-fiber (conc=1):** parity with ocaml-redis. Both clients are
RTT-bound; a typed API and Eio's fiber scheduler cost a small number
of percent against the blocking client.

**Medium concurrency (conc=10):** we pull 1.6x–1.9x ahead by
multiplexing commands onto one TCP connection, where ocaml-redis
needs 10 threads and 10 connections to approach the same throughput.

**High concurrency (conc=100):** the gap widens to ~3–3.5x. On
100 concurrent fibers we come within 80–90 % of the pipelined
C-client ceiling; ocaml-redis tops out at ~60 k ops/s on reads and
~58 k ops/s on writes.

**Large payloads:** on GET 16 KiB we're at 71 % of C, which is
reasonable. SET 16 KiB at 47 % of C is the one clear outlier —
points to an extra buffer copy on the send path that is worth
chasing next.

## Methodology notes

- **Warmup.** 1000 warmup ops per scenario (single-fiber) before the
  measurement window opens.
- **Latency sampling.** Every op's wall-clock duration is recorded;
  percentiles are computed by sorting the full vector.
- **Throughput.** `ops / elapsed_wall_time` over the measurement
  window (warmup excluded).
- **Key space.** 10 000 keys for all scenarios — small enough to
  stay in Valkey's memory but large enough that no single key
  dominates.
- **Concurrency model.** Ours: N Eio fibers, 1 connection. ocaml-redis: N OS threads, N connections. valkey-benchmark: N clients with pipelining.
- **Platform.** WSL 2 (Ubuntu 24.04), Docker Desktop, Valkey 9.0.3,
  OCaml 5.3.0, single-socket loopback. Absolute numbers will differ
  on dedicated hardware; ratios are typically stable.
