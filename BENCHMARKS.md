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

## Results — localhost Valkey 9, 50k ops per scenario

| Scenario                      |       Ours |  ocaml-redis |    C (ref) | Ours / C | Ours / ocaml-redis |
|-------------------------------|-----------:|-------------:|-----------:|---------:|-------------------:|
| SET 100B   conc=1             |  6,821 r/s |    8,468 r/s |  8,775 r/s |     78 % |              0.81x |
| GET 100B   conc=1             |  7,245 r/s |    8,786 r/s |  8,627 r/s |     84 % |              0.82x |
| SET 100B   conc=10            | 51,440 r/s |   28,657 r/s | 62,735 r/s |     82 % |           **1.80x** |
| GET 100B   conc=10            | 60,010 r/s |   27,706 r/s | 59,382 r/s |   **101 %** |         **2.17x** |
| MIX 100B   conc=10            | 55,784 r/s |   29,834 r/s |          — |        — |           **1.87x** |
| SET 100B   conc=100           |189,942 r/s |   45,116 r/s |223,214 r/s |     85 % |           **4.21x** |
| GET 100B   conc=100           |195,039 r/s |   41,133 r/s |202,429 r/s |     96 % |           **4.74x** |
| MIX 100B   conc=100           |188,494 r/s |   32,699 r/s |          — |        — |           **5.76x** |
| SET 16KiB  conc=10            | 49,851 r/s |   26,325 r/s | 55,005 r/s |   **91 %** |         **1.89x** |
| GET 16KiB  conc=10            | 43,996 r/s |   24,060 r/s | 56,883 r/s |     77 % |           **1.83x** |
| MIX 1KiB   conc=100           |109,869 r/s |   46,808 r/s |          — |        — |           **2.35x** |

p50 latency stays under 1 ms for 100B payloads at every concurrency
tier. At conc=100 our p99 is ~1.1 ms vs ocaml-redis's 6-9 ms (the
head-of-line cost of blocking sockets + OS-thread scheduling).

## What the numbers say

**Single-fiber (conc=1):** ~80 % of ocaml-redis, ~80 % of C. Both
clients are RTT-bound and a typed API + Eio scheduler cost a small
constant per op.

**Medium concurrency (conc=10):** we pull 1.8-2.2x ahead by
multiplexing commands onto one TCP connection, where ocaml-redis
needs 10 threads and 10 connections to approach the same
throughput. GET conc=10 at 101 % of C is real: the C benchmark
opens 10 separate connections + handshakes, which we skip.

**High concurrency (conc=100):** the gap widens to **4-5x**.
At 100 concurrent fibers we come within 85-96 % of the pipelined
C-client ceiling; ocaml-redis tops out at ~40-45 k ops/s, bottle-
necked by OS-thread scheduling.

**Large payloads:** SET 16 KiB at **91 %** of C — within the typed-
OCaml-client noise band. This used to be 47 %; see the writer
commit below.

## Send-path optimization (commit history)

The first benchmark pass showed SET 16 KiB at only 47 % of C. The
culprit was the wire-encoder allocating through `Buffer.t`
[`Buffer.add_string` + `Buffer.contents`] before the Cstruct copy
that `Eio.Flow.copy_string` does internally — three touches of the
16 KiB payload per request.

The fix (`Resp3_writer.command_to_cstruct`):

1. Compute the exact encoded size from the args array in O(args).
2. Allocate one `Cstruct.t` at that size via `Cstruct.create_unsafe`.
3. `Cstruct.blit_from_string` each argument in once.
4. `Eio.Flow.write sink [cs]` — single writev on the socket.

One copy of the payload per request instead of two, and no wasted
intermediate string. SET 16 KiB rose from ~28 k r/s to ~50 k r/s,
bringing the ratio to C from 47 % to 91 %.

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
