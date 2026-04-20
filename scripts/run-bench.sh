#!/usr/bin/env bash
# Run all three benchmarks (ours, ocaml-redis, valkey-benchmark C)
# against the same Valkey instance and print them in sequence so
# numbers can be compared side by side.
#
# Assumes standalone Valkey on localhost:6379 (docker compose up).

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

HOST="${HOST:-localhost}"
PORT="${PORT:-6379}"
OPS="${OPS:-50000}"
WARMUP="${WARMUP:-1000}"
KEYS="${KEYS:-10000}"

printf '\n\033[1;36m# =============================================\n'
printf '# valkey-ocaml   : %s operations per scenario\n' "$OPS"
printf '# host           : %s:%s\n' "$HOST" "$PORT"
printf '# =============================================\033[0m\n\n'

eval "$(opam env --switch=5.3.0)"
valkey-cli -h "$HOST" -p "$PORT" FLUSHDB >/dev/null

printf '\033[1;32m>>> ours (Valkey.Client, Eio multiplex, RESP3)\033[0m\n'
dune exec bin/bench/bench.exe -- \
  --host "$HOST" --port "$PORT" \
  --ops "$OPS" --warmup "$WARMUP" --keys "$KEYS"

printf '\n\033[1;32m>>> ocaml-redis (redis-sync, thread-per-conn, RESP2)\033[0m\n'
WITH_BENCH_REDIS=1 dune exec bin/bench_redis/bench_redis.exe -- \
  --host "$HOST" --port "$PORT" \
  --ops "$OPS" --warmup "$WARMUP" --keys "$KEYS"

# valkey-benchmark: the C client, pipelined + threaded. A reasonable
# ceiling for "how fast can the server + network handle this".
printf '\n\033[1;32m>>> valkey-benchmark (C, reference ceiling)\033[0m\n'
run_vb () {
  local t="$1"      # SET or GET
  local d="$2"      # payload bytes
  local c="$3"      # client count
  local label
  label="$(printf '%-13s size=%-5d conc=%-3d' "$t" "$d" "$c")"
  # valkey-benchmark writes progress updates using \r; split on \r
  # and keep only the final summary line ("TYPE: RPS req/s, p50=...").
  local out
  out="$(valkey-benchmark -h "$HOST" -p "$PORT" \
           -t "$t" -d "$d" -n "$OPS" -c "$c" -q 2>/dev/null \
         | tr '\r' '\n' | awk 'NF' | tail -1)"
  printf '%s  %s\n' "$label" "$out"
}

run_vb SET 100    1
run_vb GET 100    1
run_vb SET 100   10
run_vb GET 100   10
run_vb SET 100  100
run_vb GET 100  100
run_vb SET 16384 10
run_vb GET 16384 10

printf '\n\033[1;36m# done\033[0m\n'
