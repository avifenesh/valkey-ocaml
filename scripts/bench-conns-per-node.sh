#!/usr/bin/env bash
# Benchmark matrix for the [connections_per_node] knob.
#
# Sweeps N in {1, 2, 4} across three scenario sets:
#   - core       : small SET/GET at varied concurrency, baseline
#   - large      : 1 KiB .. 1 MiB values, mixed small+large
#   - server-cpu : EVAL spin loops and SORT_RO
#
# Also runs {core, large} under --tls so the TLS single-stream
# hypothesis gets evidence.
#
# Emits one JSON per (set, N, tls?) combination into
# bench-results-2026-04-28/ and prints the table to stdout.
#
# Usage:
#   bash scripts/bench-conns-per-node.sh
#   OPS=50000 bash scripts/bench-conns-per-node.sh

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

OPS="${OPS:-30000}"
WARMUP="${WARMUP:-2000}"
KEYS="${KEYS:-10000}"
OUT_DIR="${OUT_DIR:-bench-results-2026-04-28}"

mkdir -p "$OUT_DIR"

eval "$(opam env --switch=5.3.0 2>/dev/null || true)"

dune build bin/bench/bench.exe

run_one () {
  local set="$1" conns="$2" tls="$3"
  local port=6379 tls_flag=""
  if [[ "$tls" == "tls" ]]; then
    port=6390
    tls_flag="--tls"
  fi
  local tag="${set}_N${conns}_${tls}"
  local json="${OUT_DIR}/${tag}.json"
  printf '\n\033[1;32m>>> set=%s N=%d tls=%s\033[0m\n' "$set" "$conns" "$tls"
  dune exec bin/bench/bench.exe -- \
    --host localhost --port "$port" $tls_flag \
    --conns "$conns" --scenario-set "$set" \
    --ops "$OPS" --warmup "$WARMUP" --keys "$KEYS" \
    --json "$json" \
    | tee "${OUT_DIR}/${tag}.log"
}

# Core: plain and TLS
for conns in 1 2 4; do
  run_one core "$conns" plain
done
for conns in 1 2 4; do
  run_one core "$conns" tls
done

# Large: plain and TLS
for conns in 1 2 4; do
  run_one large "$conns" plain
done
for conns in 1 2 4; do
  run_one large "$conns" tls
done

# Server-CPU: plain only (CPU cost is server-side, TLS won't
# change the hypothesis shape)
for conns in 1 2 4; do
  run_one server-cpu "$conns" plain
done

printf '\n\033[1;36mAll runs complete. Results in %s/\033[0m\n' "$OUT_DIR"
ls -la "$OUT_DIR"
