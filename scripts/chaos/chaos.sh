#!/usr/bin/env bash
# TCP-chaos driver for soak / fuzz runs.
#
# Talks to a running toxiproxy (docker-compose.toxiproxy.yml) over
# its HTTP API on 127.0.0.1:8474. Keeps a stable upstream naming:
#
#   valkey-dev    -> localhost:26379 -> localhost:6379
#   valkey-c1     -> localhost:27000 -> localhost:7000
#   valkey-c2     -> localhost:27001 -> localhost:7001
#   valkey-c3     -> localhost:27002 -> localhost:7002
#   valkey-c4     -> localhost:27003 -> localhost:7003
#   valkey-c5     -> localhost:27004 -> localhost:7004
#   valkey-c6     -> localhost:27005 -> localhost:7005
#
# Client code under chaos dials the :2700x ports (for cluster) or
# :26379 (for standalone) and gets the same service with whatever
# TCP-layer misbehavior is currently installed.
#
# Subcommands:
#   setup                     — create / replace all proxies
#   list                      — show configured proxies
#   latency <ms> [jitter_ms]  — add/replace latency_down+up toxic
#   loss <percent>            — add/replace slicer toxic (drop bytes)
#   bandwidth <kbps>          — cap throughput
#   reset                     — tcp-reset on all connections every N ms
#   close                     — close every new connection immediately
#   clear                     — remove every toxic, keep proxies
#   teardown                  — delete every proxy
#
# Dependencies: bash, curl, jq (optional, only for pretty-print).
set -euo pipefail

API="${TOXIPROXY_API:-http://127.0.0.1:8474}"

# Mapping of upstream names to (listen_port, upstream_host, upstream_port).
proxies=(
  "valkey-dev:26379:host.docker.internal:6379"
  "valkey-c1:27000:host.docker.internal:7000"
  "valkey-c2:27001:host.docker.internal:7001"
  "valkey-c3:27002:host.docker.internal:7002"
  "valkey-c4:27003:host.docker.internal:7003"
  "valkey-c5:27004:host.docker.internal:7004"
  "valkey-c6:27005:host.docker.internal:7005"
)

wait_for_api () {
  for _ in $(seq 1 30); do
    if curl -sf "$API/version" >/dev/null 2>&1; then return 0; fi
    sleep 0.5
  done
  echo "toxiproxy API $API not reachable — is docker-compose.toxiproxy.yml up?" >&2
  exit 1
}

setup () {
  wait_for_api
  # Replace-or-create each proxy. Toxiproxy has no "upsert" so we
  # delete first (404 ignored) and then create.
  for entry in "${proxies[@]}"; do
    IFS=':' read -r name listen_port upstream_host upstream_port <<<"$entry"
    curl -sf -X DELETE "$API/proxies/$name" >/dev/null 2>&1 || true
    curl -sf -X POST "$API/proxies" \
      -H 'Content-Type: application/json' \
      -d "{\"name\":\"$name\",\"listen\":\"0.0.0.0:$listen_port\",\"upstream\":\"$upstream_host:$upstream_port\",\"enabled\":true}" \
      >/dev/null
    echo "  + $name  :$listen_port -> $upstream_host:$upstream_port"
  done
}

list () {
  wait_for_api
  if command -v jq >/dev/null 2>&1; then
    curl -sf "$API/proxies" | jq .
  else
    curl -sf "$API/proxies"
    echo
  fi
}

for_each_proxy () {
  local fn=$1; shift
  wait_for_api
  for entry in "${proxies[@]}"; do
    IFS=':' read -r name _ _ _ <<<"$entry"
    "$fn" "$name" "$@"
  done
}

add_toxic () {
  local name=$1 tox_name=$2 tox_type=$3 payload=$4
  curl -sf -X DELETE "$API/proxies/$name/toxics/$tox_name" >/dev/null 2>&1 || true
  curl -sf -X POST "$API/proxies/$name/toxics" \
    -H 'Content-Type: application/json' \
    -d "$payload" >/dev/null
  echo "  $name: $tox_name ($tox_type)"
}

latency () {
  local ms=${1:?usage: latency <ms> [jitter_ms]}
  local jit=${2:-0}
  _apply_latency () {
    add_toxic "$1" "lat_down" "latency" \
      "{\"name\":\"lat_down\",\"type\":\"latency\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":{\"latency\":$ms,\"jitter\":$jit}}"
    add_toxic "$1" "lat_up"   "latency" \
      "{\"name\":\"lat_up\",\"type\":\"latency\",\"stream\":\"upstream\",\"toxicity\":1.0,\"attributes\":{\"latency\":$ms,\"jitter\":$jit}}"
  }
  for_each_proxy _apply_latency
}

loss () {
  local pct=${1:?usage: loss <percent>}
  # Toxiproxy does not have a byte-drop toxic, but "slicer" chops
  # packets at random offsets which has a similar effect at the
  # application layer.
  local frac=$(awk "BEGIN { printf \"%.4f\", $pct / 100.0 }")
  _apply_loss () {
    add_toxic "$1" "slicer_down" "slicer" \
      "{\"name\":\"slicer_down\",\"type\":\"slicer\",\"stream\":\"downstream\",\"toxicity\":$frac,\"attributes\":{\"average_size\":64,\"size_variation\":16,\"delay\":10}}"
  }
  for_each_proxy _apply_loss
}

bandwidth () {
  local kbps=${1:?usage: bandwidth <kbps>}
  _apply_bw () {
    add_toxic "$1" "bw_down" "bandwidth" \
      "{\"name\":\"bw_down\",\"type\":\"bandwidth\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":{\"rate\":$kbps}}"
    add_toxic "$1" "bw_up"   "bandwidth" \
      "{\"name\":\"bw_up\",\"type\":\"bandwidth\",\"stream\":\"upstream\",\"toxicity\":1.0,\"attributes\":{\"rate\":$kbps}}"
  }
  for_each_proxy _apply_bw
}

reset () {
  _apply_reset () {
    add_toxic "$1" "reset_peer_down" "reset_peer" \
      "{\"name\":\"reset_peer_down\",\"type\":\"reset_peer\",\"stream\":\"downstream\",\"toxicity\":0.05,\"attributes\":{\"timeout\":100}}"
  }
  for_each_proxy _apply_reset
}

close_cmd () {
  _apply_close () {
    add_toxic "$1" "limit_data" "limit_data" \
      "{\"name\":\"limit_data\",\"type\":\"limit_data\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":{\"bytes\":0}}"
  }
  for_each_proxy _apply_close
}

clear_cmd () {
  _clear () {
    local name=$1
    local toxics
    toxics=$(curl -sf "$API/proxies/$name/toxics" | \
             python3 -c 'import sys,json; [print(t["name"]) for t in json.load(sys.stdin)]')
    for t in $toxics; do
      curl -sf -X DELETE "$API/proxies/$name/toxics/$t" >/dev/null
      echo "  - $name: $t"
    done
  }
  for_each_proxy _clear
}

teardown () {
  wait_for_api
  for entry in "${proxies[@]}"; do
    IFS=':' read -r name _ _ _ <<<"$entry"
    curl -sf -X DELETE "$API/proxies/$name" >/dev/null 2>&1 || true
    echo "  - $name"
  done
}

case "${1:-}" in
  setup)     setup ;;
  list)      list ;;
  latency)   shift; latency "$@" ;;
  loss)      shift; loss "$@" ;;
  bandwidth) shift; bandwidth "$@" ;;
  reset)     reset ;;
  close)     close_cmd ;;
  clear)     clear_cmd ;;
  teardown)  teardown ;;
  *)
    echo "usage: $0 {setup|list|latency MS [JIT]|loss PCT|bandwidth KBPS|reset|close|clear|teardown}"
    exit 2
    ;;
esac
