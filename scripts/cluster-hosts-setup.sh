#!/usr/bin/env bash
# Add /etc/hosts entries so the cluster integration test (running on
# the docker *host*, i.e. this WSL shell) can reach the same hostnames
# that CLUSTER SHARDS announces from inside the container network.
#
# Each valkey-cN hostname resolves to 127.0.0.1; Docker Desktop's
# port-forwarding catches the per-node port and routes to the right
# container.
#
# Safe to re-run: only inserts missing entries.

set -euo pipefail

ENTRIES="127.0.0.1  valkey-c1 valkey-c2 valkey-c3 valkey-c4 valkey-c5 valkey-c6"
MARKER="# ocaml-valkey cluster (docker-compose.cluster.yml)"

if grep -Fq "$MARKER" /etc/hosts; then
  echo "/etc/hosts already has ocaml-valkey cluster entries; nothing to do."
  exit 0
fi

echo "Adding cluster hostnames to /etc/hosts (requires sudo)."
printf '%s\n%s\n' "$MARKER" "$ENTRIES" | sudo tee -a /etc/hosts >/dev/null
echo "Done. Entries added:"
grep -A1 "$MARKER" /etc/hosts
