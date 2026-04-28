#!/usr/bin/env bash
# Pivots the per-run JSON files into a comparison table:
# one row per (scenario) × columns (N=1, N=2, N=4) for each
# (set, tls) combination. Computes the ops/sec ratio vs N=1.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
DIR="${1:-bench-results-2026-04-28}"

# Rely on python for JSON + table rendering; already used across
# scripts/*. Falls back to printing raw summary lines.
python3 - "$DIR" <<'PY'
import json, os, sys
from collections import defaultdict

d = sys.argv[1]
# key: (set, tls) -> scenario_name -> N -> ops/sec
rows = defaultdict(lambda: defaultdict(dict))

for fn in sorted(os.listdir(d)):
    if not fn.endswith(".json"):
        continue
    # parse "core_N1_plain.json" -> (core, 1, plain)
    stem = fn[:-5]
    parts = stem.split("_")
    if len(parts) != 3: continue
    sset, nstr, tls = parts
    n = int(nstr[1:])
    with open(os.path.join(d, fn)) as f:
        data = json.load(f)
    for s in data["scenarios"]:
        rows[(sset, tls)][s["name"]][n] = s["ops_per_sec"]

for (sset, tls), by_scen in rows.items():
    print(f"\n=== set={sset} tls={tls} ===")
    print(f"{'scenario':<38}  {'N=1':>10}  {'N=2':>10}  {'N=4':>10}  "
          f"{'N2/N1':>6}  {'N4/N1':>6}")
    print("-" * 96)
    # Preserve order of first file
    any_file = next(fn for fn in sorted(os.listdir(d))
                    if fn.startswith(f"{sset}_N1_{tls}"))
    with open(os.path.join(d, any_file)) as f:
        order = [s["name"] for s in json.load(f)["scenarios"]]
    for name in order:
        ns = by_scen.get(name, {})
        n1 = ns.get(1, 0.0)
        n2 = ns.get(2, 0.0)
        n4 = ns.get(4, 0.0)
        r21 = n2/n1 if n1 else 0.0
        r41 = n4/n1 if n1 else 0.0
        print(f"{name:<38}  {n1:>10.0f}  {n2:>10.0f}  {n4:>10.0f}  "
              f"{r21:>6.2f}  {r41:>6.2f}")
PY
