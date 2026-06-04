#!/usr/bin/env bash
# 09-cross-source-overlap — the same business key arriving via two sources.
set -euo pipefail

export ROCKY_SUPPRESS_DEPRECATION=1

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

mkdir -p expected
rm -f .rocky-state.redb .rocky-state.redb.lock poc.duckdb
rm -f models/.rocky-state.redb models/.rocky-state.redb.lock

echo "=== seed two sibling Shopify sources (us, eu) ==="
duckdb poc.duckdb < data/seed.sql

echo "=== discover (raw__<region>__<source>) ==="
rocky discover > expected/discover.json

echo "=== run (replicate both regions, then the detective checks) ==="
rocky run --filter source=shopify --output json > expected/run.json

echo
echo "=== cross_source_overlap — order_ids shared across the us/eu siblings ==="
python3 - expected/run.json <<'PY'
import json, sys
data = json.load(open(sys.argv[1]))
def walk(o):
    if isinstance(o, dict):
        if isinstance(o.get("name"), str) and o["name"].startswith("cross_source_overlap:"):
            yield o
        for v in o.values():
            yield from walk(v)
    elif isinstance(o, list):
        for v in o:
            yield from walk(v)
found = list(walk(data))
if not found:
    print("  (no cross_source_overlap result found)")
for c in found:
    print(f"  {c['name']}: passed={c['passed']} "
          f"overlap_count={c.get('overlap_count')} "
          f"contributing_tables={c.get('contributing_tables')} "
          f"sample={c.get('sample')}")
PY

echo
echo "=== unique_expr — derived (customer, day) key duplicated within a source ==="
python3 - expected/run.json <<'PY'
import json, sys
data = json.load(open(sys.argv[1]))
def walk(o):
    if isinstance(o, dict):
        if o.get("kind") == "unique_expr":
            yield o
        for v in o.values():
            yield from walk(v)
    elif isinstance(o, list):
        for v in o:
            yield from walk(v)
found = list(walk(data))
if not found:
    print("  (no unique_expr result found)")
for c in found:
    print(f"  {c['name']}: passed={c['passed']} failing_rows={c.get('failing_rows')}")
PY

echo
echo "POC complete: cross_source_overlap flagged the shared order_ids;"
echo "unique_expr flagged the derived-key dup. Both are advisory (severity=warning)."
