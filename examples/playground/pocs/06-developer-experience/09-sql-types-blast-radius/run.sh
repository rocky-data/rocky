#!/usr/bin/env bash
# Arc 7 — type-inference over raw .sql + blast-radius SELECT * lint
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

mkdir -p expected

echo "==> 1. Compile WITHOUT --with-seed"
rocky compile --models models --contracts contracts > expected/compile_no_seed.json 2>/dev/null

echo "==> 2. Compile WITH --with-seed (data/seed.sql → in-memory DuckDB → information_schema)"
rocky compile --models models --contracts contracts --with-seed > expected/compile_with_seed.json 2>/dev/null

echo
echo "==> 3. Diff: what did grounding the source schema actually change?"
python3 - <<'PY2'
import json

def unchecked(path):
    d = json.load(open(path))
    # I003: a contract column declares a type, but the model's column type is
    # Unknown, so the contract type check cannot run.
    return sorted(
        x["message"].split("'")[1]
        for x in (d.get("diagnostics") or [])
        if x["code"] == "I003" and x["model"] == "orders_typed"
    )

no_seed = unchecked("expected/compile_no_seed.json")
with_seed = unchecked("expected/compile_with_seed.json")
print(f"  contract columns whose type could not be checked (I003):")
print(f"    without --with-seed : {len(no_seed)} {no_seed}")
print(f"    with    --with-seed : {len(with_seed)} {with_seed}")
if no_seed != ["amount", "order_id"] or with_seed:
    raise SystemExit(
        f"expected I003 for exactly amount and order_id without seed data and none with it; "
        f"got {no_seed} and {with_seed}"
    )
PY2

echo
echo "==> 4. SELECT * lint on orders_star.sql (leaf trips the always-on I001; P002 blast-radius stays quiet — no downstream consumer)"
python3 - <<'PY'
import json
d = json.load(open("expected/compile_with_seed.json"))
star_lints = [x for x in (d.get("diagnostics") or []) if "select *" in (x.get("message","").lower())]
print(f"  diagnostics : {len(star_lints)}")
for x in star_lints:
    print(f"    - [{x['severity']} {x['code']}] {x['model']}: {x['message']}")
    print(f"         at {x['span']['file']}:{x['span']['line']}")
codes = [(x["code"], x["model"]) for x in star_lints]
if codes != [("I001", "orders_star")]:
    raise SystemExit(f"expected exactly one I001 on orders_star; got {codes}")
if not (star_lints[0].get("span") or {}).get("file", "").endswith("models/orders_star.sql"):
    raise SystemExit("the I001 diagnostic must carry a span pointing at models/orders_star.sql")
if any(x["code"] == "P002" for x in (d.get("diagnostics") or [])):
    raise SystemExit("P002 must stay quiet: orders_star has no downstream consumer")
PY

echo
echo "POC complete: --with-seed resolves source column types, so the contract's"
echo "type check runs instead of reporting I003; SELECT * is flagged with its span."
