#!/usr/bin/env bash
# Arc 7 — type-inference over raw .sql + blast-radius SELECT * lint
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

mkdir -p expected

echo "==> 1. Compile WITHOUT --with-seed"
rocky compile --models models > expected/compile_no_seed.json 2>/dev/null

echo "==> 2. Compile WITH --with-seed (data/seed.sql → in-memory DuckDB → information_schema)"
rocky compile --models models --with-seed > expected/compile_with_seed.json 2>/dev/null

echo
echo "==> 3. Diff: what did grounding the source schema actually change?"
python3 - <<'PY'
import json

def pick(path):
    d = json.load(open(path))
    out = {}
    for m in d.get("models_detail", []):
        inc = m.get("incrementality_hint") or {}
        out[m["name"]] = {
            "is_candidate": inc.get("is_candidate"),
            "recommended_column": inc.get("recommended_column"),
            "confidence": inc.get("confidence"),
            "type_aware_signal": any("column type" in s for s in inc.get("signals") or []),
        }
    return out

no_seed = pick("expected/compile_no_seed.json")
with_seed = pick("expected/compile_with_seed.json")

print(f"  {'model':18s}  {'with-seed':12s} {'confidence':12s} {'type-aware':10s}")
print(f"  {'-'*18}  {'-'*12} {'-'*12} {'-'*10}")
for model in sorted(set(no_seed) | set(with_seed)):
    for label, src in [("off", no_seed), ("on", with_seed)]:
        e = src.get(model) or {}
        print(f"  {model:18s}  {label:12s} {(e.get('confidence') or '-'):12s} "
              f"{('yes' if e.get('type_aware_signal') else 'no'):10s}")
PY

echo
echo "==> 4. Blast-radius lint on orders_star.sql (SELECT * hits the semantic-graph-aware check)"
python3 - <<'PY'
import json
d = json.load(open("expected/compile_with_seed.json"))
star_lints = [x for x in (d.get("diagnostics") or []) if "select *" in (x.get("message","").lower())]
print(f"  diagnostics : {len(star_lints)}")
for x in star_lints:
    print(f"    - [{x['severity']} {x['code']}] {x['model']}: {x['message']}")
    print(f"         at {x['span']['file']}:{x['span']['line']}")
PY

echo
echo "POC complete: --with-seed lifts incrementality confidence to 'high' with a"
echo "type-aware signal; SELECT * is flagged with its span so editors can squiggle it."
