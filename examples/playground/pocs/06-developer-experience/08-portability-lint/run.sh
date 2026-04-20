#!/usr/bin/env bash
# Arc 6 — portability lint (P001) + pragma + [portability] block
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

mkdir -p expected

echo "==> 1. Baseline compile — every model type-checks, no dialect gate yet"
rocky compile --models models > expected/compile_no_dialect.json 2>/dev/null

echo "==> 2. Compile with --target-dialect=bq — fires P001 on NVL"
rocky compile --models models --target-dialect bq \
    > expected/compile_target_bq.json 2>/dev/null \
    || true   # non-zero exit is expected when P001 fires at error severity

echo "==> 3. Extract P001 diagnostics from the BQ compile"
python3 - <<'PY'
import json
data = json.load(open("expected/compile_target_bq.json"))
diags = [d for d in (data.get("diagnostics") or []) if d.get("code") == "P001"]
print(f"  has_errors       : {data.get('has_errors')}")
print(f"  P001 diagnostics : {len(diags)}")
for d in diags:
    print(f"    - [{d['severity']}] {d['model']}: {d['message']}")
    print(f"         at {d['span']['file']}:{d['span']['line']}")
    if d.get("suggestion"):
        print(f"         suggestion: {d['suggestion']}")
PY

echo
echo "==> 4. Summary"
echo "  portable.sql              — portable; clean under every dialect"
echo "  non_portable_nvl.sql      — fires P001 under --target-dialect=bq"
echo "  suppressed_via_pragma.sql — same NVL, suppressed per-file via '-- rocky-allow: NVL'"
echo "  rocky.toml [portability]  — project-wide target + allowlist for 'QUALIFY'"
echo
echo "POC complete."
