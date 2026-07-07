#!/usr/bin/env bash
# 10-recipe-provenance — "what produced this?" Every materialization records a
# recipe-identity triple (recipe_hash, input_hash, env_hash). Two executions of
# the same model share one recipe_hash; `rocky history --recipe <hash>` returns
# every execution of that exact program.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Use the freshly-built engine binary by default (a bare `rocky` on PATH is
# often a stale install that predates the recipe-identity triple / --recipe).
ROCKY_BIN="${ROCKY_BIN:-$(git rev-parse --show-toplevel)/engine/target/release/rocky}"

mkdir -p expected
# Pin one state file so both runs record into the same ledger the --recipe
# query then reads.
STATE="$HERE/expected/state.redb"
rm -f "$STATE" "$STATE.lock" poc.duckdb
rm -f models/.rocky-state.redb models/.rocky-state.redb.lock

rocky() { "$ROCKY_BIN" --state-path "$STATE" -c rocky.toml "$@"; }

echo "=== seed the raw source ==="
duckdb poc.duckdb < data/seed.sql

# ---------------------------------------------------------------------------
# Execution #1 — materialize orders_clean. This stamps the recipe-identity
# triple onto the run's ModelExecution record.
# ---------------------------------------------------------------------------
echo
echo "=== execution #1 — materialize orders_clean ==="
rocky -o json run --skip-unchanged > expected/run1.json

# ---------------------------------------------------------------------------
# Append rows, then execution #2 — the SQL is unchanged, so the recipe_hash is
# identical; only the input differs. Two executions of one exact program.
# ---------------------------------------------------------------------------
echo "=== append 50 orders, then execution #2 (same program, new input) ==="
duckdb poc.duckdb "INSERT INTO raw__orders.orders
    SELECT 200 + i, 1 + (i % 25), 'complete', (i * 91) % 90000 + 500,
           TIMESTAMP '2026-05-01' + INTERVAL (i * 3600) SECOND
    FROM generate_series(1, 50) AS t(i);"
rocky -o json run --skip-unchanged > expected/run2.json

# ---------------------------------------------------------------------------
# The triple is on every model record. Read the recipe_hash off the history.
# ---------------------------------------------------------------------------
echo
echo "=== the recipe-identity triple on orders_clean (from rocky history) ==="
rocky -o json history > expected/history.json
RECIPE_HASH=$(python3 -c "
import json
h = json.load(open('expected/history.json'))
for run in h.get('runs', []):
    for m in run.get('models', []):
        ri = m.get('recipe_identity')
        if ri and ri.get('recipe_hash'):
            print(ri['recipe_hash']); raise SystemExit
")
python3 -c "
import json
h = json.load(open('expected/history.json'))
for run in h.get('runs', []):
    for m in run.get('models', []):
        ri = m.get('recipe_identity')
        if ri and ri.get('recipe_hash'):
            print(f\"    model={m.get('model_name')}\")
            print(f\"    recipe_hash      = {ri['recipe_hash']}\")
            print(f\"    input_hash       = {ri.get('input_hash')}  (proof_class={ri.get('input_proof_class')})\")
            print(f\"    env_hash         = {ri.get('env_hash')}\")
            print(f\"    hash_scheme      = {ri.get('hash_scheme')}\")
            raise SystemExit
"

echo
echo "=== what produced this? rocky history --recipe <hash> ==="
echo "    recipe_hash = ${RECIPE_HASH}"
rocky -o json history --recipe "$RECIPE_HASH" > expected/recipe_history.json
COUNT=$(python3 -c "import json; print(json.load(open('expected/recipe_history.json')).get('count', 0))")
echo "    executions of this exact program: ${COUNT}"
python3 -c "
import json
d = json.load(open('expected/recipe_history.json'))
for e in d.get('executions', []):
    print(f\"      run={e.get('run_id')}  model={e.get('model_name')}  status={e.get('status')}\")
"

# ---------------------------------------------------------------------------
# Assertions — both executions of the same program are returned by --recipe.
# ---------------------------------------------------------------------------
[ -n "$RECIPE_HASH" ] || { echo "FAIL: no recipe_hash recorded on the model" >&2; exit 1; }
[ "$COUNT" = "2" ] || { echo "FAIL: expected 2 executions of the recipe, got $COUNT" >&2; exit 1; }

echo
echo "POC complete: one recipe_hash, two executions — 'what produced this?' answered."
