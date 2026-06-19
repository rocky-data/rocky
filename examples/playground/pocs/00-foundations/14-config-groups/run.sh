#!/usr/bin/env bash
# 14-config-groups — governed materialization fan-out + the two load-time guards
#
# No `set -e`: the broken siblings are SUPPOSED to fail, and we assert on those
# failures. Unexpected failures are caught explicitly and exit non-zero.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

fail() { echo "ASSERT FAILED: $1" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Part 1 — the good fan-out: one group definition, three distinct schemas.
# ---------------------------------------------------------------------------
echo "=== Part 1: fan-out — three members of group 'daily_marts' ==="
echo "    schema_template = \"mart_{region}\"; each member fills {region} from [args]"
echo
rocky compile --models models --output json > expected/compile.json 2>/dev/null
rc=$?
[ "$rc" -eq 0 ] || fail "good fan-out compile exited $rc (expected 0)"

echo "Resolved member targets + inherited tags:"
python3 -c "
import json
d = json.load(open('expected/compile.json'))
for m in sorted(d.get('models_detail', []), key=lambda x: x['name']):
    t = m['target']
    tags = ','.join(f'{k}={v}' for k, v in sorted(m.get('tags', {}).items()))
    print(f\"  {m['name']:20s} -> {t['catalog']}.{t['schema']}.{t['table']:18s} [{tags}]\")
"

# Assert the three members resolved to THREE DISTINCT target schemas.
distinct_schemas=$(python3 -c "
import json
d = json.load(open('expected/compile.json'))
schemas = sorted({m['target']['schema'] for m in d['models_detail']})
print(' '.join(schemas))
")
echo
echo "Distinct schemas: $distinct_schemas"
[ "$distinct_schemas" = "mart_apac mart_emea mart_us_west" ] \
    || fail "expected schemas 'mart_apac mart_emea mart_us_west', got '$distinct_schemas'"

# Assert every member inherited the group's [tags] domain=sales baseline,
# and that the per-key override took (us_west tier=silver, others tier=gold).
python3 -c "
import json, sys
d = json.load(open('expected/compile.json'))
by = {m['name']: m.get('tags', {}) for m in d['models_detail']}
def need(model, key, val):
    got = by.get(model, {}).get(key)
    if got != val:
        sys.exit(f'tag {key}={val} expected on {model}, got {got!r}')
# domain=sales inherited from the group by every member:
for m in ('fct_orders_emea', 'fct_orders_us_west', 'fct_orders_apac'):
    need(m, 'domain', 'sales')
# tier: group baseline gold, except us_west which overrides to silver,
# while still keeping the inherited domain (per-key merge, sidecar > group):
need('fct_orders_emea',    'tier', 'gold')
need('fct_orders_apac',    'tier', 'gold')
need('fct_orders_us_west', 'tier', 'silver')
print('OK: group [tags] inherited by all members; us_west tier override merged per-key')
" || fail "group [tags] inheritance / override assertion failed"

# ---------------------------------------------------------------------------
# Part 2 — guard A: misplacement (pin target.schema AND supply [args]).
# ---------------------------------------------------------------------------
echo
echo "=== Part 2: load-time guard A — misplaced [args] (non-enforced group) ==="
echo "    broken/misplaced-args pins target.schema AND supplies [args] -> the"
echo "    pin bypasses the template, so the [args] are dead. Expect load FAILURE."
echo
if rocky compile \
        --models broken/misplaced-args/models \
        --output json > /dev/null 2> expected/err-misplaced.txt; then
    echo "--- stderr ---"; cat expected/err-misplaced.txt
    fail "broken/misplaced-args compiled successfully (expected a load error)"
fi
echo "    -> failed as expected. Error:"
msg=$(grep -m1 -E "the \[args\] are dead" expected/err-misplaced.txt) \
    || fail "expected misplacement error ('the [args] are dead') not found"
echo "      ${msg#Error: }"

# ---------------------------------------------------------------------------
# Part 3 — guard B: enforced group overridden (pin a group-controlled field).
# ---------------------------------------------------------------------------
echo
echo "=== Part 3: load-time guard B — overriding an ENFORCED group ==="
echo "    broken/enforce-override pins target.schema under an enforced group"
echo "    that OWNS the schema. Expect load FAILURE (governance guarantee)."
echo
if rocky compile \
        --models broken/enforce-override/models \
        --output json > /dev/null 2> expected/err-enforce.txt; then
    echo "--- stderr ---"; cat expected/err-enforce.txt
    fail "broken/enforce-override compiled successfully (expected a load error)"
fi
echo "    -> failed as expected. Error:"
msg=$(grep -m1 -E "which its enforced group 'regulated' controls" expected/err-enforce.txt) \
    || fail "expected enforcement error ('enforced group ... controls') not found"
echo "      ${msg#Error: }"

echo
echo "POC complete:"
echo "  - one config group fanned out to 3 distinct schemas (mart_emea / mart_us_west / mart_apac)"
echo "  - every member inherited the group [tags] baseline; us_west overrode tier per-key"
echo "  - misplacement guard rejected pin-schema + [args]"
echo "  - enforcement guard rejected overriding an enforced group's schema"
