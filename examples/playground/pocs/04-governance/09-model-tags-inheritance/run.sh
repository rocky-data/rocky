#!/usr/bin/env bash
# 09-model-tags-inheritance — governance tags: group baseline + per-key override
#
# Compiles two members of the `finance` config group and asserts each model's
# resolved governance tags (models_detail[].tags):
#   * fct_revenue  -> domain=finance, tier=gold    (overrides tier, inherits domain)
#   * dim_account  -> domain=finance, tier=silver  (inherits the full baseline)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

mkdir -p expected
rm -f .rocky-state.redb poc.duckdb

fail() { echo "ASSERT FAILED: $1" >&2; exit 1; }

echo "=== Config group 'finance' declares a [tags] baseline: domain=finance, tier=silver ==="
echo "    fct_revenue overrides tier=gold (keeps inherited domain); dim_account inherits both."
echo

rocky compile --models models --output json > expected/compile.json 2>/dev/null

echo "Resolved governance tags per model (models_detail[].tags):"
# House convention: assert with python3, not jq (jq may not be on the PATH).
# The jq form would be:  jq -r '.models_detail[] | "\(.name) \(.tags)"' expected/compile.json
python3 -c "
import json
d = json.load(open('expected/compile.json'))
for m in sorted(d.get('models_detail', []), key=lambda x: x['name']):
    tags = ', '.join(f'{k}={v}' for k, v in sorted(m.get('tags', {}).items()))
    print(f'  {m[\"name\"]:14s} -> {{{tags}}}')
"
echo

# Assert the inherited + overridden tags appear per model.
python3 -c "
import json, sys
d = json.load(open('expected/compile.json'))
by = {m['name']: m.get('tags', {}) for m in d['models_detail']}

def need(model, key, val):
    got = by.get(model, {}).get(key)
    if got != val:
        sys.exit(f'tag {key}={val} expected on {model}, got {got!r}')

# Member 1: overrides tier=gold, still inherits domain=finance (per-key merge).
need('fct_revenue', 'domain', 'finance')
need('fct_revenue', 'tier',   'gold')
# Member 2: inherits the full baseline, overrides nothing.
need('dim_account', 'domain', 'finance')
need('dim_account', 'tier',   'silver')

# The override is per KEY, not whole-set: fct_revenue keeps exactly two tags.
if sorted(by['fct_revenue']) != ['domain', 'tier']:
    sys.exit(f'fct_revenue should keep domain+tier, got {sorted(by[\"fct_revenue\"])}')

print('OK: group [tags] baseline inherited; fct_revenue overrode tier per-key (domain stayed inherited)')
" || fail "tag inheritance / per-key override assertion failed"

echo
echo "POC complete:"
echo "  - config group 'finance' declared the baseline domain=finance, tier=silver once"
echo "  - fct_revenue inherited domain=finance and overrode tier -> gold (per-key merge)"
echo "  - dim_account inherited the full baseline -> domain=finance, tier=silver"
echo "  - dagster-rocky projects these onto each derived asset's Dagster tags"
