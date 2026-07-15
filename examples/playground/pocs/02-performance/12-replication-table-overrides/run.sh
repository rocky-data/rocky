#!/usr/bin/env bash
# 12-replication-table-overrides — per-table most-specific-match-wins override demo
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Clean state from previous runs
rm -f poc.duckdb .rocky-state.redb .rocky-state.redb.lock
rm -f models/.rocky-state.redb models/.rocky-state.redb.lock
mkdir -p expected

echo "=== 12-replication-table-overrides — [[table_overrides]] POC ==="
echo ""
echo "Sources: raw__orders (orders, order_items, _diagnostics_sync)"
echo "         raw__events (events, user_01, user_02)"
echo ""

echo "=== seed — populate two source schemas in DuckDB ==="
duckdb poc.duckdb < data/seed.sql
echo "  raw__orders: orders (50 rows), order_items (100 rows), _diagnostics_sync (5 rows)"
echo "  raw__events: events (80 rows), user_01 (1 row), user_02 (1 row)"

echo ""
echo "=== validate — 5 [[table_overrides]] rules parse correctly ==="
rocky validate --output json > expected/validate.json
python3 -c "
import json, sys
d = json.load(open('expected/validate.json'))
msgs = [m for m in d['messages'] if m['severity'] in ('ok', 'error')]
for m in msgs:
    print(f'  {m[\"severity\"]:>5}  {m[\"code\"]}  {m[\"message\"]}')
"

echo ""
echo "=== discover — both source schemas found ==="
rocky discover --output json 2>/dev/null > expected/discover.json
python3 -c "
import json
d = json.load(open('expected/discover.json'))
for s in d['sources']:
    tables = ', '.join(t['name'] for t in s['tables'])
    print(f'  {s[\"id\"]:20}  tables: {tables}')
"

echo ""
echo "=== run #1 — first materialization (full_refresh bootstrap for all tables) ==="
rocky run --output json 2>/dev/null > expected/run1.json
python3 -c "
import json
d = json.load(open('expected/run1.json'))
print(f'  tables_copied: {d[\"tables_copied\"]}')
print(f'  excluded (table_override_disabled):')
for t in d['excluded_tables']:
    print(f'    {t[\"source_schema\"]}.{t[\"table_name\"]}  reason={t[\"reason\"]}')
print(f'  materializations (first run -> full_refresh bootstrap):')
for m in d['materializations']:
    tbl = m['asset_key'][-1]
    strat = m['metadata']['strategy']
    print(f'    {tbl:20}  strategy={strat}')
"

echo ""
echo "  NOTE: _diagnostics_sync excluded by Rule 3 (glob _diagnostics_*)"
echo "        user_01, user_02 excluded by Rule 4 (glob user_?? in raw__events)"

echo ""
echo "=== run #2 — watermarks set; per-table overrides kick in ==="
rocky run --output json 2>/dev/null > expected/run2.json
python3 -c "
import json
d = json.load(open('expected/run2.json'))
print(f'  tables_copied: {d[\"tables_copied\"]}')
print(f'  per-table effective strategy (post-override):')
for m in d['materializations']:
    tbl = m['asset_key'][-1]
    schema = m['asset_key'][-2] if len(m['asset_key']) > 2 else '?'
    strat = m['metadata']['strategy']
    print(f'    {schema}.{tbl:20}  strategy={strat}')
"

echo ""
echo "  Rule 2 effect: orders     -> incremental (timestamp_column=ordered_at)"
echo "  Rule 5 effect: events     -> incremental (timestamp_column=occurred_at)"
echo "  Default:       order_items -> full_refresh (no strategy override)"

echo ""
echo "=== --filter table=orders — replicate only the orders table across all connectors ==="
rocky run --filter table=orders --output json 2>/dev/null > expected/run_filter_orders.json
python3 -c "
import json
d = json.load(open('expected/run_filter_orders.json'))
print(f'  tables_copied: {d[\"tables_copied\"]}  (orders only, across all connectors)')
for m in d['materializations']:
    tbl = m['asset_key'][-1]
    strat = m['metadata']['strategy']
    print(f'  {tbl:20}  strategy={strat}')
"

echo ""
echo "=== row counts in target schemas ==="
duckdb poc.duckdb <<'SQL'
SELECT schema_name, table_name, row_count
FROM (
    VALUES
        ('staging__orders', 'orders',      (SELECT COUNT(*) FROM poc.staging__orders.orders)),
        ('staging__orders', 'order_items', (SELECT COUNT(*) FROM poc.staging__orders.order_items)),
        ('staging__events', 'events',      (SELECT COUNT(*) FROM poc.staging__events.events))
) t(schema_name, table_name, row_count)
ORDER BY schema_name, table_name;
SQL

echo ""
echo "=== override rules summary ==="
cat <<'EOF'

  Rule 1  match.connector=raw__orders
            merge_keys=["order_id"]
            Effect: all raw__orders tables (orders, order_items) get per-connector merge_keys.
            Demonstrates: connector-level override, most-specific-match-wins (Rule 2 also
            matches orders and supplies strategy; merge_keys still flows from Rule 1).

  Rule 2  match.connector=raw__orders  match.table=orders
            strategy=incremental  timestamp_column=ordered_at
            Effect: orders -> incremental; inherits merge_keys=["order_id"] from Rule 1.
            Demonstrates: narrower rule wins on strategy; broader rule wins on merge_keys.

  Rule 3  match.table=_diagnostics_*
            enabled=false
            Effect: _diagnostics_sync skipped across ALL connectors.
            Demonstrates: glob `*`, connector-agnostic rule.

  Rule 4  match.connector=raw__events  match.table=user_??
            enabled=false
            Effect: user_01 and user_02 skipped; `??` = exactly two characters.
            Demonstrates: glob `?`, connector-scoped rule.

  Rule 5  match.connector=raw__events  match.table=events
            strategy=incremental  timestamp_column=occurred_at
            Effect: events -> incremental with occurred_at watermark.
            Demonstrates: full two-axis match on a different connector.

EOF
echo "POC complete."
