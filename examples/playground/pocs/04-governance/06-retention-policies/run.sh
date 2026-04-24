#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
mkdir -p expected
rm -f .rocky-state.redb poc.duckdb models/.rocky-state.redb

echo "===================================================================="
echo " POC 04-governance/06-retention-policies"
echo " Data retention policies (Wave C-2 / engine-v1.16.0)"
echo "===================================================================="
echo
echo "Models in this POC:"
echo "  - page_views_30d.sql      retention = \"30d\"  ->  30 days"
echo "  - ledger_archive_1y.sql   retention = \"1y\"   ->  365 days (flat)"
echo "  - ephemeral_summary.sql   (no retention key) ->  null"
echo

# --- Seed DuckDB so `rocky run` has something to read ---
duckdb poc.duckdb < data/seed.sql

# --- 1. Validate: confirms the sidecars parse (retention grammar is
#     enforced at model-load time in rocky-core::models). A bad value
#     like "abc" would fail here with a clear diagnostic.
echo "--- rocky validate (sidecars parse) ----------------------------------"
rocky validate
echo

# --- 2. rocky run on DuckDB. The pipeline uses replication, which on
#     DuckDB falls through to a "no discovery adapter" exit — same as
#     the sibling POC 05-orchestration/09-idempotency-key. The point of
#     this step is NOT the materialization; it's that the retention
#     wiring doesn't abort the pipeline. On DuckDB the governance
#     adapter resolves to NoopGovernanceAdapter::apply_retention_policy
#     which returns Ok(()) silently (traits.rs:811-821). No warning,
#     no error.
echo "--- rocky run (DuckDB; retention apply is a silent no-op) ------------"
rocky -c rocky.toml run --filter source=events > expected/run.json 2>&1 || true
echo "  run completed (exit status tolerated on DuckDB; see README)"
echo

# --- 3. rocky retention-status — text table view ---
echo "--- rocky retention-status -o table ----------------------------------"
rocky retention-status --models models -o table | tee expected/retention_status.txt
echo

# --- 4. rocky retention-status — JSON (default) ---
echo "--- rocky retention-status (JSON, default output) --------------------"
rocky retention-status --models models > expected/retention_status.json
jq '.' expected/retention_status.json
echo

# --- 5. --model <name> scope ---
echo "--- rocky retention-status --model ledger_archive_1y -o table --------"
rocky retention-status --models models --model ledger_archive_1y -o table \
  | tee expected/retention_status_scoped.txt
echo

# --- 6. --drift is v2. Pair with -o table so the deferred-note lands
#     on stderr. With JSON output the note is suppressed by design
#     (retention_status.rs:65-67).
echo "--- rocky retention-status --drift -o table (v2 probe deferred) ------"
rocky retention-status --models models --drift -o table 2>&1 \
  | tee expected/retention_status_drift.txt
echo

# --- Validation: page_views_30d should report 30, ledger_archive_1y
#     should report 365, ephemeral_summary should report null. ---
CFG_30=$(jq -r '.models[] | select(.model=="page_views_30d") | .configured_days' expected/retention_status.json)
CFG_1Y=$(jq -r '.models[] | select(.model=="ledger_archive_1y") | .configured_days' expected/retention_status.json)
CFG_NONE=$(jq -r '.models[] | select(.model=="ephemeral_summary") | .configured_days' expected/retention_status.json)

if [[ "$CFG_30" != "30" ]]; then
  echo "FAIL: page_views_30d expected configured_days=30 but got '$CFG_30'"
  exit 1
fi
if [[ "$CFG_1Y" != "365" ]]; then
  echo "FAIL: ledger_archive_1y expected configured_days=365 but got '$CFG_1Y'"
  exit 1
fi
if [[ "$CFG_NONE" != "null" ]]; then
  echo "FAIL: ephemeral_summary expected configured_days=null but got '$CFG_NONE'"
  exit 1
fi

echo "===================================================================="
echo " POC complete: 30d -> 30, 1y -> 365 (flat), no-key -> null."
echo " Apply is a silent no-op on DuckDB; Databricks/Snowflake enforce"
echo " it via TBLPROPERTIES / DATA_RETENTION_TIME_IN_DAYS. See README."
echo "===================================================================="
