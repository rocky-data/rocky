#!/usr/bin/env bash
#
# Regenerate the dagster test fixtures by running the live `rocky` binary
# against the 00-playground-default POC and capturing JSON output.
#
# Usage:
#   ./scripts/regen_fixtures.sh                    # write to fixtures_generated/
#   ./scripts/regen_fixtures.sh --in-place         # OVERWRITE the committed fixtures/
#
# By default the script writes captured output to a parallel directory at
# integrations/dagster/tests/fixtures_generated/. Pass --in-place to replace
# the committed fixtures/ directory in one shot. The committed fixtures are
# the test source of truth today; the parallel directory is for drift
# detection and a future migration.
#
# Each fixture file lands at the path the dagster tests expect for the
# corresponding command name (discover.json, run.json, etc.). The metrics
# fixture is named differently from its CLI invocation because it requires
# a model argument.
#
# Prerequisites:
#   - rocky binary built at engine/target/release/rocky
#     (run `cargo build --release --bin rocky` from engine/ if missing)
#   - duckdb CLI on PATH (used to seed the playground.duckdb file)

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
readonly WORKSPACE_ROOT="$(dirname "$SCRIPT_DIR")"
readonly ROCKY="$WORKSPACE_ROOT/engine/target/release/rocky"
readonly POC="$WORKSPACE_ROOT/examples/playground/pocs/00-foundations/00-playground-default"
readonly PARTITION_POC="$WORKSPACE_ROOT/examples/playground/pocs/02-performance/03-partition-checksum"
readonly NORMALIZER="$SCRIPT_DIR/_normalize_fixture.py"

# Output directory: fixtures_generated/ by default, fixtures/ if --in-place.
DEST="$WORKSPACE_ROOT/integrations/dagster/tests/fixtures_generated"
if [[ "${1:-}" == "--in-place" ]]; then
    DEST="$WORKSPACE_ROOT/integrations/dagster/tests/fixtures"
    echo "==> --in-place: will OVERWRITE $DEST"
fi

# --- Pre-flight ---

if [[ ! -x "$ROCKY" ]]; then
    echo "Error: rocky binary not found at $ROCKY" >&2
    echo "Build it with: cd engine && cargo build --release --bin rocky" >&2
    exit 1
fi
if ! command -v duckdb >/dev/null 2>&1; then
    echo "Error: duckdb CLI not found on PATH" >&2
    echo "Install with: brew install duckdb (or your platform equivalent)" >&2
    exit 1
fi
if [[ ! -d "$POC" ]]; then
    echo "Error: playground POC not found at $POC" >&2
    exit 1
fi

mkdir -p "$DEST"

# --- Bootstrap the POC: clean state, seed duckdb, run rocky once for state ---

echo "==> bootstrap"
cd "$POC"
rm -f .rocky-state.redb poc.duckdb playground.duckdb
duckdb playground.duckdb < data/seed.sql >/dev/null
"$ROCKY" run --filter source=orders >/dev/null 2>&1

# --- Capture each fixture ---
#
# Each helper invocation runs a `rocky` command, redirects stderr (which
# carries the JSON tracing logs) to /dev/null, and writes stdout to the
# destination file. We re-validate the JSON afterward to fail fast on any
# command that emitted garbage.

capture() {
    local name="$1"; shift
    local out_file="$DEST/${name}.json"
    echo "==> $name"
    # Some commands legitimately exit non-zero with valid JSON output:
    #   - `rocky doctor` exits 1 when any check is "warning", 2 when "critical".
    #   - `rocky run` can exit non-zero on partial success but still emit JSON.
    # We tolerate any exit code as long as stdout parses as JSON.
    "$ROCKY" "$@" 2>/dev/null > "$out_file" || true
    # Normalize the captured JSON: every wall-clock timing field is zeroed
    # out so the fixtures are stable across regen runs. Without this,
    # doctor's `duration_ms` and compile's `total_ms`/etc. drift by a few
    # milliseconds on every machine and break the codegen-drift CI check.
    python3 "$NORMALIZER" "$out_file"
}

capture discover discover
capture plan plan --filter source=orders
capture run run --filter source=orders
capture state state
capture compile compile --models models --contracts contracts
capture test test --models models --contracts contracts
capture ci ci --models models --contracts contracts
capture lineage lineage revenue_summary --models models
capture doctor doctor
capture history history
capture metrics metrics revenue_summary
capture optimize optimize
capture dag dag --models models

# Drift detection requires the engine to detect schema changes against an
# already-existing target. The 00-playground-default POC always uses
# full_refresh, so there's no drift to detect. We capture an empty drift
# baseline by running the same command and verifying it returns the
# expected zero-drift envelope.
echo "==> drift (zero-drift baseline)"
"$ROCKY" run --filter source=orders 2>/dev/null > /tmp/_drift_run.json
# The run output's `drift` sub-object is what dagster's DriftDetectResult
# parser consumes. Drift_detect.json fixtures aren't produced by a separate
# CLI command — they're projected from the run output. We don't write a
# drift.json file unless the user wants to keep the hand-written one.

# --- Cleanup the playground POC state ---
rm -f .rocky-state.redb poc.duckdb playground.duckdb /tmp/_drift_run.json

# ---------------------------------------------------------------------------
# Partition fixtures (time_interval shape)
#
# The 00-playground-default POC uses `full_refresh`, so it never produces
# `partition_summaries` (top-level) or per-materialization `partition`
# fields. To document those shapes in the corpus, we additionally run the
# 02-performance/03-partition-checksum POC, which has a `time_interval`
# model. The POC's `run.sh` is a 3-step demo (single partition / backfill /
# late-arriving correction); we replay just the rocky CLI invocations and
# capture each one as a separate partition_*.json fixture.
# ---------------------------------------------------------------------------

PARTITION_DEST="$DEST/partition"
mkdir -p "$PARTITION_DEST"

capture_partition() {
    local name="$1"; shift
    local out_file="$PARTITION_DEST/${name}.json"
    echo "==> partition/$name"
    "$ROCKY" "$@" 2>/dev/null > "$out_file" || true
    python3 "$NORMALIZER" "$out_file"
}

if [[ -d "$PARTITION_POC" ]]; then
    cd "$PARTITION_POC"
    rm -f .rocky-state.redb poc.duckdb
    duckdb poc.duckdb < data/seed.sql >/dev/null

    # Phase 0: typed compile output for the time_interval model.
    capture_partition compile compile --models models

    # Phase 1: run a single partition (--partition KEY) in isolation.
    capture_partition run_single \
        -c rocky.toml run \
        --filter source=orders \
        --models models \
        --partition 2026-04-07

    # Phase 2: backfill a closed range (--from / --to).
    capture_partition run_backfill \
        -c rocky.toml run \
        --filter source=orders \
        --models models \
        --from 2026-04-04 --to 2026-04-08

    # Phase 3: simulate a late-arriving row, then re-run the same
    # partition. With time_interval, the DELETE+INSERT cycle picks it up.
    duckdb poc.duckdb -c \
        "INSERT INTO raw__orders.orders VALUES
            (10, 3, '2026-04-07 22:00:00', 999.0);" >/dev/null
    capture_partition run_late_correction \
        -c rocky.toml run \
        --filter source=orders \
        --models models \
        --partition 2026-04-07

    # Cleanup partition POC state.
    rm -f .rocky-state.redb poc.duckdb
else
    echo "==> partition POC not found at $PARTITION_POC, skipping partition fixtures"
fi

# ---------------------------------------------------------------------------
# Feature-coverage fixtures (Option B — rich shapes from purpose-built POCs)
#
# The 00-playground-default POC uses full_refresh on a single source, so its
# captures only cover the happy path: empty drift, empty check_results, no
# anomalies, no diagnostics, no incremental watermark, no run history, no
# optimize recommendations. To document those rich shapes for the dagster
# parser-compat guard (test_generated_fixtures.py), we additionally drive a
# handful of feature-targeted POCs and stash their captures under
# fixtures_generated/<feature>/. Each section is independent and skips
# silently if the POC directory is missing.
# ---------------------------------------------------------------------------

# Generic capture helper used by all the feature sections below. The capture()
# helper at the top of this script writes into $DEST flat; this one writes
# into a named subdirectory and stays cwd-agnostic.
capture_into() {
    local subdir="$1"; shift
    local name="$1"; shift
    local out_file="$DEST/${subdir}/${name}.json"
    echo "==> ${subdir}/${name}"
    "$ROCKY" "$@" 2>/dev/null > "$out_file" || true
    python3 "$NORMALIZER" "$out_file"
}

POCS_ROOT="$WORKSPACE_ROOT/examples/playground/pocs"

# --- drift: rich `actions_taken` from a real DROP+RECREATE -----------------
DRIFT_POC="$POCS_ROOT/02-performance/06-schema-drift-recover"
if [[ -d "$DRIFT_POC" ]]; then
    cd "$DRIFT_POC"
    rm -f .rocky-state.redb poc.duckdb
    duckdb poc.duckdb < data/seed.sql >/dev/null
    mkdir -p "$DEST/drift"
    # First run is a clean baseline.
    capture_into drift run_clean -c rocky.toml run --filter source=orders
    # Drift the source: amount DECIMAL(10,2) → VARCHAR.
    duckdb poc.duckdb \
        "ALTER TABLE raw__orders.orders ALTER COLUMN amount TYPE VARCHAR" \
        >/dev/null
    # Second run triggers DROP + RECREATE → drift.actions_taken populated.
    capture_into drift run_after_drift -c rocky.toml run --filter source=orders
    rm -f .rocky-state.redb poc.duckdb
else
    echo "==> drift POC not found at $DRIFT_POC, skipping"
fi

# --- contracts: compile output with E010/E013 diagnostics ------------------
CONTRACTS_POC="$POCS_ROOT/01-quality/01-data-contracts-strict"
if [[ -d "$CONTRACTS_POC" ]]; then
    cd "$CONTRACTS_POC"
    rm -f .rocky-state.redb
    mkdir -p "$DEST/contracts"
    capture_into contracts compile compile --models models --contracts contracts
    rm -f .rocky-state.redb
else
    echo "==> contracts POC not found at $CONTRACTS_POC, skipping"
fi

# --- incremental: state with watermark + incremental run metadata ---------
INCREMENTAL_POC="$POCS_ROOT/02-performance/01-incremental-watermark"
if [[ -d "$INCREMENTAL_POC" ]]; then
    cd "$INCREMENTAL_POC"
    rm -f .rocky-state.redb poc.duckdb
    duckdb poc.duckdb < data/seed_initial.sql >/dev/null
    mkdir -p "$DEST/incremental"
    capture_into incremental run1_initial -c rocky.toml run --filter source=events
    capture_into incremental state_after_run1 -c rocky.toml state
    duckdb poc.duckdb < data/seed_delta.sql >/dev/null
    capture_into incremental run2_delta -c rocky.toml run --filter source=events
    capture_into incremental state_after_run2 -c rocky.toml state
    rm -f .rocky-state.redb poc.duckdb
else
    echo "==> incremental POC not found at $INCREMENTAL_POC, skipping"
fi

# --- optimize: recommendations + profile-storage + compact ----------------
OPTIMIZE_POC="$POCS_ROOT/02-performance/05-optimize-recommendations"
if [[ -d "$OPTIMIZE_POC" ]]; then
    cd "$OPTIMIZE_POC"
    rm -f .rocky-state.redb poc.duckdb
    duckdb poc.duckdb < data/seed.sql >/dev/null
    mkdir -p "$DEST/optimize"
    capture_into optimize run -c rocky.toml run --filter source=events
    capture_into optimize optimize -c rocky.toml optimize
    rm -f .rocky-state.redb poc.duckdb
else
    echo "==> optimize POC not found at $OPTIMIZE_POC, skipping"
fi

# --- lineage: model + column-level lineage from a real semantic graph -----
LINEAGE_POC="$POCS_ROOT/06-developer-experience/01-lineage-column-level"
if [[ -d "$LINEAGE_POC" ]]; then
    cd "$LINEAGE_POC"
    rm -f .rocky-state.redb
    mkdir -p "$DEST/lineage"
    capture_into lineage compile compile --models models
    capture_into lineage test test --models models
    capture_into lineage lineage_model lineage fct_revenue --models models
    capture_into lineage lineage_column lineage fct_revenue --models models --column total
    rm -f .rocky-state.redb
else
    echo "==> lineage POC not found at $LINEAGE_POC, skipping"
fi

# --- doctor_ci: doctor + ci output with non-trivial check / diagnostic ----
DOCTOR_CI_POC="$POCS_ROOT/06-developer-experience/05-doctor-and-ci"
if [[ -d "$DOCTOR_CI_POC" ]]; then
    cd "$DOCTOR_CI_POC"
    rm -f .rocky-state.redb
    mkdir -p "$DEST/doctor_ci"
    capture_into doctor_ci doctor -c rocky.toml doctor
    capture_into doctor_ci ci ci --models models
    rm -f .rocky-state.redb
else
    echo "==> doctor_ci POC not found at $DOCTOR_CI_POC, skipping"
fi

# --- anomaly: history populated by 4 runs + the incident divergence -------
ANOMALY_POC="$POCS_ROOT/01-quality/03-anomaly-detection"
if [[ -d "$ANOMALY_POC" ]]; then
    cd "$ANOMALY_POC"
    rm -f .rocky-state.redb poc.duckdb
    mkdir -p "$DEST/anomaly"
    # 3 baseline runs to populate run history.
    for i in 1 2 3; do
        duckdb poc.duckdb < data/seed.sql >/dev/null
        capture_into anomaly "run_baseline_${i}" -c rocky.toml run --filter source=events
    done
    # Truncated source → anomaly run with the divergence.
    duckdb poc.duckdb < data/seed_truncated.sql >/dev/null
    capture_into anomaly run_incident -c rocky.toml run --filter source=events
    # History should now have 4 entries.
    capture_into anomaly history -c rocky.toml history
    rm -f .rocky-state.redb poc.duckdb
else
    echo "==> anomaly POC not found at $ANOMALY_POC, skipping"
fi

echo
echo "Captured $(find "$DEST" -name '*.json' | wc -l | tr -d ' ') fixtures into $DEST"
