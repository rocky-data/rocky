#!/usr/bin/env bash
# prepare.sh <demo> — set up a clean scratch workspace for a vhs tape.
#
# Each tape records inside cli-recording/scratch/<demo>/. This script builds
# that directory from a known-good source (a playground POC, or a fresh
# `rocky playground` scaffold) and strips any state that would make the
# recording non-deterministic (state store, local DuckDB file). The visible
# `rocky` commands stay in the .tape; this is the silent setup.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
POCS="$REPO_ROOT/examples/playground/pocs"

demo="${1:?usage: prepare.sh <demo>}"
scratch="$HERE/scratch/$demo"

rm -rf "$scratch"
mkdir -p "$scratch"

clean_state() {
    find "$1" -name '.rocky-state.redb*' -delete 2>/dev/null || true
    find "$1" -name '*.duckdb' -delete 2>/dev/null || true
    rm -rf "$1/expected" "$1/.rocky" 2>/dev/null || true
}

case "$demo" in
    quickstart)
        # Fresh scaffold, exactly as a new user would get it. The tape itself
        # runs `rocky playground rocky-playground`, so we only need an empty
        # working directory here.
        ;;
    column-lineage)
        cp -r "$POCS/06-developer-experience/01-lineage-column-level/." "$scratch/"
        clean_state "$scratch"
        ;;
    drift-recover)
        cp -r "$POCS/02-performance/06-schema-drift-recover/." "$scratch/"
        clean_state "$scratch"
        # Pre-seed the source so the visible `rocky run` has tables to copy.
        # The drama (the `ALTER ... TYPE VARCHAR`) stays in the tape.
        (cd "$scratch" && duckdb poc.duckdb < data/seed.sql >/dev/null)
        ;;
    data-contracts)
        cp -r "$POCS/01-quality/01-data-contracts-strict/." "$scratch/"
        clean_state "$scratch"
        ;;
    branches-replay)
        cp -r "$POCS/00-foundations/06-branches-replay-lineage/." "$scratch/"
        clean_state "$scratch"
        # Branch + replay run against DuckDB, so the source has to exist
        # before the visible run. The branch/run/replay sequence is the tape.
        (cd "$scratch" && duckdb poc.duckdb < data/seed.sql >/dev/null)
        ;;
    classification-masking)
        cp -r "$POCS/04-governance/05-classification-masking-compliance/." "$scratch/"
        clean_state "$scratch"
        ;;
    incremental-watermark)
        cp -r "$POCS/02-performance/01-incremental-watermark/." "$scratch/"
        clean_state "$scratch"
        # Seed the initial 25 rows silently; the tape shows run 1, then the
        # visible delta load (data/seed_delta.sql), then the incremental run 2.
        (cd "$scratch" && duckdb poc.duckdb < data/seed_initial.sql >/dev/null)
        ;;
    lineage-diff)
        # Git-driven: build a throwaway repo with a baseline commit on `main`
        # and a feature branch that renames a column + adds two derived ones.
        # The tape only types `rocky lineage-diff main`. Mirrors the POC's
        # own run.sh git setup (steps 1-2); the diff command is the payoff.
        poc="$POCS/06-developer-experience/11-lineage-diff"
        cp -r "$poc/models" "$poc/rocky.toml" "$scratch/"
        git_commit="git -c user.email=demo@rocky.dev -c user.name=Demo -c commit.gpgsign=false"
        (
            cd "$scratch"
            git init -q -b main
            $git_commit add .
            $git_commit commit -q -m "baseline: 5-model DAG"
            $git_commit checkout -q -b feature/revenue-rework
            cat > models/stg_orders.sql <<'EOF'
SELECT
    order_id,
    customer_id,
    amount AS amount_usd,
    amount * 0.20 AS tax_amount_usd,
    status,
    order_date
FROM poc.demo.raw_orders
WHERE status != 'cancelled'
EOF
            cat > models/fct_revenue.sql <<'EOF'
SELECT
    s.customer_id,
    c.segment,
    c.region,
    SUM(s.amount_usd) AS total_revenue,
    SUM(s.tax_amount_usd) AS total_tax
FROM poc.demo.stg_orders s
JOIN poc.demo.dim_customers c USING (customer_id)
GROUP BY s.customer_id, c.segment, c.region
EOF
            $git_commit add -A
            $git_commit commit -q -m "rename amount->amount_usd; add tax columns"
        )
        ;;
    *)
        echo "prepare.sh: unknown demo '$demo'" >&2
        exit 1
        ;;
esac

echo "prepared scratch/$demo"
