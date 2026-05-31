#!/usr/bin/env bash
# Cross-team contracts: a consumer project imports a producer's published IR
# snapshot and `rocky compile` fails (E030) when the producer drops a column
# the consumer reads.
#
# Step 4's FAILING consumer compile is the SUCCESS condition for this POC —
# we invert the exit code: we exit 0 only when that compile emitted E030.
#
# Uses `set -uo pipefail` (NOT `set -e`) so the expected step-4 failure does
# not abort the script.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROCKY="$HERE/../../../../../engine/target/release/rocky"

PRODUCER="$HERE/orders-producer"
CONSUMER="$HERE/shipments-consumer"
VENDOR="$CONSUMER/vendor/orders"

if [[ ! -x "$ROCKY" ]]; then
    echo "ERROR: built rocky binary not found at $ROCKY"
    echo "Build it first:  (cd engine && cargo build --release -p rocky)"
    exit 1
fi

echo "=================================================================="
echo " Cross-team contracts — producer drops a column the consumer reads"
echo "=================================================================="
echo "rocky: $ROCKY"
echo

mkdir -p "$VENDOR"

# Always restore the producer's full-column model on exit, even on failure.
# Stash lives OUTSIDE models/ so the model loader never tries to parse it.
STASH="$PRODUCER/.orders.full.sql"
restore_producer() {
    if [[ -f "$STASH" ]]; then
        cp "$STASH" "$PRODUCER/models/orders.sql"
        rm -f "$STASH"
    fi
}
cp "$PRODUCER/models/orders.sql" "$STASH"
trap restore_producer EXIT

# ------------------------------------------------------------------
# Step 1 — producer publishes its IR snapshot (pre-drop).
#          Vendor it as BOTH baseline.json and current.json.
# ------------------------------------------------------------------
echo "--- Step 1: producer publishes IR snapshot (pre-drop)"
"$ROCKY" publish-ir \
    --models "$PRODUCER/models" \
    --out "$PRODUCER/vendor-out/project-ir.json" \
    --with-seed || { echo "FAIL: producer publish-ir failed"; exit 1; }

cp "$PRODUCER/vendor-out/project-ir.json" "$VENDOR/baseline.json"
cp "$PRODUCER/vendor-out/project-ir.json" "$VENDOR/current.json"

if ! grep -q '"shipped_at"' "$VENDOR/baseline.json"; then
    echo "FAIL: baseline snapshot is missing typed columns (shipped_at)."
    echo "      The snapshot must carry concrete columns or E030 can never fire."
    exit 1
fi
echo "    baseline + current snapshots vendored (carry shipped_at)."
echo

# ------------------------------------------------------------------
# Step 2 — consumer compiles cleanly: it reads shipped_at, which the
#          producer still publishes. Success = no errors (exit 0).
# ------------------------------------------------------------------
echo "--- Step 2: consumer compiles against the pre-drop snapshot"
if "$ROCKY" --config "$CONSUMER/rocky.toml" compile \
        --models "$CONSUMER/models" \
        --output json > "$HERE/.step2.json" 2>"$HERE/.step2.err"; then
    echo "    PASS: consumer compiles clean (producer still outputs shipped_at)."
else
    echo "FAIL: consumer compile errored before the producer dropped anything:"
    cat "$HERE/.step2.err"
    grep -o '"code":"E0[0-9][0-9]"' "$HERE/.step2.json" || true
    exit 1
fi
echo

# ------------------------------------------------------------------
# Step 3 — producer ships a breaking change: drop shipped_at, re-publish,
#          overwrite ONLY current.json (baseline stays pre-drop).
# ------------------------------------------------------------------
echo "--- Step 3: producer drops shipped_at and re-publishes"
cp "$PRODUCER/orders.dropped.sql" "$PRODUCER/models/orders.sql"
"$ROCKY" publish-ir \
    --models "$PRODUCER/models" \
    --out "$PRODUCER/vendor-out/project-ir.json" \
    --with-seed || { echo "FAIL: producer re-publish failed"; exit 1; }
cp "$PRODUCER/vendor-out/project-ir.json" "$VENDOR/current.json"
echo "    current.json updated (shipped_at dropped); baseline unchanged."
echo

# ------------------------------------------------------------------
# Step 4 — consumer recompiles. This MUST fail with E030 (the consumer
#          still reads shipped_at). Inverted exit code: 0 iff E030 present.
# ------------------------------------------------------------------
echo "--- Step 4: consumer recompiles — expect E030 (contract gate fires)"
"$ROCKY" --config "$CONSUMER/rocky.toml" compile \
    --models "$CONSUMER/models" \
    --output json > "$HERE/.step4.json" 2>"$HERE/.step4.err"
STEP4_EXIT=$?

if grep -q '"E030"' "$HERE/.step4.json"; then
    echo "    PASS: compile failed with E030 (exit $STEP4_EXIT):"
    grep -o '"message": *"[^"]*shipped_at[^"]*"' "$HERE/.step4.json" | head -1 | sed 's/^/      /'
    if [[ "$STEP4_EXIT" -eq 0 ]]; then
        echo "FAIL: E030 present but compile exited 0 — the gate did not block."
        exit 1
    fi
    echo
    echo "Cross-team contract enforced: the producer's breaking change was"
    echo "caught at the consumer's compile, before any SQL ran."
    rm -f "$HERE/.step2.json" "$HERE/.step2.err" "$HERE/.step4.json" "$HERE/.step4.err"
    exit 0
else
    echo "FAIL: expected E030 in the consumer compile output but found none."
    cat "$HERE/.step4.err"
    cat "$HERE/.step4.json"
    exit 1
fi
