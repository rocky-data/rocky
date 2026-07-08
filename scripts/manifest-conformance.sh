#!/usr/bin/env bash
# Drift tripwire between the rocky-manifest spec and the engine: produce a
# manifest from *live* engine output and prove the standalone rocky-verify
# validates it against the published schema, offline. Also proves a tampered
# manifest fails. Credentials-free (DuckDB).
#
# If the engine's recorded recipe-identity shape changes in a way that breaks
# the spec, the jq transform below emits a null for a required field and
# rocky-verify's schema validation fails here.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ROCKY_BIN="${ROCKY_BIN:-$ROOT/engine/target/release/rocky}"
VERIFY_BIN="${VERIFY_BIN:-$ROOT/engine/target/release/rocky-verify}"

if [ ! -x "$ROCKY_BIN" ]; then
  echo "building rocky ..."
  (cd engine && cargo build --release --bin rocky)
fi
if [ ! -x "$VERIFY_BIN" ]; then
  echo "building rocky-verify ..."
  (cd engine && cargo build --release --bin rocky-verify)
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "=== 1. produce real engine output (recipe-provenance POC) ==="
ROCKY_BIN="$ROCKY_BIN" bash examples/playground/pocs/04-governance/10-recipe-provenance/run.sh >/dev/null
POC_OUT=examples/playground/pocs/04-governance/10-recipe-provenance/expected/recipe_history.json

echo "=== 2. derive a rocky-manifest v0.1 from that output ==="
jq '{
  manifest_version: "0.1",
  hash_scheme: .executions[0].recipe_identity.hash_scheme,
  producer: { name: "rocky", version: .version },
  subject: {
    model: .executions[0].model_name,
    run_id: .executions[0].run_id,
    produced_at: .executions[0].started_at,
    status: .executions[0].status
  },
  program_hash: .executions[0].recipe_identity.recipe_hash,
  inputs_hash: .executions[0].recipe_identity.input_hash,
  inputs_proof_class: .executions[0].recipe_identity.input_proof_class,
  env_hash: .executions[0].recipe_identity.env_hash
}' "$POC_OUT" > "$WORK/manifest.json"
cat "$WORK/manifest.json"

echo
echo "=== 3. verify the live manifest offline (no engine) ==="
"$VERIFY_BIN" verify "$WORK/manifest.json"

echo
echo "=== 4. byte-verify the shipped content-addressed audit sample ==="
"$VERIFY_BIN" verify engine/crates/rocky-verify/fixtures/valid/content-addressed.json \
  --artifacts-dir examples/audit-sample

echo
echo "=== 5. a tampered manifest must FAIL ==="
jq '.env_hash = "TAMPERED"' "$WORK/manifest.json" > "$WORK/tampered.json"
if "$VERIFY_BIN" verify "$WORK/tampered.json"; then
  echo "::error::a tampered manifest verified — the tripwire is broken" >&2
  exit 1
fi

echo
echo "✓ manifest conformance: live engine output validates; tampered output rejected."
