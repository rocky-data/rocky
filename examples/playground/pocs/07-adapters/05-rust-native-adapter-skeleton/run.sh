#!/usr/bin/env bash
# Drives the Rust-native adapter skeleton end-to-end. No credentials.
#
# What this proves:
# - The skeleton crate compiles cleanly against the published SDK trait
#   surface (`rocky_adapter_sdk::traits::WarehouseAdapter` + `SqlDialect`).
# - All adapter unit tests pass against an in-memory mock backend.
# - The example binary prints the SQL the adapter would have sent to a
#   real warehouse, so a reader can see exactly what the dialect emits.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE/adapter"

if ! command -v cargo >/dev/null 2>&1; then
    echo "cargo not found — install Rust 1.85+ from https://rustup.rs to run this POC" >&2
    exit 1
fi

mkdir -p ../expected

echo "=== cargo check (skeleton compiles against published SDK) ==="
cargo check --tests --examples --quiet

echo
echo "=== cargo test (8 unit tests) ==="
cargo test --quiet

echo
echo "=== cargo run --example demo (end-to-end against mock backend) ==="
cargo run --example demo --quiet | tee ../expected/demo-transcript.txt

echo
echo "POC complete: skeleton builds, tests pass, demo prints generated SQL."
