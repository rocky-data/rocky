#!/usr/bin/env bash
# Build rocky-wasm for the web target using wasm-pack.
#
# Prerequisites:
#   rustup target add wasm32-unknown-unknown
#   cargo install wasm-pack
#
# Usage:
#   ./scripts/build_wasm.sh            # default: --target web
#   ./scripts/build_wasm.sh bundler    # for webpack/rollup
#   ./scripts/build_wasm.sh nodejs     # for Node.js

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TARGET="${1:-web}"

echo "Building rocky-wasm (target: ${TARGET})..."
wasm-pack build "${REPO_ROOT}/engine/crates/rocky-wasm" --target "${TARGET}"
echo "Done. Output in engine/crates/rocky-wasm/pkg/"
