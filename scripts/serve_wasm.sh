#!/usr/bin/env bash
# Build rocky-wasm and serve the browser playground.
#
# Prerequisites:
#   rustup target add wasm32-unknown-unknown
#   cargo install wasm-pack
#
# Usage:
#   ./scripts/serve_wasm.sh          # build + serve on port 8080
#   ./scripts/serve_wasm.sh 3000     # build + serve on custom port

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${1:-8080}"

echo "Building rocky-wasm..."
bash "${REPO_ROOT}/scripts/build_wasm.sh"

echo "Copying pkg/ into playground..."
rm -rf "${REPO_ROOT}/engine/playground/pkg"
cp -r "${REPO_ROOT}/engine/crates/rocky-wasm/pkg" "${REPO_ROOT}/engine/playground/pkg"

echo "Serving playground at http://localhost:${PORT}"
cd "${REPO_ROOT}/engine/playground" && python3 -m http.server "${PORT}"
