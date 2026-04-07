#!/usr/bin/env bash
set -euo pipefail

# Build the dagster-rocky wheel from integrations/dagster/ and vendor it.
#
# Usage:
#   ./scripts/vendor_dagster_rocky.sh
#
# In the previous polyrepo layout this script took an external path to a
# dagster-rocky clone. After the monorepo consolidation, the source lives
# in-tree at integrations/dagster/, so the script is a thin wrapper around
# `uv build --wheel`.
#
# Prerequisites:
#   - uv (https://docs.astral.sh/uv/)

readonly SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
readonly WORKSPACE_ROOT="$(dirname "$SCRIPT_DIR")"
readonly DAGSTER_DIR="$WORKSPACE_ROOT/integrations/dagster"
readonly VENDOR_DIR="$WORKSPACE_ROOT/vendor"

if [[ ! -d "$DAGSTER_DIR" ]]; then
    echo "Error: dagster integration not found at $DAGSTER_DIR" >&2
    exit 1
fi
if ! command -v uv >/dev/null 2>&1; then
    echo "Error: uv not found. Install via https://docs.astral.sh/uv/" >&2
    exit 1
fi

echo "==> Building dagster-rocky wheel from $DAGSTER_DIR"
(cd "$DAGSTER_DIR" && uv build --wheel)

WHEEL=$(ls -t "$DAGSTER_DIR/dist"/dagster_rocky-*.whl 2>/dev/null | head -1)
if [[ -z "$WHEEL" ]]; then
    echo "Error: no wheel found in $DAGSTER_DIR/dist/" >&2
    exit 1
fi

echo "==> Copying wheel to $VENDOR_DIR"
mkdir -p "$VENDOR_DIR"
# Remove any previous dagster-rocky wheels to keep the vendor dir tidy
rm -f "$VENDOR_DIR"/dagster_rocky-*.whl
cp "$WHEEL" "$VENDOR_DIR/"

WHEEL_NAME=$(basename "$WHEEL")
echo "==> Vendored $WHEEL_NAME"
