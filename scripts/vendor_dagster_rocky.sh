#!/usr/bin/env bash
set -euo pipefail

# Build the rocky-sdk + dagster-rocky wheels and vendor both.
#
# Usage:
#   ./scripts/vendor_dagster_rocky.sh
#
# dagster-rocky depends on rocky-sdk (`Requires-Dist: rocky-sdk>=…`). The
# `[tool.uv.sources]` path dependency that resolves the SDK in-repo is dev-only
# and is NOT carried in the built wheel — so a clean install of the vendored
# dagster-rocky wheel can only resolve rocky-sdk if the SDK wheel is vendored
# alongside it. This script therefore builds and vendors BOTH, atomically, so a
# downstream consumer can `pip install --no-index --find-links vendor/
# dagster-rocky` without reaching PyPI.
#
# Prerequisites:
#   - uv (https://docs.astral.sh/uv/)

readonly SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
readonly WORKSPACE_ROOT="$(dirname "$SCRIPT_DIR")"
readonly SDK_DIR="$WORKSPACE_ROOT/sdk/python"
readonly DAGSTER_DIR="$WORKSPACE_ROOT/integrations/dagster"
readonly VENDOR_DIR="$WORKSPACE_ROOT/vendor"

if [[ ! -d "$SDK_DIR" ]]; then
    echo "Error: rocky-sdk not found at $SDK_DIR" >&2
    exit 1
fi
if [[ ! -d "$DAGSTER_DIR" ]]; then
    echo "Error: dagster integration not found at $DAGSTER_DIR" >&2
    exit 1
fi
if ! command -v uv >/dev/null 2>&1; then
    echo "Error: uv not found. Install via https://docs.astral.sh/uv/" >&2
    exit 1
fi

mkdir -p "$VENDOR_DIR"

# Vendor one wheel: build it, locate the freshest artifact, replace any prior
# copy of the same distribution in vendor/.
vendor_wheel() {
    local dir="$1" glob="$2" label="$3"
    echo "==> Building $label wheel from $dir"
    (cd "$dir" && uv build --wheel)

    local wheel
    wheel=$(ls -t "$dir/dist/"$glob 2>/dev/null | head -1)
    if [[ -z "$wheel" ]]; then
        echo "Error: no wheel found in $dir/dist/ (pattern $glob)" >&2
        exit 1
    fi

    rm -f "$VENDOR_DIR/"$glob
    cp "$wheel" "$VENDOR_DIR/"
    echo "==> Vendored $(basename "$wheel")"
}

# Build the SDK first — dagster-rocky depends on it.
vendor_wheel "$SDK_DIR" "rocky_sdk-*.whl" "rocky-sdk"
vendor_wheel "$DAGSTER_DIR" "dagster_rocky-*.whl" "dagster-rocky"

echo "==> Vendored wheels in $VENDOR_DIR:"
ls -1 "$VENDOR_DIR"/rocky_sdk-*.whl "$VENDOR_DIR"/dagster_rocky-*.whl
