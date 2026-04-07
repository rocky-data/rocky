#!/usr/bin/env bash
set -euo pipefail

# Vendor a Rocky CLI binary into vendor/.
#
# Usage:
#   ./scripts/vendor_rocky.sh                  # build engine/ locally and vendor it
#   ./scripts/vendor_rocky.sh --release        # download the latest engine-v* release from GitHub
#   ./scripts/vendor_rocky.sh --release v0.1.0 # download a specific release (without engine- prefix)
#
# Default behavior: cargo build --release in engine/, then copy the resulting
# binary into vendor/. This is the fastest path for local development and is
# the in-tree counterpart to scripts/build_rocky_linux.sh (which is for Linux
# cross-compilation via Docker).
#
# Release mode (--release): downloads pre-built tarballs from GitHub releases.
# Useful in CI environments where building from source is too slow. Filters
# release tags by the engine-v* prefix because the monorepo also publishes
# dagster-v* and vscode-v* tags.
#
# Prerequisites:
#   Default mode:  rust toolchain (cargo)
#   Release mode:  gh CLI (authenticated) OR GITHUB_TOKEN env var

readonly REPO="rocky-data/rocky"
readonly TAG_PREFIX="engine-v"

readonly SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
readonly WORKSPACE_ROOT="$(dirname "$SCRIPT_DIR")"
readonly ENGINE_DIR="$WORKSPACE_ROOT/engine"
readonly VENDOR_DIR="$WORKSPACE_ROOT/vendor"

mkdir -p "$VENDOR_DIR"

# --- Mode dispatch ---

MODE="local"
VERSION=""
if [[ "${1:-}" == "--release" ]]; then
    MODE="release"
    VERSION="${2:-}"
fi

# --- Local build mode ---

if [[ "$MODE" == "local" ]]; then
    if [[ ! -d "$ENGINE_DIR" ]]; then
        echo "Error: engine source not found at $ENGINE_DIR" >&2
        exit 1
    fi
    if ! command -v cargo >/dev/null 2>&1; then
        echo "Error: cargo not found. Install rust via https://rustup.rs/" >&2
        exit 1
    fi

    echo "==> Building rocky from $ENGINE_DIR (release mode)"
    (cd "$ENGINE_DIR" && cargo build --release --bin rocky)

    BUILT="$ENGINE_DIR/target/release/rocky"
    if [[ ! -f "$BUILT" ]]; then
        echo "Error: cargo finished but binary not found at $BUILT" >&2
        exit 1
    fi

    HOST_OS="$(uname -s)"
    HOST_ARCH="$(uname -m)"
    case "$HOST_OS" in
        Darwin) PLATFORM="darwin" ;;
        Linux)  PLATFORM="linux" ;;
        *)      PLATFORM="$(echo "$HOST_OS" | tr '[:upper:]' '[:lower:]')" ;;
    esac
    case "$HOST_ARCH" in
        x86_64|amd64)  ARCH="amd64" ;;
        arm64|aarch64) ARCH="arm64" ;;
        *)             ARCH="$HOST_ARCH" ;;
    esac

    DEST="$VENDOR_DIR/rocky-${PLATFORM}-${ARCH}"
    cp "$BUILT" "$DEST"
    chmod +x "$DEST"

    echo "==> Vendored to $DEST"
    "$DEST" --version 2>/dev/null || true
    exit 0
fi

# --- Release download mode ---

download_file() {
    local url="$1" output="$2"
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL -o "$output" "$url"
    elif command -v wget >/dev/null 2>&1; then
        wget -q -O "$output" "$url"
    else
        echo "Error: curl or wget required" >&2
        exit 1
    fi
}

download_asset() {
    local asset="$1" dest="$2"
    echo "==> Downloading $asset (${TAG_PREFIX}${VERSION})"

    if command -v gh >/dev/null 2>&1; then
        gh release download "${TAG_PREFIX}${VERSION}" \
            --repo "$REPO" \
            --pattern "$asset" \
            --dir /tmp \
            --clobber
    elif [[ -n "${GITHUB_TOKEN:-}" ]]; then
        local asset_url
        asset_url=$(curl -fsSL \
            -H "Authorization: token $GITHUB_TOKEN" \
            -H "Accept: application/vnd.github+json" \
            "https://api.github.com/repos/$REPO/releases/tags/${TAG_PREFIX}${VERSION}" \
            | grep -o "\"browser_download_url\": \"[^\"]*$asset\"" \
            | cut -d'"' -f4)
        if [[ -z "$asset_url" ]]; then
            echo "Error: could not find $asset in release ${TAG_PREFIX}${VERSION}" >&2
            return 1
        fi
        curl -fsSL -H "Authorization: token $GITHUB_TOKEN" -L "$asset_url" -o "/tmp/$asset"
    else
        echo "Error: need either gh CLI (authenticated) or GITHUB_TOKEN env var" >&2
        exit 1
    fi

    tar -xzf "/tmp/$asset" -C /tmp
    mv /tmp/rocky "$dest"
    chmod +x "$dest"
    rm -f "/tmp/$asset"
    echo "==> Wrote $dest"
}

if [[ -z "$VERSION" ]]; then
    if ! command -v gh >/dev/null 2>&1; then
        echo "Error: --release without an explicit version requires gh CLI to look up the latest engine release" >&2
        exit 1
    fi
    VERSION=$(gh release list --repo "$REPO" --limit 30 \
        | awk -v p="$TAG_PREFIX" '$0 ~ p {print $1; exit}' \
        | sed "s/^${TAG_PREFIX}//")
    if [[ -z "$VERSION" ]]; then
        echo "Error: no engine releases found in $REPO" >&2
        exit 1
    fi
    echo "==> Latest engine release: ${TAG_PREFIX}${VERSION}"
fi

# Always vendor the linux/amd64 binary (used by Dockerfile builds)
download_asset "rocky-x86_64-unknown-linux-gnu.tar.gz" "$VENDOR_DIR/rocky-linux-amd64"

# On macOS, also vendor the native arm64 binary for local dev
if [[ "$(uname)" == "Darwin" ]]; then
    download_asset "rocky-aarch64-apple-darwin.tar.gz" "$VENDOR_DIR/rocky-darwin-arm64"
fi
