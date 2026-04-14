#!/usr/bin/env bash
# Build the Rocky binary for Linux x86_64 and place it in vendor/rocky-linux-amd64.
#
# Use this when GitHub Actions release credits are exhausted, or when you need
# to test a Rocky source change locally before pushing. This is the local-build
# counterpart to vendor_rocky.sh (which downloads from a GH release).
#
# Requirements (zigbuild, default):
#   - cargo-zigbuild:  cargo install cargo-zigbuild
#   - zig:             brew install zig
#   - Linux target:    rustup target add x86_64-unknown-linux-gnu
#
# Requirements (Docker fallback, --docker):
#   - Docker Desktop running with >= 12 GiB memory
#
# Usage:
#   ./scripts/build_rocky_linux.sh              # zigbuild (fast, ~3-5 min)
#   ./scripts/build_rocky_linux.sh --docker     # Docker fallback (~20 min)
#   NUM_JOBS=4 ./scripts/build_rocky_linux.sh --docker  # override Docker parallelism
#
# What it does:
#   1. Cargo-builds rocky in release mode targeting x86_64-unknown-linux-gnu
#   2. Copies the resulting binary into vendor/rocky-linux-amd64
#   3. Verifies the binary is ELF x86-64
#
# Downstream consumers copy from vendor/rocky-linux-amd64 into their
# own vendor/ directory:
#   cp vendor/rocky-linux-amd64 /path/to/consumer/vendor/

set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly WORKSPACE_ROOT="$(dirname "$SCRIPT_DIR")"
readonly ROCKY_SRC="$WORKSPACE_ROOT/engine"
readonly VENDOR_DEST="$WORKSPACE_ROOT/vendor/rocky-linux-amd64"

if [[ ! -d "$ROCKY_SRC" ]]; then
    echo "ERROR: Rocky source not found at $ROCKY_SRC" >&2
    exit 1
fi

USE_DOCKER=false
for arg in "$@"; do
    case "$arg" in
        --docker) USE_DOCKER=true ;;
        *) echo "Unknown argument: $arg" >&2; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Zigbuild path (default) — native cross-compilation, uses all cores
# ---------------------------------------------------------------------------
if [[ "$USE_DOCKER" == "false" ]]; then
    # Check prerequisites
    if ! command -v cargo-zigbuild &>/dev/null; then
        echo "ERROR: cargo-zigbuild not found. Install with: cargo install cargo-zigbuild" >&2
        exit 1
    fi
    if ! command -v zig &>/dev/null; then
        echo "ERROR: zig not found. Install with: brew install zig" >&2
        exit 1
    fi
    if ! rustup target list --installed | grep -q x86_64-unknown-linux-gnu; then
        echo "Adding x86_64-unknown-linux-gnu target..."
        rustup target add x86_64-unknown-linux-gnu
    fi

    echo "Rocky source:    $ROCKY_SRC"
    echo "Vendor target:   $VENDOR_DEST"
    echo "Method:          cargo-zigbuild (native cross-compilation)"
    echo

    cd "$ROCKY_SRC"
    cargo zigbuild --release --bin rocky --target x86_64-unknown-linux-gnu

    BUILT_BINARY="$ROCKY_SRC/target/x86_64-unknown-linux-gnu/release/rocky"

# ---------------------------------------------------------------------------
# Docker path (fallback) — emulated x86_64 container
# ---------------------------------------------------------------------------
else
    readonly CONTAINER_NAME="rocky-linux-build"
    readonly RUST_IMAGE="rust:latest"

    if ! docker info >/dev/null 2>&1; then
        echo "ERROR: Docker daemon not running. Start Docker Desktop and re-run." >&2
        exit 1
    fi

    docker_mem_bytes=$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)
    docker_mem_gib=$(( docker_mem_bytes / 1024 / 1024 / 1024 ))

    if [[ -z "${NUM_JOBS:-}" ]]; then
        if   (( docker_mem_gib >= 16 )); then NUM_JOBS=4
        elif (( docker_mem_gib >= 12 )); then NUM_JOBS=2
        else                                  NUM_JOBS=1
        fi
    fi

    echo "Rocky source:    $ROCKY_SRC"
    echo "Vendor target:   $VENDOR_DEST"
    echo "Method:          Docker (emulated x86_64)"
    echo "Docker memory:   ${docker_mem_gib} GiB"
    echo "Parallel c++:    $NUM_JOBS jobs"
    if (( NUM_JOBS == 1 )); then
        echo
        echo "WARNING: Docker has < 12 GiB. Build will use single-threaded c++"
        echo "compilation and may take 1-2 hours. To speed it up, increase memory"
        echo "in Docker Desktop → Settings → Resources → Memory."
    fi
    echo

    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

    echo "Starting build container..."
    docker run \
        --name "$CONTAINER_NAME" \
        --platform linux/amd64 \
        --memory-swap=-1 \
        -v "$ROCKY_SRC:/work" \
        -w /work \
        -e NUM_JOBS="$NUM_JOBS" \
        -e CARGO_BUILD_JOBS="$NUM_JOBS" \
        -e CC=clang \
        -e CXX=clang++ \
        -e CFLAGS=-O2 \
        -e CXXFLAGS=-O2 \
        "$RUST_IMAGE" \
        bash -c '
            set -e
            apt-get update -qq
            apt-get install -y -qq cmake clang >/dev/null
            cargo build --release --bin rocky --target-dir /work/target-linux
        '

    BUILT_BINARY="$ROCKY_SRC/target-linux/release/rocky"
fi

# ---------------------------------------------------------------------------
# Verify and install
# ---------------------------------------------------------------------------
if [[ ! -f "$BUILT_BINARY" ]]; then
    echo "ERROR: Build finished but binary not found at $BUILT_BINARY" >&2
    exit 1
fi

file_output=$(file "$BUILT_BINARY")
if [[ "$file_output" != *"ELF 64-bit"*"x86-64"* ]]; then
    echo "ERROR: Built binary is not Linux ELF x86-64:" >&2
    echo "  $file_output" >&2
    exit 1
fi

mkdir -p "$(dirname "$VENDOR_DEST")"
cp "$BUILT_BINARY" "$VENDOR_DEST"
chmod +x "$VENDOR_DEST"

echo
echo "✓ Built and installed:"
echo "  $VENDOR_DEST"
echo "  $(file "$VENDOR_DEST")"
echo "  $(du -h "$VENDOR_DEST" | cut -f1)"
echo
echo "Next step: copy the binary into a consumer project, e.g.:"
echo "  cp $VENDOR_DEST /path/to/consumer/vendor/"
