#!/bin/sh
# Install Rocky from source — local development
# Usage: ./install-dev.sh
#   or:  ./install-dev.sh --release

set -e

INSTALL_DIR="${ROCKY_INSTALL_DIR:-$HOME/.local/bin}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Parse args
BUILD_MODE="debug"
CARGO_FLAGS=""
for arg in "$@"; do
    case "$arg" in
        --release) BUILD_MODE="release"; CARGO_FLAGS="--release" ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Build and install Rocky from source."
            echo ""
            echo "Options:"
            echo "  --release    Build in release mode (slower build, faster binary)"
            echo "  -h, --help   Show this help"
            echo ""
            echo "Environment:"
            echo "  ROCKY_INSTALL_DIR   Install directory (default: ~/.local/bin)"
            exit 0 ;;
        *)
            echo "Unknown option: $arg" >&2
            exit 1 ;;
    esac
done

# Check for Rust toolchain
if ! command -v cargo >/dev/null 2>&1; then
    echo "Error: cargo is not installed." >&2
    echo "Install Rust: https://rustup.rs" >&2
    exit 1
fi

echo "Building Rocky (${BUILD_MODE})..."

# Build
cd "${SCRIPT_DIR}"
cargo build ${CARGO_FLAGS} -p rocky 2>&1

# Locate binary
if [ "$BUILD_MODE" = "release" ]; then
    BINARY="${SCRIPT_DIR}/target/release/rocky"
else
    BINARY="${SCRIPT_DIR}/target/debug/rocky"
fi

if [ ! -f "${BINARY}" ]; then
    echo "Error: Build succeeded but binary not found at ${BINARY}" >&2
    exit 1
fi

# Install
mkdir -p "${INSTALL_DIR}"
cp "${BINARY}" "${INSTALL_DIR}/rocky"
chmod +x "${INSTALL_DIR}/rocky"

# Verify
if "${INSTALL_DIR}/rocky" --version >/dev/null 2>&1; then
    VERSION_STR=$("${INSTALL_DIR}/rocky" --version)
    echo "${VERSION_STR} installed to ${INSTALL_DIR}/rocky"
else
    echo "Rocky installed to ${INSTALL_DIR}/rocky"
fi

# Check PATH
if ! echo "${PATH}" | grep -q "${INSTALL_DIR}"; then
    echo ""
    SHELL_NAME=$(basename "${SHELL:-/bin/sh}")
    case "${SHELL_NAME}" in
        zsh)  PROFILE="~/.zshrc" ;;
        bash) PROFILE="~/.bashrc" ;;
        fish) PROFILE="~/.config/fish/config.fish" ;;
        *)    PROFILE="your shell profile" ;;
    esac
    echo "Add Rocky to your PATH:"
    echo ""
    if [ "${SHELL_NAME}" = "fish" ]; then
        echo "  fish_add_path ${INSTALL_DIR}"
    else
        echo "  echo 'export PATH=\"${INSTALL_DIR}:\$PATH\"' >> ${PROFILE}"
    fi
    echo ""
    echo "Then restart your terminal or run: export PATH=\"${INSTALL_DIR}:\$PATH\""
fi
