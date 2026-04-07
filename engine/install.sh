#!/bin/bash
# Install Rocky — SQL transformation engine
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
#   curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash -s -- 1.0.0
#   curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | ROCKY_INSTALL_DIR=/usr/local/bin bash

set -euo pipefail

REPO="rocky-data/rocky"
# Tag prefix used by the engine release workflow in the rocky monorepo.
# Allows the engine to release independently of dagster-rocky / rocky-vscode.
TAG_PREFIX="engine-v"
INSTALL_DIR="${ROCKY_INSTALL_DIR:-$HOME/.local/bin}"

# Parse optional version argument: bash -s -- [VERSION|stable|latest]
# Accepts bare version (1.0.0), full tag (engine-v1.0.0), or "stable"/"latest".
# ROCKY_VERSION env var takes precedence over the argument for backward compat.
VERSION_ARG="${1:-}"
if [[ -n "${VERSION_ARG}" && -z "${ROCKY_VERSION:-}" ]]; then
    case "${VERSION_ARG}" in
        stable|latest) ;;  # resolve from API below
        engine-v*) ROCKY_VERSION="${VERSION_ARG}" ;;
        *) ROCKY_VERSION="${TAG_PREFIX}${VERSION_ARG}" ;;
    esac
fi

# Check for required dependencies
if command -v curl >/dev/null 2>&1; then
    DOWNLOADER="curl"
elif command -v wget >/dev/null 2>&1; then
    DOWNLOADER="wget"
else
    echo "Error: curl or wget is required but neither is installed." >&2
    exit 1
fi

download_file() {
    local url="$1"
    local output="${2:-}"
    if [[ "${DOWNLOADER}" == "curl" ]]; then
        if [[ -n "${output}" ]]; then
            curl -fsSL -o "${output}" "${url}"
        else
            curl -fsSL "${url}"
        fi
    else
        if [[ -n "${output}" ]]; then
            wget -q -O "${output}" "${url}"
        else
            wget -q -O - "${url}"
        fi
    fi
}

# Detect platform and map to Rust target triple
OS="$(uname -s)"
ARCH="$(uname -m)"

# Detect Rosetta 2: download native arm64 binary instead of x64
if [[ "${OS}" == "Darwin" && "${ARCH}" == "x86_64" ]]; then
    if [[ "$(sysctl -n sysctl.proc_translated 2>/dev/null)" == "1" ]]; then
        ARCH="arm64"
    fi
fi

case "${OS}" in
    MINGW*|MSYS*|CYGWIN*)
        echo "Windows is not supported by this script. Use PowerShell instead:" >&2
        echo "  irm https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.ps1 | iex" >&2
        exit 1 ;;
esac

# Detect musl libc (Alpine Linux and other musl-based distributions)
if [[ "${OS}" == "Linux" ]]; then
    if ldd /bin/sh 2>/dev/null | grep -q musl || \
       [[ -f /lib/libc.musl-x86_64.so.1 || -f /lib/libc.musl-aarch64.so.1 ]]; then
        echo "Error: Pre-built binaries are not available for musl/Alpine Linux." >&2
        echo "  Build from source (requires Rust 1.85+):" >&2
        echo "    git clone https://github.com/rocky-data/rocky.git" >&2
        echo "    cd rocky/engine && cargo build --release" >&2
        exit 1
    fi
fi

case "${OS}-${ARCH}" in
    Darwin-arm64)   TARGET="aarch64-apple-darwin" ;;
    Darwin-x86_64)  TARGET="x86_64-apple-darwin" ;;
    Linux-x86_64)   TARGET="x86_64-unknown-linux-gnu" ;;
    Linux-aarch64)  TARGET="aarch64-unknown-linux-gnu" ;;
    *)
        echo "Unsupported platform: ${OS}/${ARCH}" >&2
        exit 1 ;;
esac

ARCHIVE="rocky-${TARGET}.tar.gz"

# Get latest version (or use ROCKY_VERSION env var / argument).
# The monorepo releases multiple artifacts (engine-v*, dagster-v*, vscode-v*),
# so /releases/latest can return a non-engine tag. Filter by TAG_PREFIX instead.
#
# Capture the full API response before piping through grep — piping directly
# from curl/wget into grep causes set -o pipefail to abort the script when
# grep exits 1 (no match found on old/pre-release tags).
if [[ -z "${ROCKY_VERSION:-}" ]]; then
    API_RESPONSE="$(download_file "https://api.github.com/repos/${REPO}/releases?per_page=30")"
    ROCKY_VERSION="$(echo "${API_RESPONSE}" \
        | grep '"tag_name"' \
        | grep "\"${TAG_PREFIX}" \
        | head -1 \
        | sed 's/.*"tag_name": "//;s/".*//' \
        || true)"
    if [[ -z "${ROCKY_VERSION}" ]]; then
        echo "Error: Failed to determine latest engine version (tag prefix '${TAG_PREFIX}'). Set ROCKY_VERSION manually." >&2
        exit 1
    fi
fi

URL="https://github.com/${REPO}/releases/download/${ROCKY_VERSION}/${ARCHIVE}"
CHECKSUMS_URL="https://github.com/${REPO}/releases/download/${ROCKY_VERSION}/checksums.txt"

echo "Installing Rocky ${ROCKY_VERSION} (${TARGET})..."
echo "  From: ${URL}"
echo "  To:   ${INSTALL_DIR}/rocky"

# Create install directory
mkdir -p "${INSTALL_DIR}"

# Download to a temp directory; clean up unconditionally on exit
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

if ! download_file "${URL}" "${TMP}/${ARCHIVE}"; then
    echo "Error: Download failed. Check that version ${ROCKY_VERSION} exists for ${TARGET}." >&2
    exit 1
fi

# Compute and display checksum, then verify against checksums.txt if available
ACTUAL_HASH=""
if command -v sha256sum >/dev/null 2>&1; then
    ACTUAL_HASH="$(sha256sum "${TMP}/${ARCHIVE}" | cut -d' ' -f1)"
elif command -v shasum >/dev/null 2>&1; then
    ACTUAL_HASH="$(shasum -a 256 "${TMP}/${ARCHIVE}" | cut -d' ' -f1)"
fi

if [[ -n "${ACTUAL_HASH}" ]]; then
    echo "  SHA256: ${ACTUAL_HASH}"
    if download_file "${CHECKSUMS_URL}" "${TMP}/checksums.txt" 2>/dev/null; then
        EXPECTED_HASH="$(grep "${ARCHIVE}" "${TMP}/checksums.txt" | cut -d' ' -f1 || true)"
        if [[ -n "${EXPECTED_HASH}" ]]; then
            if [[ "${ACTUAL_HASH}" != "${EXPECTED_HASH}" ]]; then
                echo "Error: Checksum mismatch!" >&2
                echo "  Expected: ${EXPECTED_HASH}" >&2
                echo "  Actual:   ${ACTUAL_HASH}" >&2
                exit 1
            fi
            echo "  Checksum verified."
        fi
    fi
fi

# Extract and install
tar xzf "${TMP}/${ARCHIVE}" -C "${TMP}"

if [[ ! -f "${TMP}/rocky" ]]; then
    echo "Error: Archive does not contain 'rocky' binary." >&2
    exit 1
fi

mv "${TMP}/rocky" "${INSTALL_DIR}/rocky"
chmod +x "${INSTALL_DIR}/rocky"

echo ""

# Verify installation
if "${INSTALL_DIR}/rocky" --version >/dev/null 2>&1; then
    VERSION_STR="$("${INSTALL_DIR}/rocky" --version)"
    echo "✓ ${VERSION_STR} installed to ${INSTALL_DIR}/rocky"
else
    echo "✓ Rocky installed to ${INSTALL_DIR}/rocky"
fi

# Check PATH
if ! echo "${PATH}" | grep -q "${INSTALL_DIR}"; then
    echo ""
    SHELL_NAME="$(basename "${SHELL:-/bin/sh}")"
    case "${SHELL_NAME}" in
        zsh)  PROFILE="~/.zshrc" ;;
        bash) PROFILE="~/.bashrc" ;;
        fish) PROFILE="~/.config/fish/config.fish" ;;
        *)    PROFILE="your shell profile" ;;
    esac
    echo "Add Rocky to your PATH:"
    echo ""
    if [[ "${SHELL_NAME}" == "fish" ]]; then
        echo "  fish_add_path ${INSTALL_DIR}"
    else
        echo "  echo 'export PATH=\"${INSTALL_DIR}:\$PATH\"' >> ${PROFILE}"
    fi
    echo ""
    echo "Then restart your terminal or run: export PATH=\"${INSTALL_DIR}:\$PATH\""
fi
