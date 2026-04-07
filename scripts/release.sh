#!/usr/bin/env bash
set -euo pipefail

# Local release fallback — builds artifacts on your machine and publishes them
# to GitHub Releases, PyPI, and/or the VS Code Marketplace.
#
# For engine releases, the primary path is now CI-driven: push an engine-v*
# tag and engine-release.yml builds all 5 platforms. This script remains as a
# hotfix fallback that builds macOS ARM64 + Linux x86_64 locally.
#
# Usage:
#   ./scripts/release.sh engine  0.2.0              # engine macOS + Linux, create GH release
#   ./scripts/release.sh dagster 0.4.0              # build wheel, create GH release
#   ./scripts/release.sh dagster 0.4.0 --publish    # + publish to PyPI
#   ./scripts/release.sh vscode  0.3.0              # build VSIX, create GH release
#   ./scripts/release.sh vscode  0.3.0 --publish    # + publish to VS Code Marketplace
#
# Prerequisites:
#   engine:  cargo, Docker (for Linux cross-compile)
#   dagster: uv;  --publish requires PyPI token in ~/.pypirc or UV_PUBLISH_TOKEN
#   vscode:  npm, npx;  --publish requires VSCE_PAT env var
#   all:     gh CLI (authenticated)

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly WORKSPACE_ROOT="$(dirname "$SCRIPT_DIR")"
readonly DIST_DIR="$WORKSPACE_ROOT/dist"

# --- Helpers -----------------------------------------------------------------

die()  { echo "ERROR: $*" >&2; exit 1; }
info() { echo "==> $*"; }

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "$1 is required but not found"
}

confirm_tag() {
    local tag="$1"
    if git -C "$WORKSPACE_ROOT" rev-parse "refs/tags/$tag" >/dev/null 2>&1; then
        die "Tag $tag already exists. Bump the version or delete the tag first."
    fi
}

create_release() {
    local tag="$1"; shift
    info "Creating GitHub Release $tag"
    gh release create "$tag" \
        --repo rocky-data/rocky \
        --generate-notes \
        "$@"
}

# --- Engine ------------------------------------------------------------------

release_engine() {
    local version="$1"
    local tag="engine-v${version}"

    require_cmd cargo
    require_cmd gh

    confirm_tag "$tag"
    mkdir -p "$DIST_DIR"

    # 1. Build macOS ARM64 (native)
    info "Building rocky for macOS ARM64 (native)"
    (cd "$WORKSPACE_ROOT/engine" && cargo build --release --bin rocky)

    local macos_bin="$WORKSPACE_ROOT/engine/target/release/rocky"
    [[ -f "$macos_bin" ]] || die "macOS build produced no binary at $macos_bin"

    local macos_archive="$DIST_DIR/rocky-aarch64-apple-darwin.tar.gz"
    tar czf "$macos_archive" -C "$(dirname "$macos_bin")" rocky
    info "Packaged $macos_archive ($(du -h "$macos_archive" | cut -f1))"

    # 2. Build Linux x86_64 (Docker)
    info "Building rocky for Linux x86_64 (Docker)"
    "$SCRIPT_DIR/build_rocky_linux.sh"

    local linux_bin="$WORKSPACE_ROOT/vendor/rocky-linux-amd64"
    [[ -f "$linux_bin" ]] || die "Linux build produced no binary at $linux_bin"

    local linux_archive="$DIST_DIR/rocky-x86_64-unknown-linux-gnu.tar.gz"
    tar czf "$linux_archive" -C "$(dirname "$linux_bin")" --transform 's/rocky-linux-amd64/rocky/' rocky-linux-amd64
    info "Packaged $linux_archive ($(du -h "$linux_archive" | cut -f1))"

    # 3. Tag and push
    info "Creating and pushing tag $tag"
    git -C "$WORKSPACE_ROOT" tag -a "$tag" -m "Release $tag"
    git -C "$WORKSPACE_ROOT" push origin "$tag"

    # 4. Create GitHub Release with macOS + Linux
    # The tag push also triggers engine-release.yml which builds all 5 targets.
    # CI will overwrite these two and add macOS Intel, Linux ARM64, and Windows.
    create_release "$tag" \
        "$macos_archive" \
        "$linux_archive"

    echo
    info "Engine release $tag created with macOS ARM64 + Linux x86_64 binaries."
    info "CI will build all 5 targets and attach them to this release."
    info "Monitor: gh run list --repo rocky-data/rocky --workflow engine-release"
}

# --- Dagster -----------------------------------------------------------------

release_dagster() {
    local version="$1"
    local publish="${2:-}"
    local tag="dagster-v${version}"

    require_cmd uv
    require_cmd gh

    confirm_tag "$tag"
    mkdir -p "$DIST_DIR"

    # 1. Build wheel + sdist
    info "Building dagster-rocky wheel"
    (cd "$WORKSPACE_ROOT/integrations/dagster" && uv build)

    # Copy artifacts to dist/
    cp "$WORKSPACE_ROOT/integrations/dagster/dist/"* "$DIST_DIR/"
    info "Built: $(ls "$WORKSPACE_ROOT/integrations/dagster/dist/")"

    # 2. Publish to PyPI (optional)
    if [[ "$publish" == "--publish" ]]; then
        info "Publishing to PyPI"
        (cd "$WORKSPACE_ROOT/integrations/dagster" && uv publish)
    fi

    # 3. Tag and push
    info "Creating and pushing tag $tag"
    git -C "$WORKSPACE_ROOT" tag -a "$tag" -m "Release $tag"
    git -C "$WORKSPACE_ROOT" push origin "$tag"

    # 4. Create GitHub Release
    create_release "$tag" \
        "$WORKSPACE_ROOT/integrations/dagster/dist/"*

    echo
    info "Dagster release $tag created."
    [[ "$publish" == "--publish" ]] && info "Published to PyPI." || info "Skipped PyPI publish (pass --publish to enable)."
}

# --- VS Code -----------------------------------------------------------------

release_vscode() {
    local version="$1"
    local publish="${2:-}"
    local tag="vscode-v${version}"

    require_cmd npm
    require_cmd npx
    require_cmd gh

    confirm_tag "$tag"
    mkdir -p "$DIST_DIR"

    # 1. Build VSIX
    info "Building VS Code extension"
    (cd "$WORKSPACE_ROOT/editors/vscode" && npm ci && npm run compile && npx @vscode/vsce package)

    # Copy VSIX to dist/
    cp "$WORKSPACE_ROOT/editors/vscode/"*.vsix "$DIST_DIR/"
    local vsix
    vsix=$(ls "$WORKSPACE_ROOT/editors/vscode/"*.vsix)
    info "Built: $vsix"

    # 2. Publish to VS Code Marketplace (optional)
    if [[ "$publish" == "--publish" ]]; then
        if [[ -z "${VSCE_PAT:-}" ]]; then
            die "VSCE_PAT env var required for VS Code Marketplace publishing"
        fi
        info "Publishing to VS Code Marketplace"
        (cd "$WORKSPACE_ROOT/editors/vscode" && npx @vscode/vsce publish --pat "$VSCE_PAT")
    fi

    # 3. Tag and push
    info "Creating and pushing tag $tag"
    git -C "$WORKSPACE_ROOT" tag -a "$tag" -m "Release $tag"
    git -C "$WORKSPACE_ROOT" push origin "$tag"

    # 4. Create GitHub Release
    create_release "$tag" \
        "$WORKSPACE_ROOT/editors/vscode/"*.vsix

    echo
    info "VS Code release $tag created."
    [[ "$publish" == "--publish" ]] && info "Published to VS Code Marketplace." || info "Skipped Marketplace publish (pass --publish to enable)."
}

# --- Main --------------------------------------------------------------------

usage() {
    cat <<'EOF'
Usage: ./scripts/release.sh <component> <version> [--publish]

Components:
  engine  <version>              Build macOS + Linux, create GH release (Windows via CI)
  dagster <version> [--publish]  Build wheel, create GH release, optionally publish to PyPI
  vscode  <version> [--publish]  Build VSIX, create GH release, optionally publish to Marketplace

Examples:
  ./scripts/release.sh engine  0.2.0
  ./scripts/release.sh dagster 0.4.0 --publish
  ./scripts/release.sh vscode  0.3.0 --publish
EOF
    exit 1
}

[[ $# -ge 2 ]] || usage

component="$1"
version="$2"
shift 2

case "$component" in
    engine)  release_engine  "$version" ;;
    dagster) release_dagster "$version" "${1:-}" ;;
    vscode)  release_vscode  "$version" "${1:-}" ;;
    *)       die "Unknown component: $component. Use engine, dagster, or vscode." ;;
esac
