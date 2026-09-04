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
    # `rev-parse --verify --quiet` returns non-zero only when the ref is
    # missing or ambiguous; any other git failure (permission denied,
    # corrupt repo) still bubbles up as an error rather than being
    # papered over the way `rev-parse >/dev/null 2>&1` would.
    if git -C "$WORKSPACE_ROOT" rev-parse --verify --quiet "refs/tags/$tag" >/dev/null; then
        die "Tag $tag already exists. Bump the version or delete the tag first."
    fi
    # Also refuse to create a tag that already exists on the remote —
    # otherwise `git push origin <tag>` succeeds silently if the local tag
    # happens to match the remote exactly.
    if git -C "$WORKSPACE_ROOT" ls-remote --exit-code --tags origin "$tag" >/dev/null 2>&1; then
        die "Tag $tag already exists on origin. Bump the version or delete the remote tag first."
    fi
}

create_release() {
    local tag="$1"; shift
    info "Creating GitHub Release $tag as a draft"
    gh release create "$tag" \
        --repo rocky-data/rocky \
        --generate-notes \
        --draft \
        "$@"
}

# Publish a draft that already has every asset attached.
#
# Always a separate step from `create_release`. `gh release create` does keep
# a release with assets as a draft until the uploads finish, but relying on
# that puts the guarantee inside gh's internals, where a version bump could
# move it. Creating a draft and flipping it here states the ordering in this
# script, and matches what the release workflows do.
publish_release() {
    local tag="$1"; shift
    info "Publishing GitHub Release $tag"
    gh release edit "$tag" \
        --repo rocky-data/rocky \
        --draft=false \
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

    # 3. Tag and push. Each step is guarded so a tag-creation failure (e.g.
    # unsigned config, protected ref) never proceeds to `push` on a missing
    # or stale tag.
    info "Creating and pushing tag $tag"
    git -C "$WORKSPACE_ROOT" tag -a "$tag" -m "Release $tag" \
        || die "git tag -a $tag failed — aborting before push"
    git -C "$WORKSPACE_ROOT" push origin "$tag" \
        || die "git push origin $tag failed"

    # 4. Create the GitHub Release as a DRAFT with macOS + Linux.
    # The tag push also triggers engine-release.yml, which builds all 5
    # targets, overwrites these two, adds checksums.txt, and publishes the
    # draft with --latest (the engine binary owns the repo's "Latest" badge).
    # A draft is invisible to engine/install.sh's "latest" lookup, so the
    # public never resolves a release the matrix has not finished, or one it
    # failed. A published release here would be resolvable but not
    # installable: this script builds neither checksums.txt nor the rocky-lsp
    # archives that install.sh downloads.
    create_release "$tag" \
        "$macos_archive" \
        "$linux_archive"

    echo
    info "Engine release $tag created as a DRAFT with macOS ARM64 + Linux x86_64 binaries."
    info "CI will build all 5 targets, attach them with checksums.txt, and publish the draft."
    info "Monitor: gh run list --repo rocky-data/rocky --workflow engine-release"
    info "If CI cannot run, publish by hand: gh release edit $tag --repo rocky-data/rocky --draft=false --latest"
    info "  (install.sh will still fail on it: no checksums.txt and no rocky-lsp archives)"
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

    # 3. Tag and push. Each step is guarded so a tag-creation failure (e.g.
    # unsigned config, protected ref) never proceeds to `push` on a missing
    # or stale tag.
    info "Creating and pushing tag $tag"
    git -C "$WORKSPACE_ROOT" tag -a "$tag" -m "Release $tag" \
        || die "git tag -a $tag failed — aborting before push"
    git -C "$WORKSPACE_ROOT" push origin "$tag" \
        || die "git push origin $tag failed"

    # 4. Create the GitHub Release as a draft with the wheel attached, then
    # publish it in a separate step below.
    # --latest=false: never let a wheel grab the "Latest" badge — the engine
    # release owns it (see release_engine).
    create_release "$tag" \
        --latest=false \
        "$WORKSPACE_ROOT/integrations/dagster/dist/"*

    # Public only once the registry has the artifact AND every asset is
    # attached. With --publish the registry upload happened in step 2 and the
    # create above has finished, so publishing here is safe; without it, the
    # tag push's CI run publishes to the registry and then flips the draft.
    if [[ "$publish" == "--publish" ]]; then
        publish_release "$tag" --latest=false
    fi

    echo
    if [[ "$publish" == "--publish" ]]; then
        info "Dagster release $tag created and published to PyPI."
        info "The tag push's CI run stops at ensure-release (already published); that is expected."
    else
        info "Dagster release $tag created as a DRAFT. CI publishes it after its PyPI upload."
        info "If CI cannot run, publish by hand: gh release edit $tag --repo rocky-data/rocky --draft=false --latest=false"
    fi
}

# --- SDK ---------------------------------------------------------------------

release_sdk() {
    local version="$1"
    local publish="${2:-}"
    local tag="sdk-v${version}"

    require_cmd uv
    require_cmd gh

    confirm_tag "$tag"
    mkdir -p "$DIST_DIR"

    # 1. Build wheel + sdist
    info "Building rocky-sdk wheel"
    (cd "$WORKSPACE_ROOT/sdk/python" && uv build)

    # Copy artifacts to dist/
    cp "$WORKSPACE_ROOT/sdk/python/dist/"* "$DIST_DIR/"
    info "Built: $(ls "$WORKSPACE_ROOT/sdk/python/dist/")"

    # 2. Publish to PyPI (optional). Publish the SDK BEFORE any dagster-rocky
    # release that raises its rocky-sdk floor — the published dagster wheel
    # resolves the SDK from PyPI, not the monorepo path source.
    if [[ "$publish" == "--publish" ]]; then
        info "Publishing to PyPI"
        (cd "$WORKSPACE_ROOT/sdk/python" && uv publish)
    fi

    # 3. Tag and push.
    info "Creating and pushing tag $tag"
    git -C "$WORKSPACE_ROOT" tag -a "$tag" -m "Release $tag" \
        || die "git tag -a $tag failed — aborting before push"
    git -C "$WORKSPACE_ROOT" push origin "$tag" \
        || die "git push origin $tag failed"

    # 4. Create the GitHub Release as a draft with the wheel attached, then
    # publish it in a separate step below.
    # --latest=false: the engine release owns the "Latest" badge.
    create_release "$tag" \
        --latest=false \
        "$WORKSPACE_ROOT/sdk/python/dist/"*

    # Public only once the registry has the artifact AND every asset is
    # attached. With --publish the registry upload happened in step 2 and the
    # create above has finished, so publishing here is safe; without it, the
    # tag push's CI run publishes to the registry and then flips the draft.
    if [[ "$publish" == "--publish" ]]; then
        publish_release "$tag" --latest=false
    fi

    echo
    if [[ "$publish" == "--publish" ]]; then
        info "SDK release $tag created and published to PyPI."
        info "The tag push's CI run stops at ensure-release (already published); that is expected."
    else
        info "SDK release $tag created as a DRAFT. CI publishes it after its PyPI upload."
        info "If CI cannot run, publish by hand: gh release edit $tag --repo rocky-data/rocky --draft=false --latest=false"
    fi
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

    # 3. Tag and push. Each step is guarded so a tag-creation failure (e.g.
    # unsigned config, protected ref) never proceeds to `push` on a missing
    # or stale tag.
    info "Creating and pushing tag $tag"
    git -C "$WORKSPACE_ROOT" tag -a "$tag" -m "Release $tag" \
        || die "git tag -a $tag failed — aborting before push"
    git -C "$WORKSPACE_ROOT" push origin "$tag" \
        || die "git push origin $tag failed"

    # 4. Create the GitHub Release as a draft with the VSIX attached, then
    # publish it in a separate step below.
    # --latest=false: the engine release owns the "Latest" badge.
    create_release "$tag" \
        --latest=false \
        "$WORKSPACE_ROOT/editors/vscode/"*.vsix

    # Public only once the Marketplace has the extension AND every asset is
    # attached. With --publish the Marketplace publish happened in step 2 and
    # the create above has finished, so publishing here is safe; without it,
    # the tag push's CI run publishes and then flips the draft.
    if [[ "$publish" == "--publish" ]]; then
        publish_release "$tag" --latest=false
    fi

    echo
    if [[ "$publish" == "--publish" ]]; then
        info "VS Code release $tag created and published to the Marketplace."
        info "The tag push's CI run stops at ensure-release (already published); that is expected."
    else
        info "VS Code release $tag created as a DRAFT. CI publishes it after its Marketplace upload."
        info "If CI cannot run, publish by hand: gh release edit $tag --repo rocky-data/rocky --draft=false --latest=false"
    fi
}

# --- Main --------------------------------------------------------------------

usage() {
    cat <<'EOF'
Usage: ./scripts/release.sh <component> <version> [--publish]

Components:
  engine  <version>              Build macOS + Linux, create a DRAFT GH release (CI completes + publishes it)
  sdk     <version> [--publish]  Build wheel, create GH release, optionally publish to PyPI
  dagster <version> [--publish]  Build wheel, create GH release, optionally publish to PyPI
  vscode  <version> [--publish]  Build VSIX, create GH release, optionally publish to Marketplace

Examples:
  ./scripts/release.sh engine  0.2.0
  ./scripts/release.sh sdk     0.1.0 --publish
  ./scripts/release.sh dagster 0.4.0 --publish
  ./scripts/release.sh vscode  0.3.0 --publish

Note: release the SDK before any dagster-rocky release that raises its
rocky-sdk floor — the published dagster wheel resolves rocky-sdk from PyPI.
EOF
    exit 1
}

[[ $# -ge 2 ]] || usage

component="$1"
version="$2"
shift 2

case "$component" in
    engine)  release_engine  "$version" ;;
    sdk)     release_sdk     "$version" "${1:-}" ;;
    dagster) release_dagster "$version" "${1:-}" ;;
    vscode)  release_vscode  "$version" "${1:-}" ;;
    *)       die "Unknown component: $component. Use engine, sdk, dagster, or vscode." ;;
esac
