---
name: rocky-release
description: Tag-namespaced release workflow for the Rocky monorepo. Use when cutting a release for the engine binary, the `dagster-rocky` wheel, or the VS Code extension. Explains the local-build-first pattern, tag conventions, and what runs in CI vs locally.
---

# Rocky release workflow

Three artifacts ship independently from one monorepo, each with its own tag namespace:

| Artifact | Tag | Destination |
|---|---|---|
| Engine binary (`rocky`) | `engine-v<version>` | GitHub Release (macOS + Linux + Windows) |
| `dagster-rocky` wheel | `dagster-v<version>` | GitHub Release, optionally PyPI |
| Rocky VS Code extension | `vscode-v<version>` | GitHub Release, optionally VS Code Marketplace |

**Never** tag a release as bare `v0.1.0` — the tag namespace is how `engine/install.sh`, `engine/install.ps1`, and downstream consumers filter for their artifact.

## When to use this skill

- Cutting any Rocky release (engine, dagster, vscode)
- Debugging a release failure — the workflow is mostly local, so the fix is usually in `scripts/release.sh`, not in CI
- Understanding what `engine-release.yml` does (Windows-only build)

## The local-build-first pattern

Historically Rocky had full CI-based release workflows. Those were replaced by `scripts/release.sh` because:

- macOS + Linux cargo builds in CI burned minutes unnecessarily when the maintainer builds on macOS anyway.
- PyPI and Marketplace publishing from CI required secret management that wasn't worth it for a solo project.
- Windows is the one exception — cross-compiling Rust for Windows from macOS is painful, so it still runs in CI.

**Current state**: `scripts/release.sh` builds artifacts on your machine and publishes via `gh` CLI. For the engine, macOS + Linux binaries are built locally; pushing the tag triggers `.github/workflows/engine-release.yml`, which builds **only Windows** and attaches it to the already-created GitHub Release.

## Prerequisites

| Artifact | Tools |
|---|---|
| All | `gh` CLI (authenticated to `rocky-data/rocky`) |
| Engine | `cargo`, Docker (for Linux cross-compile on macOS) |
| Dagster | `uv`; `--publish` needs PyPI token in `~/.pypirc` or `UV_PUBLISH_TOKEN` |
| VS Code | `npm`, `npx`; `--publish` needs `VSCE_PAT` env var |

## Engine release

```bash
just release-engine 0.2.0
# or:
./scripts/release.sh engine 0.2.0
```

This:
1. Builds `x86_64-unknown-linux-gnu` via Docker.
2. Builds `aarch64-apple-darwin` and `x86_64-apple-darwin` locally.
3. Creates the GitHub Release `engine-v0.2.0` with `--generate-notes`.
4. Uploads the macOS + Linux tarballs.
5. The tag push triggers `engine-release.yml`, which builds Windows and appends it to the same release.

After the run, verify:
- `gh release view engine-v0.2.0 --repo rocky-data/rocky` shows all three platforms
- `engine/install.sh` and `engine/install.ps1` can resolve the new version (they filter releases by the `engine-v*` prefix)

## Dagster release

```bash
just release-dagster 0.4.0                # GH release only
just release-dagster 0.4.0 --publish      # + publish to PyPI
```

This builds the wheel + sdist via `uv build`, creates the `dagster-v0.4.0` GitHub Release, uploads the artifacts, and (with `--publish`) pushes to PyPI via `uv publish`.

## VS Code release

```bash
just release-vscode 0.3.0                 # GH release only
just release-vscode 0.3.0 --publish       # + publish to VS Code Marketplace
```

This packages the VSIX via `npx vsce`, creates the `vscode-v0.3.0` GitHub Release, uploads the VSIX, and (with `--publish`) pushes to the Marketplace via `vsce publish`.

## Pre-flight: what to check before tagging

Run before any release:

```bash
# 1. Everything builds + tests
just build
just test
just lint

# 2. Codegen is clean (no drift)
just codegen
git status     # should show no diff

# 3. Changelog updated
# For engine releases: engine/CHANGELOG.md
# For dagster: integrations/dagster/CHANGELOG.md
# For vscode: editors/vscode/CHANGELOG.md

# 4. Version numbers bumped
# engine:  engine/Cargo.toml (workspace package.version) + engine/rocky/Cargo.toml
# dagster: integrations/dagster/pyproject.toml
# vscode:  editors/vscode/package.json
```

## Version bump + tag commit

Rocky uses a single "release" commit per artifact that bumps the version file + updates the changelog, committed directly to `main`:

```
chore(engine): release 0.2.0
chore(dagster): release 0.4.0
chore(vscode): release 0.3.0
```

`scripts/release.sh` does NOT bump versions for you — that's a manual step before you run it. It WILL refuse to proceed if the tag already exists (`confirm_tag()` in `release.sh`).

## Common pitfalls

- **Forgetting the namespace**: `v0.2.0` instead of `engine-v0.2.0`. The install scripts filter by prefix; a bare tag is invisible to them.
- **Tag already exists**: `release.sh` hard-aborts. Delete the tag and retag, or bump the version.
- **Dirty codegen**: `just codegen` produced a diff that wasn't committed — the drift CI will retroactively fail.
- **Docker not running** (engine): Linux cross-compile silently fails.
- **Stale binary in `vendor/`**: downstream consumers that vendor the rocky binary via `scripts/vendor_rocky.sh` need a re-run after a release if they pin to a vendored copy.

## CI surface

Path-filtered workflows in `.github/workflows/`:

- `engine-ci.yml` — test + clippy + fmt on every PR touching `engine/**`
- `engine-weekly.yml` — coverage (tarpaulin) + cargo-audit, Monday schedule + manual dispatch
- `engine-bench.yml` — only PRs labeled `perf` touching `engine/crates/**` or `engine/Cargo.*`
- `engine-release.yml` — Windows-only build on tag `engine-v*` (triggered by the release creation event, not tag push)
- `engine-docs.yml` — build + deploy Astro docs from `docs/` to GitHub Pages
- `codegen-drift.yml` — fails any PR where committed bindings drift from `just codegen` output

## Post-release checklist

- [ ] `gh release view <tag>` shows all expected artifacts
- [ ] Install script (`engine/install.sh` or `install.ps1`) resolves and installs the new version on a clean machine
- [ ] Downstream consumers that vendor the binary + Python wheel atomically have been updated — see `scripts/vendor_rocky.sh` for the vendoring workflow
- [ ] Changelog merged to `main`
- [ ] Announcement, if public-facing (blog, release notes)
