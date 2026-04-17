---
name: rocky-release
description: Tag-namespaced release workflow for the Rocky monorepo. All three artifacts (engine, dagster-rocky, vscode) are CI-driven — land a release PR with the version bump + CHANGELOG, tag the merged commit, push the tag, and the matching release workflow handles everything. Use when cutting any Rocky release.
---

# Rocky release workflow

Three artifacts ship independently from one monorepo, each with its own tag namespace:

| Artifact | Tag | Destination | Build path |
|---|---|---|---|
| Engine binary (`rocky`) | `engine-v<version>` | GitHub Release (5 platforms) | **CI** — `engine-release.yml` |
| `dagster-rocky` wheel | `dagster-v<version>` | GitHub Release + PyPI | **CI** — `dagster-release.yml` (OIDC → PyPI) |
| Rocky VS Code extension | `vscode-v<version>` | GitHub Release + VS Code Marketplace | **CI** — `vscode-release.yml` (`VSCE_PAT` secret → Marketplace) |

**Never** tag a release as bare `v0.1.0` — the tag namespace is how `engine/install.sh`, `engine/install.ps1`, and downstream consumers filter for their artifact.

## When to use this skill

- Cutting any Rocky release (engine, dagster, vscode)
- Debugging a release failure — the failing job is always in the relevant `*-release.yml` logs
- Deciding whether a release needs the local-build fallback (only when CI runner credits are exhausted or a workflow itself is broken)

## The flow: release PR → merge → tag → push

All three artifacts follow the same pattern:

1. **Land a release PR** that bumps the version file(s) + updates the changelog.
2. **Tag the merged commit** with the namespaced tag (`engine-v*`, `dagster-v*`, `vscode-v*`).
3. **Push the tag** — this triggers the matching `*-release.yml` workflow.

The workflow handles the GitHub Release creation, build, and (for dagster/vscode) the publish to the external registry.

`scripts/release.sh` + the `just release-engine|dagster|vscode` recipes survive as **local-build fallbacks** when CI is unavailable. The local path also works — it creates the GH Release + uploads artifacts, and `ensure-release` is idempotent.

## Engine release (default: just tag and push)

```bash
# 1. Bump versions + changelog in a PR, merge to main (see "Pre-flight" below).
# 2. From main at the commit you want to release:
git tag -a engine-v0.2.0 -m "Release engine-v0.2.0"
git push origin engine-v0.2.0
```

That's it. The tag push triggers `engine-release.yml`, which:

1. `ensure-release` — creates the `engine-v0.2.0` GitHub Release if missing (`--generate-notes`).
2. `build` matrix — runs on macos-14, ubuntu-24.04, and windows-2022. Each produces a tarball (or `.zip` for Windows) named `rocky-<target>.tar.gz`.
3. `checksums` — generates `SHA256SUMS` and uploads it alongside the binaries.

Total elapsed: ~15–25 min. Watch with:

```bash
gh run watch $(gh run list --workflow=engine-release.yml --limit=1 --json databaseId --jq '.[0].databaseId')
```

After the run, verify:

- `gh release view engine-v0.2.0 --repo rocky-data/rocky` shows 5 platform archives + `SHA256SUMS`
- `engine/install.sh` and `engine/install.ps1` resolve the new version (they filter releases by the `engine-v*` prefix)

### Engine fallback: local build (only when CI is unavailable)

When GitHub Actions credits are exhausted or the CI matrix is broken, `scripts/release.sh` (exposed as `just release-engine <version>`) builds on your laptop:

```bash
just release-engine 0.2.0
# or:
./scripts/release.sh engine 0.2.0
```

This builds macOS locally (`cargo --release`), cross-builds Linux via `cargo-zigbuild` or Docker (`scripts/build_rocky_linux.sh`), creates the GitHub Release with `--generate-notes`, uploads macOS + Linux tarballs, then pushes the tag. The tag push still triggers `engine-release.yml` — if CI is healthy it'll re-build everything and overwrite the local uploads; if CI is broken but the tag-push side-effect you want is just the release itself, the local upload suffices.

**Only reach for this when CI is genuinely unavailable.** It's slower, riskier, and produces artifacts signed by your laptop instead of the GitHub runner.

## Dagster release (default: just tag and push)

```bash
# 1. Bump pyproject.toml + CHANGELOG in a PR, merge to main.
# 2. Tag the merged commit and push:
git tag -a dagster-v0.4.0 -m "Release dagster-v0.4.0"
git push origin dagster-v0.4.0
```

The tag push triggers `dagster-release.yml`, which:

1. `ensure-release` — creates the `dagster-v0.4.0` GitHub Release if missing.
2. `build` — `uv build` produces the wheel + sdist, uploads them to the GH Release.
3. `publish` — `uv publish` pushes to PyPI via **OIDC** (trusted publisher; no token in repo secrets).

### Dagster fallback: local build

```bash
just release-dagster 0.4.0                # GH release only
just release-dagster 0.4.0 --publish      # + PyPI via UV_PUBLISH_TOKEN or ~/.pypirc
```

The local path is idempotent with CI — `ensure-release` detects an existing release. Only reach for this when the CI workflow itself is broken.

## VS Code release (default: just tag and push)

```bash
# 1. Bump package.json + CHANGELOG in a PR, merge to main.
# 2. Tag the merged commit and push:
git tag -a vscode-v0.3.0 -m "Release vscode-v0.3.0"
git push origin vscode-v0.3.0
```

The tag push triggers `vscode-release.yml`, which:

1. `ensure-release` — creates the `vscode-v0.3.0` GitHub Release if missing.
2. `package` — `npx vsce package` produces the VSIX, uploads it to the GH Release.
3. `publish` — `vsce publish` pushes to the VS Code Marketplace using the `VSCE_PAT` repo secret.

### VS Code fallback: local build

```bash
just release-vscode 0.3.0                 # GH release only
just release-vscode 0.3.0 --publish       # + Marketplace via local VSCE_PAT
```

## Prerequisites

| Artifact | Default path (CI) | Local fallback |
|---|---|---|
| Engine | `git` + `gh` CLI | plus `cargo`, `cargo-zigbuild` + `zig` (or Docker) for local Linux cross-compile |
| Dagster | `git` + `gh` CLI; PyPI OIDC trusted-publisher configured on the project | `uv` + `gh`; `--publish` needs `UV_PUBLISH_TOKEN` or `~/.pypirc` |
| VS Code | `git` + `gh` CLI; `VSCE_PAT` configured as a repo secret | `npm`, `npx` + `gh`; `--publish` needs `VSCE_PAT` in the shell environment |

`gh` must be authenticated against `rocky-data/rocky` with release-write permission for all paths.

## Pre-flight: what to check before tagging

Runs before any release:

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
# engine:  every engine/crates/*/Cargo.toml + engine/rocky/Cargo.toml (~19 files)
# dagster: integrations/dagster/pyproject.toml
# vscode:  editors/vscode/package.json
```

## Version bump + tag commit

Rocky uses a single "release" commit per artifact that bumps the version file + updates the changelog. Land it as a PR to `main`, not a direct push:

```
chore(engine): release 0.2.0
chore(dagster): release 0.4.0
chore(vscode): release 0.3.0
```

For engine releases, the PR touches ~19 `Cargo.toml` files — one per crate, plus `engine/rocky/Cargo.toml`. `rocky-bigquery` is tracked on its own version track (currently `0.1.0`) and is not bumped by the engine release PR; prior releases followed the same pattern.

Neither CI (`engine-release.yml`) nor `scripts/release.sh` bump versions for you — that's a manual step before the tag. `scripts/release.sh` WILL refuse to proceed if the tag already exists (`confirm_tag()` in `release.sh`); `engine-release.yml` won't, but the `ensure-release` job will silently attach to the existing release.

## Common pitfalls

- **Forgetting the namespace**: `v0.2.0` instead of `engine-v0.2.0`. The install scripts filter by prefix; a bare tag is invisible to them.
- **Wrong commit tagged**: verify `git log -1` before tagging — the tag captures HEAD, not main.
- **Missing Cargo.toml bumps**: every crate in `engine/crates/*` must bump (except `rocky-bigquery`). Grep for the old version before pushing the release PR: `grep -rn '^version = "1.2.0"$' engine --include="Cargo.toml"` should return zero after the bump.
- **Dirty codegen**: `just codegen` produced a diff that wasn't committed — `codegen-drift.yml` CI retroactively fails.
- **Docker not running (fallback only)**: `scripts/build_rocky_linux.sh` silently falls back to zigbuild which has its own issues with `ring` on newer Rust. The `--docker` flag forces the Docker path.
- **Stale binary in `vendor/`**: downstream consumers that vendor the rocky binary via `scripts/vendor_rocky.sh` need a re-run after a release if they pin to a vendored copy.

## CI surface

Path-filtered workflows in `.github/workflows/`:

- `engine-ci.yml` — test + clippy + fmt on every PR touching `engine/**`
- `engine-weekly.yml` — coverage (tarpaulin) + cargo-audit, Monday schedule + manual dispatch
- `engine-bench.yml` — only PRs labeled `perf` touching `engine/crates/**` or `engine/Cargo.*`
- `engine-release.yml` — **full 5-target matrix build on tag `engine-v*` push**. Owns the GitHub Release creation + binary uploads + SHA256SUMS.
- `engine-docs.yml` — build + deploy Astro docs from `docs/` to GitHub Pages
- `codegen-drift.yml` — fails any PR where committed bindings drift from `just codegen` output

## Post-release checklist

- [ ] `gh release view <tag>` shows all expected artifacts (5 for engine + SHA256SUMS; 2 for dagster; 1 for vscode)
- [ ] Install script (`engine/install.sh` or `install.ps1`) resolves and installs the new version on a clean machine
- [ ] Downstream consumers that vendor the binary + Python wheel atomically have been updated — see `scripts/vendor_rocky.sh` for the vendoring workflow
- [ ] Changelog is on `main` (it merged with the release PR, but double-check)
- [ ] Announcement, if public-facing (blog, release notes)
