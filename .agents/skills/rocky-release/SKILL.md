---
name: rocky-release
description: Tag-namespaced release workflow for the Rocky monorepo. All four artifacts (engine, rocky-sdk, dagster-rocky, vscode) are CI-driven — land a release PR with the version bump + CHANGELOG, tag the merged commit, push the tag, and the matching release workflow handles everything. Use when cutting any Rocky release.
---

# Rocky release workflow

Four artifacts ship independently from one monorepo, each with its own tag namespace:

| Artifact | Tag | Destination | Build path |
|---|---|---|---|
| Engine binaries (`rocky` + `rocky-lsp`) | `engine-v<version>` | GitHub Release (5 targets x 2 binaries = 10 archives) | **CI** — `engine-release.yml` |
| `rocky-sdk` wheel | `sdk-v<version>` | GitHub Release + PyPI | **CI** — `sdk-release.yml` (OIDC → PyPI) |
| `dagster-rocky` wheel | `dagster-v<version>` | GitHub Release + PyPI | **CI** — `dagster-release.yml` (OIDC → PyPI) |
| Rocky VS Code extension | `vscode-v<version>` | GitHub Release + VS Code Marketplace | **CI** — `vscode-release.yml` (`VSCE_PAT` secret → Marketplace) |

**Never** tag a release as bare `v0.1.0` — the tag namespace is how `engine/install.sh`, `engine/install.ps1`, and downstream consumers filter for their artifact.

## When to use this skill

- Cutting any Rocky release (engine, sdk, dagster, vscode)
- Debugging a release failure — the failing job is always in the relevant `*-release.yml` logs. A failed run leaves the GitHub Release as a draft; re-run the failed job. If a sdk/dagster/vscode job failed *after* its registry upload, the re-run fails on the duplicate upload: attach the artifacts with `gh release upload` and publish with `gh release edit <tag> --draft=false --latest=false` by hand.
- Deciding whether a release needs the local-build fallback (only when CI runner credits are exhausted or a workflow itself is broken)

## The flow: release PR → merge → tag → push

All four artifacts follow the same pattern:

1. **Land a release PR** that bumps the version file(s) + updates the changelog.
2. **Stop for the maintainer's go-ahead.** Do not tag, push a tag, or run a manual publish until the maintainer approves the release in chat. This applies to every agent, not only the orchestrator.
3. **Tag the merged commit** with the namespaced tag (`engine-v*`, `sdk-v*`, `dagster-v*`, `vscode-v*`).
4. **Push the tag** — this triggers the matching `*-release.yml` workflow.

The workflow handles the GitHub Release creation, build, and (for dagster/vscode) the publish to the external registry.

A manual publish (`gh release edit <tag> --draft=false`, or a hand upload) is a release action. It also waits for the maintainer's go-ahead.

`scripts/release.sh` + the `just release-engine|sdk|dagster|vscode` recipes survive as **local-build fallbacks** when CI is unavailable. The local path creates the GH Release as a **draft** with its artifacts attached. The tag push's CI run accepts that draft and publishes it. `ensure-release` refuses a release that is already published, so CI never attaches to, or re-publishes, a live release.

Every release is a draft until its last step — all five workflows and every local path. No path publishes a GitHub Release before every artifact it lists exists and the registry upload (PyPI, Marketplace, npm) has succeeded. A failed run leaves a draft, which the public release list omits. Re-run the failed job; the draft is the intended leftover, not a bug.

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
# For sdk: sdk/python/CHANGELOG.md
# For dagster: integrations/dagster/CHANGELOG.md
# For vscode: editors/vscode/CHANGELOG.md

# 4. Version numbers bumped
# engine:  every engine/crates/*/Cargo.toml + engine/rocky/Cargo.toml + engine/rocky-lsp/Cargo.toml (~25 files)
# sdk:     sdk/python/pyproject.toml
# dagster: integrations/dagster/pyproject.toml
# vscode:  editors/vscode/package.json
```

## Common pitfalls

- **Forgetting the namespace**: `v0.2.0` instead of `engine-v0.2.0`. The install scripts filter by prefix; a bare tag is invisible to them.
- **Wrong commit tagged**: verify `git log -1` before tagging — the tag captures HEAD, not main.
- **Missing Cargo.toml bumps**: every crate in `engine/crates/*` must bump. Grep for the old version before pushing the release PR: `grep -rn '^version = "1.2.0"$' engine --include="Cargo.toml"` should return zero after the bump.
- **Missing the `rocky-mcp` bump specifically**: `rocky mcp` announces its own crate version to every MCP client as `serverInfo.version`, so a missed bump makes the server claim an older Rocky than `rocky --version` prints. `engine/rocky/tests/mcp_server_identity.rs` fails the build when the two disagree, which the grep above cannot do (it looks for the OLD version, so a crate that already fell behind is invisible to it).
- **The MCP served-text golden moves on every release**: `rocky-mcp`'s `served_text.golden` pins the `initialize` payload, which carries the version. Expect exactly two rows to change, `default/initialize` and `worker/initialize`, and re-bless with `ROCKY_BLESS_MCP_SERVED_TEXT=1 cargo test -p rocky-mcp --test roundtrip`. Any OTHER row moving in the same diff is a real change to the served text: read it before blessing.
- **Dirty codegen**: `just codegen` produced a diff that wasn't committed — `codegen-drift.yml` CI retroactively fails.
- **Docker not running (fallback only)**: `scripts/build_rocky_linux.sh` silently falls back to zigbuild which has its own issues with `ring` on newer Rust. The `--docker` flag forces the Docker path.

## Per-artifact detail

Read the file for the artifact you release.

| Topic | File |
|---|---|
| Engine: tag, workflow jobs, verify, local-build fallback | [`references/engine.md`](references/engine.md) |
| SDK and Dagster: tag, workflow jobs, ordering rule, local fallback | [`references/sdk-dagster.md`](references/sdk-dagster.md) |
| VS Code: tag, workflow jobs, local fallback | [`references/vscode.md`](references/vscode.md) |
| Tools per artifact, version bump, release commit | [`references/prerequisites-and-versions.md`](references/prerequisites-and-versions.md) |
| Workflows in `.github/workflows/`, post-release checklist | [`references/ci-and-post-release.md`](references/ci-and-post-release.md) |

Each artifact follows the same default: tag the merged commit and push the tag. Example: `git tag -a sdk-v0.2.0 -m "Release sdk-v0.2.0"`, then `git push origin sdk-v0.2.0`.
