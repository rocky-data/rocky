# Prerequisites and version bump

Part of the `rocky-release` skill. Read `../SKILL.md` first.

## Prerequisites

| Artifact | Default path (CI) | Local fallback |
|---|---|---|
| Engine | `git` + `gh` CLI | plus `cargo`, `cargo-zigbuild` + `zig` (or Docker) for local Linux cross-compile |
| SDK | `git` + `gh` CLI; PyPI OIDC trusted-publisher configured on the project | `uv` + `gh`; `--publish` needs `UV_PUBLISH_TOKEN` or `~/.pypirc` |
| Dagster | `git` + `gh` CLI; PyPI OIDC trusted-publisher configured on the project | `uv` + `gh`; `--publish` needs `UV_PUBLISH_TOKEN` or `~/.pypirc` |
| VS Code | `git` + `gh` CLI; `VSCE_PAT` configured as a repo secret | `npm`, `npx` + `gh`; `--publish` needs `VSCE_PAT` in the shell environment |

`gh` must be authenticated against `rocky-data/rocky` with release-write permission for all paths.

## Version bump + tag commit

Rocky uses a single "release" commit per artifact that bumps the version file + updates the changelog. Land it as a PR to `main`, not a direct push:

```
chore(engine): release 0.2.0
chore(sdk): release 0.2.0
chore(dagster): release 0.4.0
chore(vscode): release 0.3.0
```

For engine releases, the PR touches ~25 `Cargo.toml` files — one per crate (including `rocky-bigquery`), plus `engine/rocky/Cargo.toml` and `engine/rocky-lsp/Cargo.toml`. All crates version in lockstep.

Neither CI (`engine-release.yml`) nor `scripts/release.sh` bump versions for you — that's a manual step before the tag. `scripts/release.sh` WILL refuse to proceed if the tag already exists (`confirm_tag()` in `release.sh`); `engine-release.yml` won't, but its `ensure-release` job attaches only to an existing **draft** and refuses a release that is already published.
