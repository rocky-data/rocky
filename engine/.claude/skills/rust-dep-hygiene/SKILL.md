---
name: rust-dep-hygiene
description: Dependency hygiene for the Rocky engine workspace — how to add/update deps via [workspace.dependencies], MSRV 1.85 policy, cargo-audit / cargo-deny / cargo-machete usage, and how the weekly security audit surfaces advisories.
---

# Dependency hygiene for the Rocky engine

## Workspace-dep rule

**All dependencies go through `[workspace.dependencies]` in `engine/Cargo.toml`.** Individual crates under `engine/crates/*` inherit versions by referencing the workspace:

```toml
# In crates/rocky-fivetran/Cargo.toml
[dependencies]
reqwest  = { workspace = true }
tokio    = { workspace = true }
thiserror = { workspace = true }
```

Never do this in a leaf crate:

```toml
# DON'T — bypasses the workspace pin
[dependencies]
reqwest = "0.12"
```

Why: the workspace is 20 crates. Per-crate version pins drift, cause duplicate dep compilations, and break the MSRV contract.

### Adding a new workspace dependency

1. Edit `engine/Cargo.toml` and add the dep to `[workspace.dependencies]` with the exact version and feature set you need.
2. In the specific crate that needs it, add `<dep> = { workspace = true }` (plus a `features = [...]` override if the crate needs a narrower subset — it's allowed to turn features **on**, never off).
3. Run `cargo build -p <crate>` to confirm resolution.
4. Run `cargo tree -d` to check you haven't introduced a duplicate version of anything (see "Duplicates" below).
5. **Never `cargo add` inside a sub-crate without the `--workspace` or without editing `[workspace.dependencies]` first.**

### Upgrading a workspace dependency

```bash
# From engine/
cargo update -p <crate_name>            # Minor/patch only — respects the Cargo.toml version req.
cargo update                            # Everything — use with care, always review Cargo.lock diff.
```

Major version bumps require editing `engine/Cargo.toml` directly and checking the changelog for breaking changes. Pin the version in `[workspace.dependencies]`, not in individual crates.

## MSRV (1.85, Rust 2024 edition)

`engine/Cargo.toml` declares:

```toml
[workspace.package]
edition = "2024"
rust-version = "1.85"
```

**Rules:**

1. **Don't use features newer than 1.85** in any engine crate. If a stable feature lands in 1.86+, either wait for the next MSRV bump or gate your usage behind something that compiles on 1.85.
2. **Dependencies can have their own MSRV.** If a dep requires Rust 1.87 and Rocky is on 1.85, you have two choices: pin an older version of the dep (check `cargo tree` and the dep's changelog) or propose an MSRV bump.
3. **Bumping MSRV is a policy change.** Don't do it unilaterally. Propose to Hugo, note the reason (typically: a dep dropped support for the old MSRV), and bump both the `[workspace.package]` line **and** the CI toolchain installer in `.github/workflows/engine-ci.yml` in the same PR.
4. **Edition bumps** (2024 → 2027 when that exists) are separate from MSRV bumps and even rarer. Don't conflate them.

## Security audit: `cargo audit`

Rocky runs `cargo audit` weekly, not on every PR. From `.github/workflows/engine-weekly.yml`:

```yaml
schedule:
  - cron: '0 8 * * 1'   # Monday 08:00 UTC

- name: Run cargo audit
  run: cargo audit
```

The workflow is also manually dispatchable (`workflow_dispatch`) and marked `continue-on-error: true`, so advisories don't block PRs — they surface as a Monday morning report.

**To run it locally:**

```bash
cargo install cargo-audit     # first time only
cd engine
cargo audit
```

**When to react:**

- `critical` or `high` — fix this week. Either bump the transitive dep (via `cargo update -p` or a workspace bump) or vendor a patch.
- `medium` — schedule a fix in the current iteration.
- `low` / informational — note, don't drop everything.
- `unsound` advisories on a **library** dep are different from vulnerabilities — they indicate soundness bugs in the dep, and the right fix is usually a version bump or a report upstream, not a workspace allowlist.

`cargo audit` does **not** currently have an allow-list for accepted advisories in Rocky. If you want to ignore a specific advisory with a reason, that's a policy change — propose it to Hugo first. (The mechanism is `audit.toml` with `ignore = ["RUSTSEC-YYYY-NNNN"]` and a `reason = "..."` line.)

## License / supply-chain checks: `cargo-deny` (not currently configured)

Rocky does **not** ship a `deny.toml` or run `cargo-deny` in CI as of this skill's authoring. The project's license is Apache-2.0 (`engine/Cargo.toml` → `[workspace.package]`). If you want to enforce license compatibility or a banned-deps policy, proposing `cargo-deny` adoption is the right move — but do it as a dedicated PR, not smuggled into an unrelated change. A minimal `deny.toml` for Rocky would cover:

- `licenses`: allow `Apache-2.0`, `MIT`, `BSD-3-Clause`, `ISC`, `Unicode-DFS-2016`; deny `GPL-*` / `AGPL-*` (Rocky is Apache-2.0 and can't take copyleft deps).
- `bans`: ban known-bad crates (e.g. `openssl-sys` when `rustls` is already the chosen TLS stack — Rocky uses `reqwest = { features = ["rustls-tls"] }`).
- `advisories`: fail on `unmaintained` + `unsound` (beyond what `cargo audit` already catches).
- `sources`: restrict to `crates-io` unless explicit git deps are reviewed.

Don't enable this without Hugo's sign-off — it's the kind of change that turns a clean workspace red on day one.

## Unused deps: `cargo-machete`

`cargo-machete` finds unused dependencies per crate. Run it when you've done a large refactor and want to clean up:

```bash
cargo install cargo-machete   # first time only
cd engine
cargo machete
```

**Caveats:**

- `cargo-machete` looks at `Cargo.toml` vs. actual `use` statements. It **sometimes false-positives** on deps that are used only through macros (e.g. `serde_json` via `json!`) or only behind `#[cfg(...)]` feature gates.
- Always review the diff before deleting a dep — especially if the dep is declared at workspace level; removing it from a leaf crate's `Cargo.toml` is fine, but removing it from `[workspace.dependencies]` may break another crate.
- Run `cargo build --all-targets` after any deletion to confirm nothing broke.

## Duplicates: `cargo tree -d`

```bash
cd engine
cargo tree -d             # show duplicate versions of any transitive dep
cargo tree -d -e features # include feature-flag differences
```

Duplicates bloat compile time and binary size. The common causes:

- A workspace dep and a git-pinned dep both pull the same transitive crate at different versions.
- A feature flag causes two copies of a dep with different feature sets.
- A sub-crate declared its own version of a dep that's already in `[workspace.dependencies]`.

Fix by unifying on one version at the workspace level. Sometimes you can't — two upstreams depend on incompatible ranges of the same crate — in which case document it as a known duplicate and move on.

## Dep policy cheatsheet

| Check | Frequency | Blocking? | Tool |
|---|---|---|---|
| Security advisories | Weekly (Monday UTC) | No (`continue-on-error`) | `cargo-audit` |
| MSRV compliance | Every PR | Yes (via CI `cargo build`) | Cargo built-in |
| Clippy on deps | Every PR | Yes (`-D warnings`) | `cargo clippy` — see `rust-clippy-triage` |
| License compatibility | — | Not enforced | `cargo-deny` (not configured) |
| Unused deps | Ad hoc | No | `cargo-machete` |
| Duplicate versions | Ad hoc | No | `cargo tree -d` |

## Related skills

- **`rust-clippy-triage`** — deprecated-API warnings from bumped deps often surface as clippy lints first.
- **`rust-error-handling`** — when upgrading `thiserror` or `anyhow` specifically, read the release notes for breaking changes in derive syntax.
- **`rocky-release`** (monorepo root) — release-time dep pinning and lockfile policy for the `engine-v*` tag.
