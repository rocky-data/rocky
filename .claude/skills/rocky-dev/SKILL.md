---
name: rocky-dev
description: Top-level router for Rocky development tasks. Use this FIRST for any request to change Rocky — it points at the right subskill based on what's being touched. Also use for quick monorepo orientation (build, test, lint, install, where-is-what).
---

# Rocky dev router

Rocky is a monorepo with three shipping subprojects + a playground catalog. Most non-trivial changes cross subproject boundaries, and the cascade rules are easy to forget. Use this skill as a router.

## The subprojects at a glance

| Path | Language | Ships as | Build | Test |
|---|---|---|---|---|
| `engine/` | Rust (20 crates) | `rocky` CLI binary | `cargo build --release` | `cargo test` |
| `integrations/dagster/` | Python | `dagster-rocky` wheel | `uv build --wheel` | `uv run pytest` |
| `editors/vscode/` | TypeScript | Rocky VSIX | `npm run compile` | `npm run test:unit` |
| `examples/playground/` | Config | (not published) | `./run.sh` per POC | `./scripts/run-all-duckdb.sh` |

Each has its own `CLAUDE.md` with subproject-specific coding standards and commands. **Read the subproject `CLAUDE.md` before editing it** — they're kept current.

## Monorepo commands (from root)

```bash
just build       # cargo build --release + uv build --wheel + npm compile
just test        # cargo test + pytest + vitest (vscode unit tests only)
just lint        # cargo clippy/fmt + ruff + eslint
just codegen     # Rust → JSON schemas → Pydantic + TypeScript
just --list      # all recipes
```

Each subproject's native tool (`cargo`, `uv`, `npm`) also works directly from inside the subproject dir. The `justfile` is convenience, not a requirement.

## Route by task

### "Change the CLI's JSON output" (add field, new command, rename)
→ **`rocky-codegen`** skill. You'll edit Rust in `engine/crates/rocky-cli/src/output.rs` then run `just codegen`. The CI drift check will fail the PR if you skip this.

### "Add a new `rocky <command>` subcommand"
→ **`rocky-new-cli-command`** skill. This is the superset that walks through engine impl + codegen + dagster resource method + vscode command registration.

### "Change Rocky DSL syntax (`.rocky` files)"
→ **`rocky-dsl-change`** skill. Lockstep change across `engine/rocky-lang`, `engine/rocky-compiler`, `editors/vscode/syntaxes/rocky.tmLanguage.json`, `editors/vscode/snippets/rocky.json`, and `docs/src/content/docs/rocky-lang-spec.md`.

### "Add a new warehouse or source adapter"
→ **`rocky-new-adapter`** skill. New crate under `engine/crates/rocky-<name>/`, implementing traits from `rocky-adapter-sdk`, with conformance tests. Template crates: `rocky-databricks`, `rocky-snowflake`, `rocky-duckdb`, `rocky-fivetran`.

### "Demo a feature with a POC"
→ **`rocky-poc`** skill. Scaffold with `examples/playground/scripts/new-poc.sh <category> <id-name>`. Defaults to DuckDB; credential-gated POCs must fail fast. Do NOT duplicate anything from `engine/examples/` (official starter set).

### "Write or review a `rocky.toml`"
→ **`rocky-config`** skill. Covers the 4 pipeline types (replication / transformation / quality / snapshot), adapter variants (duckdb / databricks / snowflake / fivetran), minimal-config defaults, env-var substitution (`${VAR:-default}`), governance, checks, contracts, state backends, hooks. Also lists the pre-Phase-2 legacy keys that no longer work (`[source]`, `[warehouse]`, `[replication]`, `[checks]` at top level).

### "Cut a release"
→ **`rocky-release`** skill. Tag-namespaced (`engine-v*`, `dagster-v*`, `vscode-v*`). Local build via `scripts/release.sh`; only Windows runs in CI for engine releases. Use `just release-engine`, `just release-dagster`, `just release-vscode`.

### "Change Databricks / Snowflake / Fivetran integration behavior"
→ Use the `databricks-api`, `fivetran-api`, or engine-local `rocky` skills in `engine/.claude/skills/` — they're activated automatically when you're in the engine directory and contain the REST API references.

### "Something Dagster-specific"
→ Also install and use the **`dagster-expert`** Claude Code skill (marketplace). The integration at `integrations/dagster/` leans on Dagster's current component/pipes/asset model, which evolves quickly.

## Cross-cascade reference table

When a change touches X, what else moves?

| You change | Also touch |
|---|---|
| `engine/crates/rocky-cli/src/output.rs` (any `*Output` struct) | Run `just codegen` — auto-regenerates `schemas/`, `integrations/dagster/.../types_generated/`, `editors/vscode/src/types/generated/`. Then re-export in `integrations/dagster/src/dagster_rocky/types.py` and add a dispatch row in `parse_rocky_output()`. |
| A `.rocky` keyword or operator | `engine/rocky-lang/src/{token,parser,lower}.rs`, `engine/rocky-compiler/src/`, `editors/vscode/syntaxes/rocky.tmLanguage.json`, `editors/vscode/snippets/rocky.json`, `docs/src/content/docs/rocky-lang-spec.md` |
| A new CLI command | Engine impl + `rocky-cli/src/output.rs` struct + `rocky-cli/src/commands/export_schemas.rs` registration + `just codegen` + dagster `resource.py` method + vscode `src/commands/<group>.ts` + `package.json` contribution |
| A new adapter | New crate in `engine/crates/` + `engine/Cargo.toml` members + conformance test hookup + factory registration in `rocky-core/src/config.rs` |
| `rocky.toml` config schema | `engine/crates/rocky-core/src/config.rs` + `editors/vscode/schemas/rocky-config.schema.json` (for IDE autocompletion) |
| Test fixture for dagster | `just regen-fixtures` (writes to `integrations/dagster/tests/fixtures_generated/` for drift detection) or `./scripts/regen_fixtures.sh --in-place` to promote to `fixtures/` (destructive) |

## Optional: install local hooks

```bash
just install-hooks
```

This sets `core.hooksPath` to `.git-hooks/`. The pre-commit hook runs `just codegen` and fails the commit if `engine/crates/rocky-cli/src/output.rs`, `commands/doctor.rs`, or `commands/export_schemas.rs` is staged and the regenerated bindings differ from what's staged — same invariant as the CI drift check, caught locally. Skip with `ROCKY_SKIP_CODEGEN_HOOK=1 git commit …` in emergencies.

## Key invariants

- **Never** commit a Rust `*Output` change without the regenerated bindings — `codegen-drift.yml` will fail the PR.
- **Never** include `Co-Authored-By` trailers in commit messages. Use conventional commits (`feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`), scoped by crate or subproject.
- **Never** `format!()` untrusted input into SQL — use `rocky-sql/validation.rs` to validate identifiers first.
- **Never** hand-edit files under `*/types_generated/` or `*/types/generated/` except the curated `__init__.py` / `index.ts` barrels (those are protected by `git checkout` in the codegen recipes).
- **Never** skip hooks (`--no-verify`). The dagster integration has git hooks under `integrations/dagster/.git-hooks/` for ruff + conventional commit validation + pytest.

## Release cadence

Three artifacts, three tag namespaces, independent release schedules:
- `engine-v*` → GitHub Releases (macOS + Linux local, Windows via `engine-release.yml`)
- `dagster-v*` → GitHub Releases + PyPI (with `--publish`)
- `vscode-v*` → GitHub Releases + VS Code Marketplace (with `--publish`)

See the `rocky-release` skill for the full checklist.

## Reference files worth remembering

- `CLAUDE.md` (root) — monorepo-wide cascade rules
- `engine/CLAUDE.md` — engine coding standards, JSON output schema table, Databricks SQL patterns, auth flows
- `integrations/dagster/CLAUDE.md` — dagster layer architecture, "Adding support for a new Rocky CLI command" 9-step checklist
- `editors/vscode/CLAUDE.md` — vscode extension architecture, 25 command list
- `examples/playground/CLAUDE.md` — POC authoring conventions
- `justfile` (root) — orchestration recipes (build, test, lint, codegen, release, fixtures)
- `.github/workflows/` — path-filtered CI per subproject
