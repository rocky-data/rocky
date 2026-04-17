# Contributing to Rocky

Rocky is a monorepo. The four subprojects share one repository, one issue tracker, and one pull-request flow, but each has its own build system.

| Subproject | Path | Language | Build |
|---|---|---|---|
| Rocky CLI engine | `engine/` | Rust (20-crate Cargo workspace) | `cargo` |
| Dagster integration | `integrations/dagster/` | Python | `uv` |
| VS Code extension | `editors/vscode/` | TypeScript | `npm` |
| Sample project | `examples/playground/` | TOML / SQL config | none |

## Getting started

Clone once, work everywhere:

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky
```

Each subproject is built and tested independently. From the repo root, the top-level `justfile` orchestrates common tasks across all of them. Install [`just`](https://github.com/casey/just) and then:

```bash
just build       # build engine + dagster wheel + vscode extension
just test        # run all test suites
just lint        # cargo clippy/fmt + ruff + eslint
just --list      # see all available recipes
```

You can also build a single subproject directly without `just` — see the per-subproject sections below.

### Engine (`engine/`)

```bash
cd engine
cargo build --release
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

The engine is a Cargo workspace with 20 crates (Rust edition 2024, MSRV 1.85). Run a single crate's tests with `cargo test -p rocky-core`. End-to-end tests in `crates/rocky-core/tests/e2e.rs` use DuckDB and need no credentials.

### Dagster integration (`integrations/dagster/`)

```bash
cd integrations/dagster
uv sync --dev
uv run pytest -v
uv run ruff check && uv run ruff format --check
```

All tests run without the Rocky binary or credentials — they use JSON fixtures in `tests/fixtures/`.

### VS Code extension (`editors/vscode/`)

```bash
cd editors/vscode
npm install
npm run compile
npm run test:unit              # vitest unit tests (fast)
npm test                       # full electron integration tests (downloads ~344 MB)
```

For interactive development, open `editors/vscode/` in VS Code and press <kbd>F5</kbd> to launch the Extension Development Host.

### Sample project (`examples/playground/`)

A self-contained DuckDB pipeline used as a smoke test for the engine and as a fixture source for the dagster integration. No build step.

## Cross-project changes

The single biggest reason Rocky is a monorepo: changes to the engine's CLI JSON output schema or DSL syntax can be made atomically across all consumers in one PR.

**When modifying CLI JSON output** (codegen-driven as of Phase 2):
1. Edit the typed `*Output` struct in `engine/crates/rocky-cli/src/output.rs` (or `engine/crates/rocky-cli/src/commands/doctor.rs` for the doctor types).
2. From the repo root, run `just codegen`. This regenerates:
   - `schemas/<command>.schema.json` (canonical JSON Schema)
   - `integrations/dagster/src/dagster_rocky/types_generated/` (Pydantic v2 models)
   - `editors/vscode/src/types/generated/` (TypeScript interfaces)
3. Commit the regenerated bindings together with your Rust change.
4. The `codegen-drift` CI workflow will fail any PR where the committed bindings don't match what the engine produces, so this is enforced.

The hand-written `integrations/dagster/src/dagster_rocky/types.py` and `editors/vscode/src/types/rockyJson.ts` are now re-export shims over the generated modules, providing backward-compat aliases for the renamed Python/TypeScript class names.

**When modifying Rocky DSL syntax** (`.rocky` files), update in lockstep:
1. `engine/crates/rocky-lang/` (parser + lexer)
2. `engine/crates/rocky-compiler/` (type checking)
3. `editors/vscode/syntaxes/rocky.tmLanguage.json` (TextMate grammar)
4. `editors/vscode/snippets/rocky.json` (snippets)

## Releases

Each artifact is released independently using a tag-namespaced scheme. Releases are **CI-driven** — land a release PR (version bump + CHANGELOG entry), tag the merged commit, push the tag, and the matching release workflow does the rest:

| Artifact | Tag pattern | CI workflow (on tag push) |
|---|---|---|
| Rocky CLI binary | `engine-v*` | `engine-release.yml` — full 5-target matrix (macOS ARM64/Intel, Linux x86_64/ARM64, Windows x86_64), all attached to the GitHub Release |
| dagster-rocky wheel | `dagster-v*` | `dagster-release.yml` — build + PyPI publish via OIDC |
| Rocky VSIX | `vscode-v*` | `vscode-release.yml` — build + VS Code Marketplace publish |

Canonical flow for each artifact:

```bash
# 1. Bump the version + update the CHANGELOG in a release PR; merge it.
# 2. Tag the merged commit and push:
git tag engine-v1.7.0
git push origin engine-v1.7.0
```

For convenience, the monorepo exposes `just release-engine <version>`, `just release-dagster <version> [--publish]`, and `just release-vscode <version> [--publish]` — these wrap the local-build path below. The `rocky-release` Claude skill in `.claude/skills/rocky-release/SKILL.md` walks the full checklist.

`scripts/release.sh engine|dagster|vscode <version>` remains as a **local-build fallback** for hotfix scenarios where CI is unavailable. It builds what it can locally (macOS natively, Linux via Docker) and attaches those artifacts to the GitHub Release. Prefer the CI-driven flow for normal releases.

## Pull requests

- Branch from `main`.
- One logical change per PR. Cross-project PRs are encouraged when they ship a coordinated change (schema or DSL); otherwise keep changes scoped to one subproject.
- Conventional commits required: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`. Scope by subproject or crate when relevant: `feat(engine/rocky-databricks): add OAuth M2M auth`, `fix(dagster): handle partial-success exit codes`, `docs(vscode): update README screenshots`.
- **Never** include `Co-Authored-By` trailers in commit messages.
- CI runs path-filtered workflows for the subprojects you touch. All required checks must pass before merge. Note: benchmarks only run on PRs labeled `perf`; coverage and audit run weekly via `engine-weekly.yml`.

### Merge strategy

GitHub lets you pick a merge strategy per-PR via the dropdown on the merge button. Rocky's default is **squash and merge**, but choose deliberately — the right choice depends on the PR's shape:

| PR shape | Strategy | Why |
|---|---|---|
| Single commit | **Squash** | Mechanically equivalent to rebase for a single-commit PR; you get the `(#N)` suffix on `main` for PR traceability |
| Docs, chore, tooling, small fixes | **Squash** | No `git bisect` value from intra-PR granularity; one atomic revert point is easier |
| Multi-commit feature crossing subprojects (DSL cascade, new adapter, new CLI command) | **Rebase** | Preserves per-subproject conventional-commit scopes in `git log` (e.g. `feat(engine/rocky-lang):` → `feat(engine/rocky-compiler):` → `feat(vscode):`) so `git log --grep` by crate still works |
| Refactor series where intermediate states build meaningfully | **Rebase** | Preserves the step-by-step narrative and keeps `git bisect` granular for debugging future regressions |
| WIP-heavy branches with "fix typo" / "oops" commits | **Squash** (or clean up via interactive rebase before opening the PR) | Collapses noise into one coherent commit |

**Heuristic**: if every commit in your PR has a distinct meaningful conventional-commit scope, **rebase** — squashing would collapse those scopes into one megacommit and you'd lose the grep-by-crate affordance. Otherwise **squash**.

Avoid the third option (create a merge commit) for normal PRs — it adds a branching structure to trunk-based linear history for no practical benefit in a solo-scale project.

## Code style

Each subproject follows its language's idioms; the linter is the source of truth.

- **Rust** (`engine/`): edition 2024, `cargo fmt`, `cargo clippy --all-targets -- -D warnings`. Use `tracing` for logs. Use `thiserror` for library errors and `anyhow` for binary/CLI errors. SQL identifiers must be validated via `rocky-sql/validation.rs` before interpolation.
- **Python** (`integrations/dagster/`): Python 3.11+, `from __future__ import annotations`, Pydantic for all data structures. Line length 100. Ruff rules: E, F, I, N, UP, B, SIM.
- **TypeScript** (`editors/vscode/`): ES2022 target, strict mode, `cp.execFile()` (never `cp.exec()`), escape HTML in webview content.

## Reporting issues

File issues against `rocky-data/rocky` with a label naming the subproject (`engine`, `dagster`, `vscode`, `playground`). Include the subproject's version, your platform, and minimal repro steps.
