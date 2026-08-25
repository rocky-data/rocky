# Contributing to Rocky

Rocky is a monorepo. Every subproject shares one repository, one issue tracker, and one pull-request flow. Each subproject keeps its own build system.

| Subproject | Path | Language | Build |
|---|---|---|---|
| Rocky CLI engine | `engine/` | Rust (multi-crate Cargo workspace) | `cargo` |
| Python SDK | `sdk/python/` | Python | `uv` |
| Dagster integration | `integrations/dagster/` | Python | `uv` |
| VS Code extension | `editors/vscode/` | TypeScript | `npm` |
| Documentation site | `docs/` | Astro | `npm` |
| POC catalog | `examples/playground/` | TOML / SQL config | none |

## Getting started

Clone once, then work anywhere in the tree.

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky
```

The top-level `justfile` runs one task across the four code subprojects: the engine, the SDK, the Dagster integration, and the VS Code extension. It does not cover `docs/` or the playground. Install [`just`](https://github.com/casey/just), then:

```bash
just build       # engine, sdk wheel, dagster wheel, vscode extension
just test        # cargo test, both pytest suites, vscode unit tests
just lint        # cargo clippy/fmt + ruff + eslint
just --list      # every recipe
```

Two gaps to know about. `just test` runs the VS Code unit tests only; the electron suite has its own recipe, `just test-vscode-electron`. `just build` compiles the extension but does not bundle it, so it does not produce the `dist/extension.js` that debugging needs.

You can also build one subproject directly. The sections below give the commands.

Optional: run `just install-hooks` to point `core.hooksPath` at `.git-hooks/`. Every hook check is conditional. A check runs only when the change touches the subproject it covers.

`post-checkout` is a warning, not a check: `git checkout <branch>` does not stop for a dirty worktree — it carries non-conflicting changes across, and they end up committed on whichever branch you landed on. git has no pre-checkout hook, so the switch cannot be refused; the hook makes the carry loud instead of silent.

To start a branch, `scripts/new-branch.sh <name> [base]` refuses on a dirty tree and defaults the base to a freshly-fetched `origin/main`. Branching from whatever happened to be checked out is the other half of the same problem: the new branch inherits the current one's commits, so it conflicts when that branch merges — and a squash-merge of the child can land the parent's work, including its `Closes #NNN` trailers, under the child's number.

- **pre-commit:** `cargo fmt --check` for `engine/`, `ruff format --check` for `integrations/dagster/`, eslint for `editors/vscode/`.
- **pre-commit, codegen drift:** runs only when you stage `output.rs`, `commands/doctor.rs`, or `commands/export_schemas.rs` under `engine/crates/rocky-cli/src/`.
- **pre-push:** `cargo clippy` for `engine/`, `ruff check` for `integrations/dagster/`.

Neither hook checks `sdk/python/`. A check also skips silently when its tool is not installed, so a missing `uv` turns the ruff check into a pass. The drift check compares fewer paths than `codegen-drift.yml` does, so a commit can pass the hook and still fail CI. Treat the hooks as a fast first pass, not as the gate. Set `ROCKY_SKIP_HOOKS=1` to skip every hook, or `ROCKY_SKIP_CODEGEN_HOOK=1` to skip the drift check alone.

### Engine (`engine/`)

```bash
cd engine
cargo build --release
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

CI is stricter than the `cargo test` and `cargo clippy` lines above. It runs `cargo nextest run --all-features` and `cargo clippy --all-targets --all-features -- -D warnings`. Some adapter code sits behind a Cargo feature, so a default-feature run can pass while CI fails.

The engine is a Cargo workspace: the library crates under `engine/crates/` plus the `rocky` and `rocky-lsp` binary crates. It targets Rust edition 2024 with an MSRV of 1.88. Test one crate with `cargo test -p rocky-core`. The end-to-end tests in `crates/rocky-core/tests/e2e.rs` run against DuckDB and need no credentials.

### Python SDK (`sdk/python/`)

```bash
cd sdk/python
uv sync --dev
uv run pytest -v
uv run ruff check src/ tests/ examples/ && uv run ruff format --check src/ tests/ examples/
```

`rocky-sdk` is the standalone typed client (`RockyClient`) that `dagster-rocky` builds on. The unit tests patch the subprocess layer, so they need no `rocky` binary and no credentials. `examples/quickstart.py` runs against a real binary, and the `sdk-ci` smoke job installs one. `just codegen` writes the Pydantic models in `src/rocky_sdk/types_generated/`; do not hand-edit them.

### Dagster integration (`integrations/dagster/`)

```bash
cd integrations/dagster
uv sync --dev
uv run pytest -v
uv run ruff check && uv run ruff format --check
```

Every test runs without the `rocky` binary and without credentials. Hand-written scenario data lives in `tests/scenarios.py` as Python dicts. Captures from a real binary live in `tests/fixtures_generated/`, and `just regen-fixtures` refreshes them.

### VS Code extension (`editors/vscode/`)

```bash
cd editors/vscode
npm install
npm run compile                # tsc, writes out/
npm run bundle                 # esbuild, writes dist/extension.js
npm run test:unit              # vitest unit tests (fast)
npm test                       # electron integration tests (~344 MB download)
```

Run `npm run bundle` before you debug. `package.json` loads the extension from `dist/extension.js`, which esbuild produces; `npm run compile` writes only `out/`. Then open `editors/vscode/` in VS Code and press <kbd>F5</kbd> to launch the Extension Development Host.

### Documentation site (`docs/`)

```bash
cd docs
npm ci
npm run dev      # local preview
npm run build    # the same build CI runs
```

`docs-build.yml` builds the site on every pull request that touches `docs/`. `engine-docs.yml` deploys it on push to `main`.

### POC catalog (`examples/playground/`)

A catalog of small POCs, one per Rocky feature. Each POC is a complete project you can run through `./run.sh`, and most need only DuckDB and no credentials. The weekly `poc-smoke` job builds a fresh binary and runs the credential-free POCs against it. It skips any POC that needs credentials, Docker, or a Rust toolchain. A second step parse-checks the credential-gated POCs, but that step is `continue-on-error`, so it cannot fail the job. `just regen-fixtures` captures the Dagster test fixtures from specific POCs here. The catalog has no build step.

## Cross-project changes

A change to the engine's CLI JSON output, or to the DSL, reaches every consumer. The monorepo lets you land all of it in one pull request.

**When you change CLI JSON output**, edit the typed `*Output` struct in `engine/crates/rocky-cli/src/output.rs`. The doctor types live in `engine/crates/rocky-cli/src/commands/doctor.rs`. Then run `just codegen` from the repo root.

```
 engine/crates/rocky-cli/src/output.rs  (the typed *Output structs)
                 │
                 │ just codegen — builds the rocky binary, then runs:
       ┌─────────┴──────────┐
       │ export-schemas     │ export-openapi
       ▼                    ▼
 schemas/*.schema.json   docs/public/openapi.json
       │
       ├────────────► Pydantic models       (rocky-sdk)
       ├────────────► TypeScript interfaces (VS Code)
       └────────────► project-file schema   (VS Code)
```

The OpenAPI document comes straight from the engine's schema registry and its route table, not from the committed schema files. The other three read `schemas/`:

- `sdk/python/src/rocky_sdk/types_generated/` — Pydantic v2 models
- `editors/vscode/src/types/generated/` — TypeScript interfaces
- `editors/vscode/schemas/rocky-project.schema.json` — copied from `schemas/rocky_project.schema.json`

Commit everything the cascade writes, `schemas/` included, in the same PR as the Rust change. `codegen-drift.yml` re-runs `just codegen` and `just regen-fixtures` on your PR, then fails it on any diff. Use `just codegen-all` locally when your change also alters the shape of command output; it bundles both steps.

Three shims keep older imports working: `dagster_rocky.types` and `dagster_rocky.types_generated` re-export the Pydantic models from `rocky_sdk`, and `editors/vscode/src/types/rockyJson.ts` re-exports the generated TypeScript under its earlier names.

**When you change Rocky DSL syntax** (`.rocky` files), update all five in lockstep:

1. `engine/crates/rocky-lang/` (parser + lexer)
2. `engine/crates/rocky-compiler/` (type checking)
3. `editors/vscode/syntaxes/rocky.tmLanguage.json` (TextMate grammar)
4. `editors/vscode/snippets/rocky.json` (snippets)
5. `docs/src/content/docs/concepts/rocky-dsl.md` + `docs/rocky-lang-spec.md` (published DSL page + full spec)

## Releases

Each artifact ships on its own tag prefix. Releases are CI-driven: you land a release PR, then push a tag, and the matching workflow does the rest.

```
  1. release PR        version bump + CHANGELOG entry
        │
        │ merge
        ▼
  2. commit on main    the commit you will tag
        │
        │ tag it <prefix>-v<x.y.z>, then push the tag
        ▼
  3. release workflow  builds every artifact for that prefix
        │
        │ publish
        ▼
  4. GitHub Release, plus PyPI or the Marketplace for three of them
```

Tag the merge commit, then push the tag. Take the version from the release PR. Git refuses a tag that already exists, so a stale version number fails loudly rather than silently.

```bash
git tag -a engine-v0.2.0 -m "Release engine-v0.2.0"
git push origin engine-v0.2.0
```

| Artifact | Tag pattern | Workflow | Also published to |
|---|---|---|---|
| Rocky CLI binaries | `engine-v*` | `engine-release.yml` | nothing else |
| `rocky-sdk` wheel | `sdk-v*` | `sdk-release.yml` | PyPI, via OIDC |
| `dagster-rocky` wheel | `dagster-v*` | `dagster-release.yml` | PyPI, via OIDC |
| Rocky VSIX | `vscode-v*` | `vscode-release.yml` | VS Code Marketplace |

Each of these four workflows creates a GitHub Release and attaches what it built. Three of them also publish to a package registry.

A fifth workflow, `engine-wasm-release.yml`, builds the compiler pipeline to WebAssembly on an `engine-wasm-v*` tag. Nothing has shipped from it. `@rocky-data/compiler` is not in the npm registry. The one `engine-wasm-v*` tag has no GitHub Release. The publish step exits early, because the repository has no `NPM_TOKEN` secret. Treat the WebAssembly package as unreleased.

The engine matrix builds five targets: macOS ARM64, macOS Intel, Linux x86_64, Linux ARM64, and Windows x86_64. It attaches a `rocky` archive and a `rocky-lsp` archive for each one.

`dagster-rocky` depends on `rocky-sdk`. When a `dagster-v*` release raises its `rocky-sdk` floor, push the `sdk-v*` tag first. The published dagster wheel resolves the SDK from PyPI, not from the path source in this repository.

`scripts/release.sh engine|sdk|dagster|vscode <version>` is a local-build fallback for a hotfix when CI is unavailable. Run the engine path on an Apple Silicon Mac. It packages the native host build as the macOS ARM64 archive without checking the host architecture, and builds Linux x86_64 in Docker.

Four `just` recipes wrap that same script: `just release-engine <version>`, `just release-sdk <version> [--publish]`, `just release-dagster <version> [--publish]`, and `just release-vscode <version> [--publish]`. Without `--publish`, the sdk, dagster, and vscode paths build the artifact and create the GitHub Release, but never reach PyPI or the Marketplace. `just release-engine` takes no `--publish`, because the engine publishes to GitHub Releases only.

Prefer the tag-driven flow for a normal release. The `rocky-release` skill, mirrored at `.agents/skills/rocky-release/` and `.claude/skills/rocky-release/`, walks the full checklist.

## Pull requests

- Branch from `main`.
- One logical change per PR. Cross-project PRs are welcome when they ship a coordinated schema or DSL change; otherwise keep a PR inside one subproject.
- Conventional commits required: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`. Scope by subproject or crate where it helps: `feat(engine/rocky-databricks): add OAuth M2M auth`, `fix(dagster): handle partial-success exit codes`, `docs(vscode): update README screenshots`.
- **Never** include `Co-Authored-By` trailers in commit messages.

### What CI runs

CI is path-filtered. The paths your PR touches decide which workflows run. Every required check must pass before merge.

| What you touch | Workflow | What it runs |
|---|---|---|
| `engine/**` | `engine-ci.yml` | nextest, clippy, `cargo fmt --check`, an adapter-boundary lint, a release-build smoke test |
| `sdk/python/**` | `sdk-ci.yml` | pytest, ruff, and a smoke job driving the SDK against a real `rocky` |
| `integrations/dagster/**` | `dagster-ci.yml` | pytest and ruff |
| `editors/vscode/**` | `vscode-ci.yml` | compile, the electron integration tests, eslint |
| `docs/**` | `docs-build.yml` | an Astro build of the docs site |
| `scripts/**` | `scripts-ci.yml` | shellcheck, and a self-test of the soak-verdict script |
| `.agents/skills/**` or `.claude/skills/**` | `skills-mirror-drift.yml` | diffs the two skill trees and fails unless they are byte-identical |
| `engine/**`, `schemas/**`, `justfile`, `examples/playground/pocs/**`, or a generated binding | `codegen-drift.yml` | re-runs `just codegen` and `just regen-fixtures`, then fails on any diff |
| `examples/playground/pocs/**` | `poc-counts-drift.yml` | recounts the POCs and fails on any diff |
| `engine/evals/**` or `engine/crates/rocky-mcp/**` | `engine-evals.yml` | the eval harness self-test and the structured-error contract |
| the recipe-manifest surface (`rocky-verify`, `recipe_identity.rs`, `history.rs`, `examples/audit-sample/**`) | `manifest-conformance.yml` | builds `rocky` and `rocky-verify`, then runs the conformance script |

The table covers the common cases, not every path. Each workflow in `.github/workflows/` holds its own exact `paths:` list. Read it there when you need certainty.

Expect more than one workflow on most PRs. Any `engine/**` change triggers at least `engine-ci.yml` and `codegen-drift.yml`, and a narrower engine path can add more. `schemas/**` triggers `engine-ci.yml`, `sdk-ci.yml`, `dagster-ci.yml`, and `vscode-ci.yml`, plus `codegen-drift.yml`. `sdk/python/**` also triggers `dagster-ci.yml`, because the Dagster integration depends on the SDK. The credential-containment policy checks run on every PR, whatever it touches.

Benchmarks run only on a PR labelled `perf` (`engine-bench.yml`). Coverage, the dependency audit, and a POC smoke run happen weekly (`engine-weekly.yml`).

### Merge strategy

GitHub offers a merge-strategy dropdown on the merge button. This repository allows squash and rebase, and disables merge commits. Squash is the default. Choose deliberately, because the right choice depends on the PR's shape.

| PR shape | Strategy | Why |
|---|---|---|
| Single commit | **Squash** | Mechanically equivalent to rebase for a single-commit PR; you get the `(#N)` suffix on `main` for PR traceability |
| Docs, chore, tooling, small fixes | **Squash** | No `git bisect` value from intra-PR granularity; one atomic revert point is easier |
| Multi-commit feature crossing subprojects (DSL cascade, new adapter, new CLI command) | **Rebase** | Preserves per-subproject conventional-commit scopes in `git log` (e.g. `feat(engine/rocky-lang):` → `feat(engine/rocky-compiler):` → `feat(vscode):`) so `git log --grep` by crate still works |
| Refactor series where intermediate states build meaningfully | **Rebase** | Preserves the step-by-step narrative and keeps `git bisect` granular for debugging future regressions |
| WIP-heavy branches with "fix typo" / "oops" commits | **Squash** (or clean up via interactive rebase before opening the PR) | Collapses noise into one coherent commit |

**Heuristic:** rebase when every commit carries a distinct, meaningful conventional-commit scope. Squashing would collapse those scopes into one commit, and `git log --grep` by crate would stop finding them. Otherwise squash.

## Code style

Each subproject follows its language's idioms. The linter is the source of truth.

- **Rust** (`engine/`): edition 2024, `cargo fmt`, `cargo clippy --all-targets -- -D warnings`. Use `tracing` for logs. Use `thiserror` for library errors and `anyhow` for binary and CLI errors. Validate SQL identifiers through `engine/crates/rocky-sql/src/validation.rs` before you interpolate them.
- **Python** (`sdk/python/`, `integrations/dagster/`): Python 3.11+, `from __future__ import annotations`. Model every parsed Rocky payload with Pydantic; a frozen dataclass is fine for internal value types. Line length 100. Ruff rules: E, F, I, N, UP, B, SIM.
- **TypeScript** (`editors/vscode/`): ES2022 target, strict mode, `cp.execFile()` (never `cp.exec()`), escape HTML in webview content.

## Reporting issues

File issues against `rocky-data/rocky`. Five labels name a subproject: `engine`, `sdk`, `dagster`, `vscode`, and `playground`. There is no `docs` label, so use the `documentation` label for a documentation issue. Include the subproject's version, your platform, and minimal steps to reproduce.
