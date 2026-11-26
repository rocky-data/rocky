# Contributing to Rocky

Rocky is a monorepo. Every subproject shares one repository, one issue tracker, and one pull-request flow. Each subproject keeps its own build system.

| Subproject | Path | Language | Build |
|---|---|---|---|
| Rocky CLI engine | `engine/` | Rust (multi-crate Cargo workspace) | `cargo` |
| Browser UI (`rocky serve --ui`) | `engine/ui/` | TypeScript (React, Vite) | `npm` |
| Python SDK | `sdk/python/` | Python | `uv` |
| Dagster integration | `integrations/dagster/` | Python | `uv` |
| VS Code extension | `editors/vscode/` | TypeScript | `npm` |
| Documentation site | `docs/` | Astro | `npm` |
| POC catalog | `examples/playground/` | TOML / SQL config | none |

## Share a first attempt

You can contribute without code. Use the [first-run feedback form](https://github.com/rocky-data/rocky/issues/new?template=first_run_feedback.yml) to describe a completed workflow or where you stopped. Include your goal, Rocky version, installation method, and the step that helped or blocked you. A small example with synthetic data is enough.

The form creates a public issue only when you submit it. Remove credentials, private data, and internal addresses first. Use [private security reporting](https://github.com/rocky-data/rocky/security/advisories/new) for vulnerabilities.

## Getting started

Clone once, then work anywhere in the tree.

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky
```

The top-level `justfile` runs one task across the code subprojects: the engine, the browser UI, the SDK, the Dagster integration, and the VS Code extension. It does not cover `docs/` or the playground. Install [`just`](https://github.com/casey/just), then:

```bash
just build       # engine, sdk wheel, dagster wheel, vscode extension
just test        # cargo test, both pytest suites, vscode unit tests, UI typecheck + tests
just lint        # cargo clippy/fmt + ruff + eslint (vscode and UI)
just --list      # every recipe
```

Three gaps:

- `just build` does not build the browser UI. `just build-engine-ui` builds it and embeds it in a release binary.
- `just test` runs the VS Code unit tests only. The electron suite is `just test-vscode-electron`.
- `just build` compiles the extension but does not bundle it, so it does not produce the `dist/extension.js` that debugging needs.

You can also build one subproject directly. The sections below give the commands.

### Git hooks (optional)

Run `just install-hooks` to point `core.hooksPath` at `.git-hooks/`. A check runs only when the change touches the subproject it covers.

- **pre-commit:** `cargo fmt --check` for `engine/`, `ruff format --check` for `integrations/dagster/`, eslint for `editors/vscode/`.
- **pre-commit, codegen drift:** runs only when you stage `output.rs`, `commands/doctor.rs`, or `commands/export_schemas.rs` under `engine/crates/rocky-cli/src/`.
- **pre-push:** `cargo clippy` for `engine/`, `ruff check` for `integrations/dagster/`.
- **post-checkout:** a warning only. `git checkout <branch>` carries a dirty worktree across, and the changes end up committed on the branch you landed on. git has no pre-checkout hook, so the hook makes the carry loud.

Neither hook checks `sdk/python/` or lints `engine/ui/`. A check skips silently when its tool is missing, so no `uv` turns the ruff check into a pass. The drift check compares fewer paths than `codegen-drift.yml`, so a commit can pass the hook and still fail CI. Treat the hooks as a fast first pass, not the gate. `ROCKY_SKIP_HOOKS=1` skips every hook. `ROCKY_SKIP_CODEGEN_HOOK=1` skips the drift check alone.

To start a branch, run `scripts/new-branch.sh <name> [base]`. It refuses on a dirty tree and defaults the base to a freshly fetched `origin/main`. A branch cut from another feature branch inherits its commits, and a squash-merge can land the parent's work, including its `Closes #NNN` trailers, under the child's number.

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

### Browser UI (`engine/ui/`)

```bash
cd engine/ui
npm ci
npm run build      # writes dist/, then refuses any external load
npm test           # vitest
npm run typecheck
npm run lint
```

`cargo build --features ui` (from `engine/`) embeds `engine/ui/dist/` in the binary. Plain `cargo build` needs no Node toolchain. For local development, run `just ui-dev` from the repository root. It starts `rocky serve` on port 8080 and Vite on 5173; open `http://localhost:5173/ui/`. [`engine/ui/README.md`](engine/ui/README.md) has the details.

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

A catalog of small POCs, one per Rocky feature. Each POC is a complete project you run with `./run.sh`. Most need only DuckDB. The weekly `poc-smoke` job runs the credential-free POCs against a fresh binary. It skips any POC that needs credentials, Docker, or a Rust toolchain. A second step parse-checks the credential-gated POCs, but it is `continue-on-error` and cannot fail the job. `just regen-fixtures` captures the Dagster test fixtures from specific POCs here. The catalog has no build step.

## Cross-project changes

A change to the CLI JSON output, or to the DSL, reaches every consumer. Land it in one pull request.

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

Three shims keep older imports working: `dagster_rocky.types` and `dagster_rocky.types_generated` re-export the models from `rocky_sdk`, and `editors/vscode/src/types/rockyJson.ts` re-exports the generated TypeScript.

**When you change Rocky DSL syntax** (`.rocky` files), update all five in lockstep:

1. `engine/crates/rocky-lang/` (parser + lexer)
2. `engine/crates/rocky-compiler/` (type checking)
3. `editors/vscode/syntaxes/rocky.tmLanguage.json` (TextMate grammar)
4. `editors/vscode/snippets/rocky.json` (snippets)
5. `docs/src/content/docs/concepts/rocky-dsl.md` + `docs/rocky-lang-spec.md` (published DSL page + full spec)

## Releases

Releases are CI-driven. Land a release PR (version bump + CHANGELOG entry), tag the merge commit `<prefix>-v<x.y.z>`, and push the tag. The matching workflow builds, creates a GitHub Release and publishes. Git refuses a tag that exists, so a stale version fails loudly.

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

- The engine matrix builds five targets: macOS ARM64, macOS Intel, Linux x86_64, Linux ARM64, Windows x86_64. Each gets a `rocky` and a `rocky-lsp` archive.
- When a `dagster-v*` release raises its `rocky-sdk` floor, push the `sdk-v*` tag first. The published dagster wheel resolves the SDK from PyPI, not the path source.
- `engine-wasm-release.yml` builds the compiler to WebAssembly on an `engine-wasm-v*` tag. Nothing has shipped from it: `@rocky-data/compiler` is not on npm, and the publish step exits early without an `NPM_TOKEN` secret. Treat it as unreleased.
- `scripts/release.sh engine|sdk|dagster|vscode <version>` is a local fallback for a hotfix when CI is down. Run the engine path on an Apple Silicon Mac: it packages the host build as the macOS ARM64 archive without checking the architecture, and builds Linux x86_64 in Docker.
- Recipes that wrap it: `just release-engine <version>`, `just release-sdk <version> [--publish]`, `just release-dagster <version> [--publish]`, `just release-vscode <version> [--publish]`. Without `--publish`, the GitHub Release stays a draft and nothing reaches PyPI or the Marketplace; the tag push's CI run publishes it. The engine has no `--publish`: it ships to GitHub Releases only.

Prefer the tag-driven flow. The `rocky-release` skill (`.agents/skills/rocky-release/`) has the full checklist. For live-warehouse checks, see [`RELEASE_SMOKE.md`](RELEASE_SMOKE.md).

## Pull requests

- Branch from `main`.
- One logical change per PR. Cross-project PRs are welcome when they ship a coordinated schema or DSL change; otherwise keep a PR inside one subproject.
- Conventional commits required: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`. Scope by subproject or crate where it helps: `feat(engine/rocky-databricks): add OAuth M2M auth`, `fix(dagster): handle partial-success exit codes`, `docs(vscode): update README screenshots`.
- **Never** include `Co-Authored-By` trailers in commit messages.

On a first pull request from a fork, GitHub holds the CI runs until a maintainer approves them. Required checks may be missing meanwhile. Each new push needs approval again. You do nothing.

### What CI runs

CI is path-filtered. The paths your PR touches decide which workflows run. Every required check must pass before merge.

| What you touch | Workflow | What it runs |
|---|---|---|
| `engine/**` | `engine-ci.yml` | nextest, clippy, `cargo fmt --check`, an adapter-boundary lint, the browser UI's lint, typecheck, tests and build, a release-build smoke test with the UI embedded |
| `sdk/python/**` | `sdk-ci.yml` | pytest, ruff, and a smoke job driving the SDK against a real `rocky` |
| `integrations/dagster/**` | `dagster-ci.yml` | pytest and ruff |
| `editors/vscode/**` | `vscode-ci.yml` | compile, vitest unit tests, the electron integration tests, eslint |
| `docs/**` | `docs-build.yml` | an Astro build of the docs site |
| `scripts/**` | `scripts-ci.yml` | shellcheck, and a self-test of the soak-verdict script |
| `.agents/skills/**` or `.claude/skills/**` (root, `engine/`, `editors/vscode/`) | `skills-mirror-drift.yml` | diffs each `.agents/skills` tree against its `.claude/skills` twin and fails unless every pair is byte-identical |
| `engine/**`, `schemas/**`, `justfile`, `examples/playground/pocs/**`, or a generated binding | `codegen-drift.yml` | re-runs `just codegen` and `just regen-fixtures`, then fails on any diff |
| `examples/playground/pocs/**` | `poc-counts-drift.yml` | recounts the POCs and fails on any diff |
| every PR to `main` | `attribution-gate.yml` | fails if the PR title, body or a commit message carries an agent attribution line, an agent session trailer, or an agent session link |
| `engine/evals/**` or `engine/crates/rocky-mcp/**` | `engine-evals.yml` | the eval harness self-test and the structured-error contract |
| the recipe-manifest surface (`rocky-verify`, `recipe_identity.rs`, `history.rs`, `examples/audit-sample/**`) | `manifest-conformance.yml` | builds `rocky` and `rocky-verify`, then runs the conformance script |

The table covers the common cases. Each workflow in `.github/workflows/` holds its exact `paths:` list. Most PRs trigger several workflows:

- `engine/**` triggers at least `engine-ci.yml` and `codegen-drift.yml`.
- `schemas/**` triggers `engine-ci.yml`, `sdk-ci.yml`, `dagster-ci.yml`, `vscode-ci.yml` and `codegen-drift.yml`.
- `sdk/python/**` also triggers `dagster-ci.yml`, because Dagster depends on the SDK.
- The credential-containment policy checks run on every PR.

Benchmarks run only on a PR labelled `perf` (`engine-bench.yml`). Coverage, the dependency audit, and a POC smoke run happen weekly (`engine-weekly.yml`).

### Maintainer: approve CI for a first-time fork contributor

Match each held run to the pull request and current head commit before you approve it. Approve again after each push.

```bash
gh api 'repos/rocky-data/rocky/actions/runs?status=action_required'
gh api --method POST repos/rocky-data/rocky/actions/runs/<id>/approve
```

A fork adds no separate approving-review requirement. The ruleset's extra approval for unattributed Copilot pull requests is a different rule.

### Merge strategy

The repository allows squash and rebase, and disables merge commits. Squash is the default.

| PR shape | Strategy | Why |
|---|---|---|
| Single commit; docs, chore, tooling, small fixes; WIP-heavy branches | **Squash** | One atomic revert point and the `(#N)` suffix on `main`. For WIP noise, you can also clean up with an interactive rebase first. |
| Multi-commit feature crossing subprojects (DSL cascade, new adapter, new CLI command) | **Rebase** | Keeps per-subproject scopes in `git log` (`feat(engine/rocky-lang):`, `feat(vscode):`), so `git log --grep` by crate works |
| Refactor series where intermediate states build | **Rebase** | Keeps the step-by-step narrative and `git bisect` granular |

Rebase when every commit carries a distinct, meaningful scope. Otherwise squash.

## Code style

Each subproject follows its language's idioms. The linter is the source of truth.

- **Rust** (`engine/`): edition 2024, `cargo fmt`, `cargo clippy --all-targets -- -D warnings`. Use `tracing` for logs. Use `thiserror` for library errors and `anyhow` for binary and CLI errors. Validate SQL identifiers through `engine/crates/rocky-sql/src/validation.rs` before you interpolate them.
- **Python** (`sdk/python/`, `integrations/dagster/`): Python 3.11+, `from __future__ import annotations`. Model every parsed Rocky payload with Pydantic; a frozen dataclass is fine for internal value types. Line length 100. Ruff rules: E, F, I, N, UP, B, SIM.
- **TypeScript** (`editors/vscode/`): ES2022 target, strict mode, `cp.execFile()` (never `cp.exec()`), escape HTML in webview content.

## Reporting issues

File issues against `rocky-data/rocky`. Five labels name a subproject: `engine`, `sdk`, `dagster`, `vscode`, and `playground`. There is no `docs` label, so use the `documentation` label for a documentation issue. Include the subproject's version, your platform, and minimal steps to reproduce.
