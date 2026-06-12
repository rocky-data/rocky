# Rocky monorepo

Single repository for the Rocky data platform — a typed-program layer above the warehouse — and its first-party integrations.

## Subprojects

| Path | Language | What it is |
|---|---|---|
| `engine/` | Rust | Core CLI + engine — SQL transformation, schema drift, incremental loads, adapters. 23-crate Cargo workspace. |
| `sdk/python/` | Python | `rocky-sdk` — standalone typed Python client (`RockyClient`) over the `rocky` CLI. Owns the generated Pydantic models. For notebooks, scripts, and orchestrators. |
| `integrations/dagster/` | Python | Dagster integration — a thin `ConfigurableResource` adapter over `rocky-sdk`'s `RockyClient`; maps results to assets/checks. Depends on `rocky-sdk`. |
| `editors/vscode/` | TypeScript | VS Code extension — LSP client (stdio to `rocky lsp`), syntax highlighting, commands for AI features. |
| `examples/playground/` | Config only | Self-contained DuckDB sample pipeline. Used as a smoke test and a benchmark fixture. No credentials needed. |

Each subproject has its own `CLAUDE.md` with build commands, coding standards, and architecture details. **Always read the subproject `CLAUDE.md` before working inside it.**

## How they fit together

```
editors/vscode  ──(LSP stdio)──▶  engine (rocky lsp)
sdk/python (rocky-sdk) ──(subprocess)──▶  engine (rocky discover/plan/run/...)
integrations/dagster ──(RockyClient)──▶  sdk/python ──▶  engine
examples/playground ──(config)──▶  engine (rocky run --config rocky.toml)
```

- `sdk/python` (`rocky-sdk`) invokes the `rocky` CLI via subprocess and parses JSON output into Pydantic models. The Python side does not depend on Rust source — only on the binary on `$PATH`.
- `integrations/dagster` depends on `rocky-sdk`: `RockyResource` is a thin Dagster adapter that delegates to `RockyClient` and translates the SDK's `RockyError` hierarchy into `dagster.Failure`. In-repo dev/CI resolves the SDK via a `[tool.uv.sources]` path dependency; the published `dagster-rocky` wheel floors on `rocky-sdk>=…` from PyPI.
- `editors/vscode` spawns `rocky lsp` as a language server over stdio. The TypeScript side does not depend on Rust source either.
- `examples/playground` is read by `engine` at runtime; it has no code dependency on the engine source.

## Claude Code skills

This repo ships a set of task-specific skills in `.claude/skills/` that should be the **first stop** for any non-trivial change. They're cross-referenced and deliberately kept lean — each one points at the right files and runs the right tooling for a specific kind of work:

| Skill | When it fires |
|---|---|
| [`rocky-dev`](.claude/skills/rocky-dev/SKILL.md) | Top-level router for any change — read this first if unsure |
| [`rocky-codegen`](.claude/skills/rocky-codegen/SKILL.md) | Editing `engine/crates/rocky-cli/src/output.rs` or any `*Output` struct — walks the `just codegen` cascade |
| [`rocky-config`](.claude/skills/rocky-config/SKILL.md) | Writing or reviewing a `rocky.toml` — covers the 4 pipeline types, adapter variants, defaults, legacy keys to avoid |
| [`rocky-new-cli-command`](.claude/skills/rocky-new-cli-command/SKILL.md) | Adding a new `rocky <verb>` — end-to-end across engine + dagster + vscode |
| [`rocky-new-adapter`](.claude/skills/rocky-new-adapter/SKILL.md) | Adding a warehouse or source adapter crate |
| [`rocky-dsl-change`](.claude/skills/rocky-dsl-change/SKILL.md) | Changing `.rocky` DSL syntax — the 4-file lockstep cascade |
| [`rocky-test-fixtures`](.claude/skills/rocky-test-fixtures/SKILL.md) | Regenerating dagster JSON test fixtures via `just regen-fixtures` |
| [`rocky-ai-workflow`](.claude/skills/rocky-ai-workflow/SKILL.md) | Authoring or modifying a Rocky model on behalf of a user — inspect → sample → SQL → compile-loop → plan → propose → review → apply |
| [`rocky-poc`](.claude/skills/rocky-poc/SKILL.md) | Scaffolding a new POC under `examples/playground/` |
| [`rocky-release`](.claude/skills/rocky-release/SKILL.md) | Cutting a tag-namespaced release (`engine-v*`, `dagster-v*`, `vscode-v*`) |

Engine-local skills also live at `engine/.claude/skills/` (12 skills) and activate when working inside `engine/`: `rocky` (CLI cheat sheet), `databricks`, `fivetran`, plus 9 Rust convention skills (`rust-style`, `rust-error-handling`, `rust-doc`, `rust-unsafe`, `rust-clippy-triage`, `rust-bench-criterion`, `rust-dep-hygiene`, `rust-async-tokio`, `rust-analyzer-ssr`).

VS Code-local skills live at `editors/vscode/.claude/skills/` and activate when working inside `editors/vscode/`: `tailwind-plus-elements` (using `@tailwindplus/elements` headless web components in the React webviews).

## Cross-project changes (the reason this is one repo)

DSL changes still touch multiple subprojects in lockstep. The monorepo lets you land them as one PR with one CI run.

**When modifying the CLI's JSON output schema (Phase 2 — codegen-driven):**

Every Rocky CLI command that emits `--output json` is backed by a typed Rust output struct in `engine/crates/rocky-cli/src/output.rs` (or `commands/doctor.rs` for doctor) that derives `JsonSchema`. The Pydantic and TypeScript bindings are autogenerated from those schemas. To change a CLI output:

1. Edit the relevant `*Output` struct in `engine/crates/rocky-cli/src/output.rs`.
2. Run `just codegen` from the monorepo root. This:
   - exports JSON schemas via `cargo run --bin rocky -- export-schemas schemas/`,
   - regenerates Pydantic v2 models in `sdk/python/src/rocky_sdk/types_generated/` (re-exported by `dagster_rocky.types` for backward compatibility),
   - regenerates TypeScript interfaces in `editors/vscode/src/types/generated/`.
3. Commit the schema and the regenerated bindings together with the Rust change.

The `codegen-drift` CI workflow (`.github/workflows/codegen-drift.yml`) fails any PR where the committed bindings drift from what `just codegen` produces locally.

**Status:** The codegen migration is complete — all 63 CLI JSON schemas are backed by typed Rust structs deriving `JsonSchema`. The pipeline runs end-to-end via `just codegen` (recipe `codegen-sdk`), enforced by `codegen-drift.yml` CI. The vscode `rockyJson.ts` is a type-alias shim over generated TypeScript. The generated Pydantic models live in `rocky-sdk` (`sdk/python/`); the dagster `types.py` re-exports them from `rocky_sdk.types` — see [`sdk/python/CLAUDE.md`](sdk/python/CLAUDE.md) and [`integrations/dagster/CLAUDE.md`](integrations/dagster/CLAUDE.md). `just regen-fixtures` captures fresh dagster test fixtures from the live binary.

**When modifying Rocky DSL syntax (`.rocky` files):**
1. `engine/crates/rocky-lang/` (parser + lexer)
2. `engine/crates/rocky-compiler/` (type checking)
3. `editors/vscode/syntaxes/rocky.tmLanguage.json` (TextMate grammar)
4. `editors/vscode/snippets/rocky.json` (snippets)

## Build orchestration

Top-level `justfile` orchestrates common tasks across all subprojects. Install [`just`](https://github.com/casey/just), then:

```bash
just build       # cargo build --release + uv build --wheel + npm compile
just test        # cargo test + pytest + vitest (vscode unit tests)
just lint        # cargo clippy/fmt + ruff + eslint
just --list      # all recipes
```

Each subproject's tools (`cargo`, `uv`, `npm`) work directly from inside the subproject directory. The `justfile` is a convenience, not a requirement.

## Releases

Tag-namespaced — each artifact ships independently:

- `engine-v0.1.0` → CLI binary on GitHub Releases
- `sdk-v0.1.0` → `rocky-sdk` wheel on PyPI
- `dagster-v0.1.0` → `dagster-rocky` wheel on PyPI
- `vscode-v0.1.0` → Rocky VSIX on VS Code Marketplace

Release **`rocky-sdk` before any `dagster-rocky` release that raises its `rocky-sdk>=…` floor** — the published `dagster-rocky` wheel resolves the SDK from PyPI, not the monorepo path source.

**Release workflow:** Tagging `engine-v*` triggers `engine-release.yml`, which builds all 5 platform targets (macOS ARM64/Intel, Linux x86_64/ARM64, Windows x86_64) via a CI matrix and attaches them to the GitHub Release. `scripts/release.sh` remains as a local-build hotfix fallback. Convenience recipes: `just release-engine <version>`, `just release-dagster <version> [--publish]`, `just release-vscode <version> [--publish]`.

The `engine/install.sh` and `engine/install.ps1` scripts filter releases by the `engine-v*` prefix when fetching the latest version.

## Git conventions

- **Never** include `Co-Authored-By` trailers in commit messages.
- Conventional commits required: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`.
- Scope by subproject or crate: `feat(engine/rocky-databricks): add OAuth M2M auth`, `fix(dagster): handle partial-success exit codes`, `chore(vscode): bump vscode-languageclient`.

## CI

`.github/workflows/` contains path-filtered workflows. Touching `engine/**` triggers `engine-ci.yml` only; touching multiple subprojects triggers each relevant workflow independently.

Cost-saving measures:
- `engine-ci.yml` runs test + clippy + fmt on every PR. Coverage and security audit are in `engine-weekly.yml` (Monday schedule + manual dispatch).
- `engine-bench.yml` only runs on PRs labeled `perf`.
- Engine releases build all platforms in CI via `engine-release.yml`; `scripts/release.sh` is a local fallback.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the full development guide.
