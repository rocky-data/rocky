# Rocky monorepo

Canonical agent guidance lives in [`AGENTS.md`](AGENTS.md) (shared across agent tools). Claude Code loads it via the import below; the rest of this file is the Claude Code skills router.

@AGENTS.md

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
| [`rocky-release`](.claude/skills/rocky-release/SKILL.md) | Cutting a tag-namespaced release (`engine-v*`, `sdk-v*`, `dagster-v*`, `vscode-v*`) |

Engine-local skills also live at `engine/.claude/skills/` (12 skills) and activate when working inside `engine/`: `rocky` (CLI cheat sheet), `databricks`, `fivetran`, plus 9 Rust convention skills (`rust-style`, `rust-error-handling`, `rust-doc`, `rust-unsafe`, `rust-clippy-triage`, `rust-bench-criterion`, `rust-dep-hygiene`, `rust-async-tokio`, `rust-analyzer-ssr`).

VS Code-local skills live at `editors/vscode/.claude/skills/` and activate when working inside `editors/vscode/`: `tailwind-plus-elements` (using `@tailwindplus/elements` headless web components in the React webviews).
