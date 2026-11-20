# Rocky monorepo

Canonical agent guidance lives in [`AGENTS.md`](AGENTS.md) (shared across agent tools). Claude Code loads it via the import below; the rest of this file is Claude Code-specific: the red-team mapping and the skills router.

@AGENTS.md

## Red team in Claude Code

The independent red team that [`AGENTS.md`](AGENTS.md) requires for substantive analyses and plans is chosen in this order. Take the first one that is available:

For a soundness-critical engine change (the top four review priorities in `AGENT_REVIEW.md`), a fresh read-only Fable agent goes ahead of step 1, when its weekly limit allows and Fable did not author the change.

1. **Codex**, if the `codex:codex-rescue` agent is in your list of agent types. Check the list each session; do not assume either way.
2. **Opus authored the change:** a fresh read-only **Fable** agent (`model: "fable"`), when Fable's weekly limit allows (no limit warning from a Fable call this week, and Hugo has not said to save it). Otherwise a fresh read-only **Opus** agent at high effort that did not author the change.
3. **Sonnet or Haiku authored it:** a fresh read-only **Opus** agent.
4. **Fable authored it:** a fresh read-only **Opus** agent.

Never use Sonnet as the red team for a substantive change. Check which model you are before you choose. The session's model can change mid-session.

Fable has its own weekly limit, so spend it where judgment changes the outcome. Do not spend Fable on mechanical reviews; a docs-only change that is substantive still gets a red team (`AGENTS.md`), from the Opus tier if Fable is tight.

Give the reviewer the diff, the two `AGENTS.md` answers and `AGENT_REVIEW.md`, never the author's reasoning. For an analysis or plan with no diff, give it the findings or the plan and the files they rest on instead of the diff. Name the reviewer and its model in the PR or report. `/code-review` run by the authoring agent does not count as the red team.

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

Engine-local skills also live at `engine/.claude/skills/` and load when you first read or edit a file inside `engine/`: `rocky` (CLI cheat sheet), `databricks`, `fivetran`, plus the Rust convention skills (`rust-style`, `rust-error-handling`, `rust-doc`, `rust-unsafe`, `rust-clippy-triage`, `rust-bench-criterion`, `rust-dep-hygiene`, `rust-async-tokio`, `rust-analyzer-ssr`).

VS Code-local skills live at `editors/vscode/.claude/skills/` and activate when working inside `editors/vscode/`: `tailwind-plus-elements` (using `@tailwindplus/elements` headless web components in the React webviews).

The permission allowlists in `engine/.claude/settings.json`, `editors/vscode/.claude/settings.json` and `integrations/dagster/.claude/settings.json` apply only when Claude Code starts in that directory. Claude Code does not merge a subdirectory's project settings into a session started at the repo root. The root `.claude/settings.json` holds only the cloud session-start hook. Run `/status` to see which settings files a session loaded.
