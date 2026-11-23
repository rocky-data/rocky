# Rocky monorepo

Canonical agent guidance lives in [`AGENTS.md`](AGENTS.md) (shared across agent tools). Claude Code loads it via the import below; the rest of this file is Claude Code-specific.

@AGENTS.md

## Claude Code skills

Claude Code discovers skills in `.claude/skills/`, a byte-identical copy of `.agents/skills/`. Edit the `.agents/skills/` copy, then copy it across. Nested skills load only after you read or edit a file in that subtree. If unsure which one applies, start with [`rocky-dev`](.agents/skills/rocky-dev/SKILL.md), the top-level router.

| Directory | Skills |
|---|---|
| `.claude/skills/` | `rocky-dev`, `rocky-codegen`, `rocky-config`, `rocky-new-cli-command`, `rocky-new-adapter`, `rocky-dsl-change`, `rocky-test-fixtures`, `rocky-ai-workflow`, `rocky-poc`, `rocky-release` |
| `engine/.claude/skills/` | `rocky` (CLI cheat sheet), `databricks`, `fivetran`, `rust-style`, `rust-error-handling`, `rust-doc`, `rust-unsafe`, `rust-clippy-triage`, `rust-bench-criterion`, `rust-dep-hygiene`, `rust-async-tokio`, `rust-analyzer-ssr` |
| `editors/vscode/.claude/skills/` | `tailwind-plus-elements` |

## Settings

The permission allowlists in `engine/.claude/settings.json`, `editors/vscode/.claude/settings.json` and `integrations/dagster/.claude/settings.json` apply only when Claude Code starts in that directory. Claude Code does not merge a subdirectory's project settings into a session started at the repo root. The root `.claude/settings.json` holds the attribution switch-off and the cloud session-start hook. Run `/status` to see which settings files a session loaded.
