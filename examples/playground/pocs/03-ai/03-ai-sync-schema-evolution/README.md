# 03-ai-sync-schema-evolution — `rocky ai-sync` after upstream changes

> **Category:** 03-ai
> **Credentials:** `ANTHROPIC_API_KEY` required
> **Runtime:** depends on Anthropic API latency
> **Rocky features:** `rocky ai-sync`, intent-guided downstream updates

## What it shows

When an upstream model's schema changes (column added/removed/renamed),
`rocky ai-sync` proposes downstream model updates. Each downstream model's
`intent` field is fed back into the prompt so the suggested update
preserves the original intent of the model.

## Why it's distinctive

- **Maintenance-focused AI**, not generation. dbt has no equivalent.
- The intent field is the contract between human and AI for what the
  model should *be*, independent of the SQL it currently is.

## Run

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
./run.sh
```
