# 02-ai-explain-bootstrap — Reverse-engineer intent on existing models

> **Category:** 03-ai
> **Credentials:** `ANTHROPIC_API_KEY` required
> **Runtime:** depends on Anthropic API latency
> **Rocky features:** `rocky ai-explain --all --save`

## What it shows

`rocky ai-explain` reads each model's SQL and generates a plain-English
description. With `--save`, the description is written to the model's
`.toml` sidecar as an `intent` field. This is the canonical first step
for adopting AI features on an existing project.

## Why it's distinctive

- **Bootstrap AI on a legacy project** in one command instead of writing
  intent fields manually.
- The intent field enables downstream AI features (`ai-sync`, `ai-test`).

## Run

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
./run.sh
```
