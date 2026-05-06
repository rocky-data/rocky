# 01-model-generation — `rocky ai "intent..."` with compile-verify retry loop

![rocky ai generates a .rocky body + .toml sidecar from natural language intent; Attempts: 2 shows the compile-validate retry loop](../../../../../docs/public/demo-ai-model-generation.gif)

> **Category:** 03-ai
> **Credentials:** `ANTHROPIC_API_KEY` required
> **Runtime:** depends on Anthropic API latency
> **Rocky features:** `rocky ai`, compile-verify retry, `--format rocky|sql`, `--materialization`, `--watermark`, `--target`, sidecar emission

## What it shows

Generate a Rocky model from a natural language description. Rocky sends
your intent to Claude, receives generated code, and **verifies it compiles**
before writing it. If compile fails, Rocky retries with the error context
(up to 3 attempts).

The output is a pair of files under `models/`:

- `<name>.rocky` — the model body
- `<name>.toml` — sidecar with `[strategy]` (from `--materialization`) and
  `[target]` (from `--target`)

## Why it's distinctive

- **Self-correcting AI** — the compile-verify loop closes the gap between
  "model that looks right" and "model that actually works".
- **No copy-paste step** — body + sidecar land on disk, ready to `rocky run`.
- Different from `rocky/examples/ai-intent` (which ships pre-generated tests).
  This POC generates a fresh model live.

## Run

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
./run.sh
```
