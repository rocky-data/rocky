# 04-ai-test-generation — `rocky ai-test --all --save`

> **Category:** 03-ai
> **Credentials:** `ANTHROPIC_API_KEY` required
> **Runtime:** depends on Anthropic API latency
> **Rocky features:** `rocky ai-test`, generated SQL assertions from intent + schema

## What it shows

`rocky ai-test` reads each model's intent + output schema and asks Claude
to generate SQL assertions (uniqueness, non-null, range checks, etc.).
With `--save`, the assertions are written to a `tests/` directory.

## Why it's distinctive

- **Generation, not just hand-written tests.** Different from
  `rocky/examples/ai-intent` which ships *pre-generated* assertions —
  this POC generates them live.

## Run

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
./run.sh
```
