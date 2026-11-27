# 04-ai-test-generation — `rocky ai-test --all --save`

> **Category:** 03-ai
> **Credentials:** `ANTHROPIC_API_KEY` required
> **Runtime:** depends on Anthropic API latency
> **Rocky features:** `rocky ai-test`, generated SQL assertions from intent + schema

## What it shows

`rocky ai-test` reads each model's intent and output schema and asks Claude for SQL assertions (uniqueness, non-null, range checks). With `--save`, they are written to `tests/`.

## Run

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
./run.sh
```
