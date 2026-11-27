# 01-model-generation — `rocky ai "intent..."` with compile-verify retry loop

![rocky ai generates a .rocky body + .toml sidecar from natural language intent; Attempts: 2 shows the compile-validate retry loop](../../../../../docs/public/demo-ai-model-generation.gif)

> **Category:** 03-ai
> **Credentials:** `ANTHROPIC_API_KEY` required
> **Runtime:** depends on Anthropic API latency
> **Rocky features:** `rocky ai`, compile-verify retry, `--format rocky|sql`, `--materialization`, `--unique-key`, `--target`, sidecar emission

## What it shows

`rocky ai` sends a natural-language intent to Claude and **verifies the generated code compiles** before writing it, retrying with the compiler errors (up to 3 attempts). It writes `models/<name>.rocky` and a `<name>.toml` sidecar with `[strategy]` (from `--materialization` + `--unique-key`) and `[target]` (from `--target`).

The POC runs it twice:

1. Default `full_refresh`: `monthly_revenue.{rocky,toml}`.
2. `--materialization merge --unique-key order_date`: `orders_daily.{rocky,toml}`.

`incremental` is refused: on a transformation model it re-inserts every row on each run (E037). `ephemeral` is refused too: a generated model has no consumer to inline it into.

## Run

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
./run.sh
```
