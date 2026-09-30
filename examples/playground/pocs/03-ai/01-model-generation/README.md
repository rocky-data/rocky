# 01-model-generation — `rocky ai "intent..."` with compile-verify retry loop

![rocky ai generates a .rocky body + .toml sidecar from natural language intent; Attempts: 2 shows the compile-validate retry loop](../../../../../docs/public/demo-ai-model-generation.gif)

> **Category:** 03-ai
> **Credentials:** `ANTHROPIC_API_KEY` required
> **Runtime:** depends on Anthropic API latency
> **Rocky features:** `rocky ai`, compile-verify retry, `--format rocky|sql`, `--materialization`, `--unique-key`, `--target`, sidecar emission

## What it shows

Generate a Rocky model from a natural language description. Rocky sends
your intent to Claude, receives generated code, and **verifies it compiles**
before writing it. If compile fails, Rocky retries with the error context
(up to 3 attempts).

The output is a pair of files under `models/`:

- `<name>.rocky` — the model body
- `<name>.toml` — sidecar with `[strategy]` (from `--materialization` +
  `--unique-key`) and `[target]` (from `--target`)

The POC runs `rocky ai` twice to cover both flag paths:

1. **Default `full_refresh`** — `monthly_revenue.{rocky,toml}` lands with
   `[strategy] type = "full_refresh"`.
2. **`--materialization merge --unique-key order_date`** —
   `orders_daily.{rocky,toml}` lands with `[strategy] type = "merge"`
   and `unique_key = ["order_date"]`.

## Why it's distinctive

- **Self-correcting AI** — the compile-verify loop closes the gap between
  "model that looks right" and "model that actually works".
- **No copy-paste step** — body + sidecar land on disk, ready to `rocky run`.
- **Materializations on one flag surface** — `full_refresh` and `merge`
  (+ `--unique-key`). `incremental` is refused: on a transformation model it
  re-inserts every row on each run (E037). `ephemeral` is refused too: it is
  never inlined into its consumers (E038).
- Different from `engine/examples/ai-intent` (which ships pre-generated tests).
  This POC generates fresh models live.

## Run

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
./run.sh
```
