# 02-ai-explain-bootstrap — Reverse-engineer intent on existing models

> **Category:** 03-ai
> **Credentials:** `ANTHROPIC_API_KEY` required
> **Runtime:** depends on Anthropic API latency
> **Rocky features:** `rocky ai-explain --all --save`

## What it shows

`rocky ai-explain --all --save` reads each model's SQL and writes a plain-English description into its `.toml` sidecar as `intent`. This is the first step for adopting AI features (`ai-sync`, `ai-test`) on an existing project.

## Run

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
./run.sh
```

`run.sh` calls `rocky ai-explain --all --save --models models` (teeing to `expected/explain.log`), then prints every `models/*.toml`.

## Expected output

Wording is LLM-generated and varies run to run. A representative `models/raw_orders.toml` (the sidecar is serialized fresh, so `intent`, and a `name` key if there was none, sort ahead of the tables):

```toml
intent = "Stages raw order rows (order_id, customer_id, amount) from the seed source for downstream aggregation."
name = "raw_orders"

[strategy]
type = "full_refresh"

[target]
catalog = "poc"
schema = "staging"
```

The run finishes with `POC complete: intent fields added to all models.`
