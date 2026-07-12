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

## Layout

```
models/
  raw_orders.sql          # upstream staging SELECT over the seed orders
  raw_orders.toml         # sidecar — gains an intent field after the run
  customer_revenue.rocky  # downstream rollup grouping orders by customer
  customer_revenue.toml   # sidecar — gains an intent field after the run
data/seed.sql             # sample orders
rocky.toml
run.sh
```

## Run

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
./run.sh
```

`run.sh` calls `rocky ai-explain --all --save --models models` (teeing to
`expected/explain.log`), then cats every `models/*.toml` so you can see the
new `intent` fields.

## Expected output

`ai-explain` reads each model's SQL, asks Claude for a one-line description,
and — because of `--save` — writes it back to the model's `.toml` sidecar as
an `intent` key. The exact wording is LLM-generated and varies run to run;
a representative result for `models/raw_orders.toml` (the writer serializes
the sidecar fresh, so the injected `intent` — and a `name` key, added when the
sidecar has none — sort ahead of the existing `[strategy]`/`[target]` tables):

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
