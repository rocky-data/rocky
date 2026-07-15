# 03-ai-sync-schema-evolution — `rocky ai-sync` intent-guided updates

> **Category:** 03-ai
> **Credentials:** `ANTHROPIC_API_KEY` required
> **Runtime:** depends on Anthropic API latency
> **Rocky features:** `rocky ai-sync`, intent-guided downstream updates

## What it shows

`rocky ai-sync` scans models that carry an `intent` field and asks the LLM
to propose updates that keep each model faithful to its declared intent.
The intent field is fed back into the prompt, so a suggested update
preserves the original purpose of the model rather than just its current SQL.

The project ships two models:

- `raw_orders` — an upstream staging model (`SELECT` over the seed orders).
- `customer_revenue` — an intent-annotated downstream rollup that groups
  orders by `customer_id`. Its `intent` is the contract ai-sync syncs against.

## Current behavior (important)

Upstream schema-change *detection* is **not yet wired** in the engine: the
state store does not snapshot prior compilations, so `ai-sync` cannot diff a
"before" schema against an "after" one. Proposals are therefore driven by the
model's **declared intent alone**, with no upstream column-added / removed /
renamed diff. The CLI prints this same caveat at the top of its output
(`proposals are based on declared model intent only …`).

Once compile-snapshot persistence lands, the same command will additionally
feed detected upstream changes into the prompt.

## Why it's distinctive

- **Maintenance-focused AI**, not generation. dbt has no equivalent.
- The intent field is the contract between human and AI for what the
  model should *be*, independent of the SQL it currently is.

## Layout

```
models/
  raw_orders.sql        # upstream staging
  raw_orders.toml
  customer_revenue.rocky # intent-annotated downstream rollup
  customer_revenue.toml
data/seed.sql           # sample orders
rocky.toml
run.sh
```

## Run

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
./run.sh
```

`run.sh` compiles the models and runs `rocky ai-sync`, writing the proposals
to `expected/sync.log`.
