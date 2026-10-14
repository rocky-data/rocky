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

## How the upstream baseline works

`ai-sync` stores each model's upstream column types in a snapshot next to the
state store (`<state>.ai-sync.json`). A later sync diffs the current upstream
types against that baseline and feeds the changes into the prompt.

```
first sync ──▶ no baseline: proposal uses intent only, baseline saved
later sync ──▶ diff upstream types vs baseline ──▶ changes go into the prompt
--apply    ──▶ writes the proposal and advances that model's baseline
```

`run.sh` runs one sync without `--apply`, so on a clean checkout every model
is a first sync. The CLI prints `Note: no upstream schema snapshot existed …
Their proposals follow declared intent only.` To see a diff, change an upstream
column and run `rocky ai-sync --models models` again.

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

`run.sh` runs `rocky ai-sync --models models` (a dry run) and writes the
proposals to `expected/sync.log`.
