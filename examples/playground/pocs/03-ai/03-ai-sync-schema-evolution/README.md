# 03-ai-sync-schema-evolution — `rocky ai-sync` intent-guided updates

> **Category:** 03-ai
> **Credentials:** `ANTHROPIC_API_KEY` required
> **Runtime:** depends on Anthropic API latency
> **Rocky features:** `rocky ai-sync`, intent-guided downstream updates

## What it shows

`rocky ai-sync` scans models that carry an `intent` field and asks the LLM to propose updates that keep each model faithful to it. Here `customer_revenue` (a rollup by `customer_id`, with an `intent`) sits downstream of `raw_orders`.

## How the upstream baseline works

`ai-sync` stores each model's upstream column types in `<state>.ai-sync.json`, next to the state store.

```
first sync ──▶ no baseline: proposal uses intent only, baseline saved
later sync ──▶ diff upstream types vs baseline ──▶ changes go into the prompt
--apply    ──▶ writes the proposal and advances that model's baseline
```

`run.sh` runs one sync without `--apply`, so on a clean checkout every model is a first sync. The CLI prints `Note: no upstream schema snapshot existed … Their proposals follow declared intent only.` To see a diff, change an upstream column and run `rocky ai-sync --models models` again.

## Run

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
./run.sh
```

This is a dry run; proposals go to `expected/sync.log`.
