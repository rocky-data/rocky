# 06-mcp-grounding — schema-only models compile but reconcile wrong; the MCP tools fix it

> **Category:** 03-ai
> **Credentials:** none — the `run.sh` is deterministic and creds-free (no LLM, no Claude Code). The optional interactive demo needs Claude Code with MCP support.
> **Runtime:** < 5s
> **Rocky features:** `rocky mcp` server (`inspect_schema`, `sample_rows`, `profile_column`, `compile`, `plan_preview`, `propose`), `rocky review`/`rocky apply` human gate, `rocky test --declarative`

## What it shows

A model can compile cleanly and still produce the wrong answer, because the schema alone hides the two things that decide correctness: the literal values in a column and the units a number is stored in. This POC makes that gap concrete, then shows how grounding the model in real data via the `rocky mcp` tools closes it.

It ships two transformation models over the same source:

- `revenue_naive.sql` — the plausible schema-only guess: `WHERE status = 'completed'`, `SUM(amount_cents)` treated as dollars. It compiles. It returns the wrong number.
- `revenue_correct.sql` — written after looking at the data: `WHERE status = 'COMPLETE'`, `SUM(amount_cents) / 100.0`. It reconciles to the true total and carries a typed assertion that locks the invariant in place.

## Why it's distinctive

- **"It compiled" is not "it's correct."** Both models pass `rocky compile`. Schema-only verification cannot tell them apart. Only the data can.
- **The trap is realistic.** A `WHERE status = 'completed'` that silently matches zero rows, and an `amount_cents` summed as dollars (100x too large), are two of the most common reconcile bugs in real pipelines. They are exactly what an agent that reads column names but never samples rows gets wrong.
- **The fix is a tool, not a prompt trick.** The Rocky MCP server hands an agent `sample_rows` and `profile_column`, so it sees `'COMPLETE'` and the cents scale before it writes a filter. The correctness it learns becomes a `[[tests]]` assertion, not a comment.
- **Materialization stays human-gated.** The agent can `propose` a plan, but a human runs `rocky review <plan-id> --approve` before `rocky apply`. The agent never ships unreviewed SQL.

## The engineered scenario

`data/seed.sql` seeds eight orders:

| trap | reality | the schema-only guess |
|---|---|---|
| `status` | only ever `'COMPLETE'` (plus `'PENDING'`, `'CANCELLED'`) | `WHERE status = 'completed'` matches zero rows |
| `amount_cents` | integer **cents** | summed as dollars, 100x too large |

Five completed orders sum to `12500 + 7500 + 30000 + 5000 + 45000 = 100000` cents = **$1000.00**.

- `revenue_naive` produces `revenue_usd = NULL` — the `'completed'` filter matches nothing, so `SUM` over zero rows is NULL. A confidently shipped zero.
- `revenue_correct` produces `revenue_usd = 1000.00`.

## Layout

```
.
├── README.md                 this file
├── .mcp.json                 registers the Rocky MCP server for Claude Code
├── rocky.toml                DuckDB pipeline (persistent poc.duckdb)
├── run.sh                    deterministic, creds-free reconcile-gap proof
├── data/seed.sql             the engineered traps (uppercase status + cents)
└── models/
    ├── _defaults.toml        catalog=poc, schema=demo
    ├── revenue_naive.sql      schema-only guess — compiles, reconciles wrong
    ├── revenue_naive.toml     full_refresh, no assertion (its author never looked)
    ├── revenue_correct.sql    data-grounded — reconciles to $1000.00
    └── revenue_correct.toml   full_refresh + [[tests]] aggregate SUM = 1000
```

## Prerequisites

- `rocky` on PATH (the script also auto-detects a local `engine/target/{release,debug}/rocky` build)
- `duckdb` CLI for seeding and materializing (`brew install duckdb`)

## Run

```bash
./run.sh
```

The script is deterministic and needs no credentials. It compiles both models (both pass), shows the source data so the traps are visible, materializes both models, reconciles them side by side, and runs `rocky test --declarative` so the grounded model's assertion goes green.

> Note: the value assertion runs under `rocky test --declarative`, which executes the sidecar `[[tests]]` against the materialized target table. The default `rocky test` (no flag) only checks that a model's SQL executes — it does not evaluate `[[tests]]`. That is why `run.sh` materializes the tables first, then runs the declarative path.

## Expected output

```text
==> 4. Reconcile naive vs correct
    revenue_naive   (WHERE status='completed', cents-as-dollars) -> revenue_usd = NULL
    revenue_correct (WHERE status='COMPLETE',  cents/100.0)      -> revenue_usd = 1000.00
    Confirmed: the naive guess is wrong; only the grounded model is right.

==> 5. rocky test --declarative — the grounded model's assertion passes
  ✓ revenue_correct.revenue_usd [aggregate]
  Result: 1 passed, 0 failed, 0 warned, 0 errored
```

## The interactive MCP demo (Claude Code)

`run.sh` proves the gap with no agent in the loop. This section layers the live demo on top: an agent driving the Rocky MCP tools to reach `revenue_correct` on its own.

`.mcp.json` registers the server:

```json
{ "mcpServers": { "rocky": { "command": "rocky", "args": ["mcp", "--config", "rocky.toml"] } } }
```

Open this directory in Claude Code (it picks up `.mcp.json`), confirm the `rocky` MCP server is connected, then give it the prompt:

> Build a Rocky model for total completed-order revenue in USD from the orders source.

### Prerequisite: seed the warehouse

The MCP grounding tools read the **warehouse**, so the source data must exist in `poc.duckdb` first. Run `./run.sh` once (it seeds `poc.duckdb` with `seeds.orders`) before the interactive demo.

### Expected agent trajectory

1. **`inspect_schema`** — lists `seeds.orders` under `sources` with its typed columns: `order_id`, `customer_id`, `status`, `amount_cents`, `ordered_at`. (Physical warehouse tables the project never declared as a Rocky source are discovered here too, so the agent can find what to ground against.)
2. **`sample_rows`** on `seeds.orders` — sees real rows. The `status` values are `'COMPLETE'` (uppercase), never `'completed'`. With no `percent`, the first rows come back deterministically — the right default for a small table. This is the step a schema-only agent skips.
3. **`profile_column`** on `seeds.orders` / `amount_cents` — sees the scale: values like 12500, 30000, 45000. Integer cents, not dollars. On `status`, the result's `top_values` lists the exact literals (`COMPLETE`, `PENDING`, `CANCELLED`) that a `min`/`max` pair would hide.
4. **Writes the model** — `WHERE status = 'COMPLETE'`, `SUM(amount_cents) / 100.0 AS revenue_usd`. Equivalent to `revenue_correct.sql`.
5. **`compile`** — type-checks clean. So would the naive model; compile is necessary, not sufficient.
6. **`plan_preview`** — reads the exact SQL that would run and confirms it matches intent.
7. **`propose`** — records an AI-authored plan and returns a `plan_id`. The agent stops here. It does not apply.
8. **Human review** — you run `rocky review <plan-id>` to read the breaking-change report, then `rocky review <plan-id> --approve` to sign off, then `rocky apply <plan-id>` to materialize.
9. **`rocky test --declarative`** — the `SUM(revenue_usd) = 1000` assertion reconciles green.

Both grounding tools accept either a compiled model name **or** a qualified `schema.table` source reference (like `seeds.orders`), so an agent can ground a raw source before it has authored any model.

### Why a schema-only agent ships `revenue_naive`

Without `sample_rows` and `profile_column`, the agent has only column names. `status` strongly implies a value like `'completed'`, and an agent in a hurry sees `amount_cents` and sums it. Both guesses are reasonable from the schema and both are wrong. The model compiles, so nothing flags it. The bug surfaces only when someone reconciles the number, by which point it has shipped. The MCP tools move that discovery to authoring time, and the `[[tests]]` assertion keeps it discovered.

## Related

- Agent workflow: the `rocky-ai-workflow` skill — the same text the MCP server ships as its `instructions`.
- MCP server source: `engine/crates/rocky-mcp/`.
- Sibling POCs: [`01-model-generation/`](../01-model-generation/), [`05-schema-grounded-validation/`](../05-schema-grounded-validation/).
