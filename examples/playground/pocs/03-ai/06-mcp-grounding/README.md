# 06-mcp-grounding — schema-only models compile but reconcile wrong; the MCP tools fix it

> **Category:** 03-ai
> **Credentials:** none — the `run.sh` is deterministic and creds-free (no LLM, no Claude Code). The optional interactive demo needs Claude Code with MCP support.
> **Runtime:** < 5s
> **Rocky features:** `rocky mcp` server (`inspect_schema`, `sample_rows`, `profile_column`, `compile`, `plan_preview`, `propose`), `rocky review`/`rocky apply` human gate, `rocky test --declarative`

## What it shows

A model can compile cleanly and still be wrong, because the schema hides a column's literal values and a number's units. `data/seed.sql` seeds eight orders:

| trap | reality | the schema-only guess |
|---|---|---|
| `status` | only ever `'COMPLETE'` (plus `'PENDING'`, `'CANCELLED'`) | `WHERE status = 'completed'` matches zero rows |
| `amount_cents` | integer **cents** | summed as dollars, 100x too large |

Five completed orders sum to 100000 cents = **$1000.00**. Both models pass `rocky compile`:

- `revenue_naive.sql`: `WHERE status = 'completed'`, `SUM(amount_cents)` as dollars. Returns `revenue_usd = NULL`.
- `revenue_correct.sql`: `WHERE status = 'COMPLETE'`, `SUM(amount_cents) / 100.0`. Returns `1000.00`, locked by a `[[tests]]` assertion.

The `rocky mcp` tools (`sample_rows`, `profile_column`) let an agent see the data before writing a filter. The agent can `propose` a plan, but `rocky apply` refuses it until `rocky review <plan-id> --approve` has written a marker naming that exact plan. The check is on the marker, not on who wrote it.

## Prerequisites

- `rocky` on PATH (the script also auto-detects a local `engine/target/{release,debug}/rocky` build)
- `duckdb` CLI for seeding and materializing (`brew install duckdb`)
- `python3` (reads the compile and test JSON)

## Run

```bash
./run.sh
```

The script compiles both models, shows the source data, materializes and reconciles both, and runs `rocky test --declarative`.

> The value assertion runs only under `rocky test --declarative`, which executes the sidecar `[[tests]]` against the materialized table (hence `run.sh` materializes first). Plain `rocky test` only checks that a model's SQL executes.

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

An agent uses the Rocky MCP tools to reach `revenue_correct` on its own. Run `./run.sh` once first: the tools read the **warehouse**, so `seeds.orders` must exist in `poc.duckdb`.

`.mcp.json` registers the server:

```json
{ "mcpServers": { "rocky": { "command": "rocky", "args": ["mcp", "--config", "rocky.toml"] } } }
```

Open this directory in Claude Code, confirm the `rocky` MCP server is connected, then prompt:

> Build a Rocky model for total completed-order revenue in USD from the orders source.

### Expected agent trajectory

1. **`inspect_schema`**: lists `seeds.orders` under `sources` with typed columns `order_id`, `customer_id`, `status`, `amount_cents`, `ordered_at`. Warehouse tables never declared as a Rocky source are discovered too.
2. **`sample_rows`**: shows `status` is `'COMPLETE'`. With no `percent`, the first rows come back deterministically.
3. **`profile_column`**: on `amount_cents`, shows integer cents (12500, 30000, 45000); on `status`, `top_values` lists the literals (`COMPLETE`, `PENDING`, `CANCELLED`) that `min`/`max` would hide.
4. **Writes the model**: `WHERE status = 'COMPLETE'`, `SUM(amount_cents) / 100.0 AS revenue_usd`.
5. **`compile`**: type-checks clean (necessary, not sufficient).
6. **`plan_preview`**: shows the SQL Rocky generates offline. It is not the whole plan: a model it cannot render offline is skipped and not named in the result. This plain `full_refresh` model renders.
7. **`propose`**: records an AI-authored plan and returns a `plan_id`. The agent stops; it does not apply.
8. **Human review**: `rocky review <plan-id>` (breaking-change report), `rocky review <plan-id> --approve`, then `rocky apply <plan-id>`.
9. **`rocky test --declarative`**: the `SUM(revenue_usd) = 1000` assertion passes.

Both grounding tools accept a compiled model name **or** a `schema.table` source reference, so an agent can ground a raw source before authoring any model.

## Related

The `rocky-ai-workflow` skill (the same text the MCP server ships as its `instructions`); source in `engine/crates/rocky-mcp/`.
