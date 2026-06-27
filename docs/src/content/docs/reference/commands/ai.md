---
title: AI Commands
description: AI-powered model generation, schema sync, intent explanation, and test generation
sidebar:
  order: 3
---

AI-assisted commands for generating, reconciling, explaining, and testing Rocky models. These commands require an AI provider to be configured.

---

## `rocky ai`

Generate a model from a natural language description and write **both the body file and a matching `.toml` sidecar** to the models directory. The emitted sidecar carries the materialization strategy and target coordinates, so Rocky's model loader picks the generated model up on the next `rocky apply` without manual editing.

```bash
rocky ai <intent> [flags]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `intent` | `string` | **(required)** | Natural language description of the model to generate. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--format <FORMAT>` | `string` | `rocky` | Output format: `rocky` (`.rocky` body + `.toml` sidecar) or `sql` (`.sql` body + `.toml` sidecar). |
| `--models <PATH>` | `string` | `models` | Models directory. Used both to ground the prompt in real schemas and as the destination directory for the emitted body + sidecar. |
| `--materialization <STRATEGY>` | `string` | `full_refresh` | Materialization strategy written into the sidecar `[strategy]` block. One of `full_refresh`, `incremental`, `merge`, `ephemeral`. |
| `--watermark <COLUMN>` | `string` | | Watermark column for `--materialization=incremental`. Maps to `[strategy].timestamp_column` in the sidecar. Required when materialization is `incremental`; ignored otherwise. |
| `--target <FQN>` | `string` | `generated.ai.<name>` | Target table coordinates as `catalog.schema.table`. Written into the sidecar `[target]` block. |
| `--overwrite` | `bool` | `false` | Overwrite an existing body or sidecar file at the destination. Without this flag, the command fails loudly rather than silently clobber user-authored models. |

:::note[Merge limitation]
`--materialization merge` v1 is incomplete: there is no `--unique-key` flag yet, so the emitted sidecar is missing the merge key and must be hand-edited before `rocky apply` will accept it. `time_interval`, `delete_insert`, `microbatch`, `materialized_view`, `dynamic_table`, and `content_addressed` are intentionally not exposed by `--materialization`: they need richer flag plumbing or live in the IR-only surface.
:::

### Examples

Generate a revenue model (Rocky DSL, the default):

```bash
rocky ai "monthly revenue by customer, joining orders and refunds"
```

```json
{
  "version": "1.30.0",
  "command": "ai",
  "intent": "monthly revenue by customer, joining orders and refunds",
  "format": "rocky",
  "name": "fct_monthly_revenue_by_customer",
  "source": "from stg_orders\njoin stg_refunds on order_id {\n    keep stg_refunds.refund_amount\n}\nderive {\n    revenue_month: date_trunc('month', order_date),\n    net_revenue: total_amount - coalesce(refund_amount, 0)\n}\ngroup customer_id, revenue_month {\n    customer_id,\n    revenue_month,\n    net_revenue: sum(net_revenue)\n}",
  "attempts": 1,
  "body_path": "models/fct_monthly_revenue_by_customer.rocky",
  "sidecar_path": "models/fct_monthly_revenue_by_customer.toml"
}
```

Generate an incremental model with a watermark, into a non-default target:

```bash
rocky ai "daily order facts from stg_orders" \
  --materialization incremental --watermark order_date \
  --target analytics.marts.fct_orders_daily
```

The emitted sidecar (`models/fct_orders_daily.toml`) carries the parsed materialization + watermark + target:

```toml
name = "fct_orders_daily"

[strategy]
type = "incremental"
timestamp_column = "order_date"

[target]
catalog = "analytics"
schema  = "marts"
table   = "fct_orders_daily"
```

Generate raw SQL instead (still emits both `.sql` body and `.toml` sidecar):

```bash
rocky ai "top 10 customers by lifetime value" --format sql
```

The `source` field then contains standard SQL using bare model references (resolved by the compiler against project models):

```sql
SELECT
  customer_id,
  SUM(total_amount) AS lifetime_value,
  COUNT(DISTINCT order_id) AS total_orders,
  MIN(order_date) AS first_order,
  MAX(order_date) AS last_order
FROM stg_orders
GROUP BY customer_id
ORDER BY lifetime_value DESC
LIMIT 10
```

Rocky generates plain SQL: no Jinja, no templating. `stg_orders` is resolved by the compiler to the project model of that name.

### Related Commands

- [`rocky ai-explain`](#rocky-ai-explain) -- generate intent descriptions for existing models
- [`rocky ai-test`](#rocky-ai-test) -- generate tests from model intent
- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- compile the generated model

---

## `rocky ai-sync`

Detect schema changes in upstream sources and propose intent-guided model updates. Compares the current state of source schemas against what models expect and suggests SQL modifications that preserve each model's declared intent.

```bash
rocky ai-sync [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--apply` | `bool` | `false` | Apply proposed changes (default: dry run). |
| `--model <NAME>` | `string` | | Filter to a specific model. |
| `--with-intent` | `bool` | `false` | Only show models that have intent metadata. |
| `--models <PATH>` | `string` | `models` | Models directory. |

### Examples

Dry-run sync detection across all models:

```bash
rocky ai-sync
```

```json
{
  "version": "1.30.0",
  "command": "ai-sync",
  "proposals": [
    {
      "model": "fct_revenue",
      "intent": "Monthly net revenue per customer after refunds",
      "diff": "--- a/models/fct_revenue.sql\n+++ b/models/fct_revenue.sql\n@@ -3,5 +3,6 @@\n     o.customer_id,\n     DATE_TRUNC('month', o.order_date) AS revenue_month,\n-    SUM(o.total_amount) - COALESCE(SUM(r.refund_amount), 0) AS net_revenue\n+    SUM(o.total_amount * (1 - o.discount_pct)) - COALESCE(SUM(r.refund_amount), 0) AS net_revenue",
      "proposed_source": "SELECT\n  o.customer_id,\n  DATE_TRUNC('month', o.order_date) AS revenue_month,\n  SUM(o.total_amount * (1 - o.discount_pct)) - COALESCE(SUM(r.refund_amount), 0) AS net_revenue\nFROM stg_orders o\nLEFT JOIN stg_refunds r ON o.order_id = r.order_id\nGROUP BY 1, 2"
    }
  ]
}
```

Each proposal carries a unified `diff` (ready to show in a review UI) plus the full `proposed_source` (ready to write if you apply). The sync command is dry-run by default.

Sync a specific model and apply changes:

```bash
rocky ai-sync --model fct_revenue --apply
```

Same output shape. `--apply` writes `proposed_source` to disk after the proposal passes the compile-verify loop.

Only check models that have intent metadata:

```bash
rocky ai-sync --with-intent --models src/models
```

### Related Commands

- [`rocky ai-explain`](#rocky-ai-explain) -- add intent to models before syncing
- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- recompile after applying changes
- [`rocky lineage`](/reference/commands/modeling/#rocky-lineage) -- understand column dependencies affected by changes

---

## `rocky ai-explain`

Generate natural language intent descriptions from existing model SQL. Analyzes the SQL logic and produces human-readable descriptions of what each model does, which can be saved to the model's TOML sidecar for use by `ai-sync`.

```bash
rocky ai-explain [model] [flags]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `model` | `string` | | Model name to explain. If omitted, requires `--all`. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--all` | `bool` | `false` | Explain all models that do not already have intent metadata. |
| `--save` | `bool` | `false` | Save the generated intent descriptions to each model's TOML config. |
| `--models <PATH>` | `string` | `models` | Models directory. |

### Examples

Explain a single model:

```bash
rocky ai-explain fct_revenue
```

```json
{
  "version": "1.30.0",
  "command": "ai-explain",
  "explanations": [
    {
      "model": "fct_revenue",
      "intent": "Calculates monthly net revenue per customer by joining orders with refunds. Groups by customer and month, computing total order amounts minus refund amounts.",
      "saved": false
    }
  ]
}
```

Explain all models without intent and save to TOML:

```bash
rocky ai-explain --all --save
```

```json
{
  "version": "1.30.0",
  "command": "ai-explain",
  "explanations": [
    { "model": "fct_revenue",   "intent": "Calculates monthly net revenue per customer by joining orders with refunds.", "saved": true },
    { "model": "dim_customers", "intent": "Customer dimension combining profile data with computed lifetime metrics.",  "saved": true },
    { "model": "fct_orders",    "intent": "Order fact table enriched with customer and product dimensions.",             "saved": true }
  ]
}
```

Explain models from a custom directory:

```bash
rocky ai-explain fct_revenue --models src/transformations
```

### Related Commands

- [`rocky ai-sync`](#rocky-ai-sync) -- use intent metadata for schema change proposals
- [`rocky ai`](#rocky-ai) -- generate new models from intent
- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- compile to verify the model structure

---

## `rocky ai-test`

Generate test assertions from a model's intent and SQL logic. Produces assertion queries that validate the model's expected behavior.

```bash
rocky ai-test [model] [flags]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `model` | `string` | | Model name to generate tests for. If omitted, requires `--all`. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--all` | `bool` | `false` | Generate tests for all models. |
| `--save` | `bool` | `false` | Save generated tests to the `tests/` directory. |
| `--models <PATH>` | `string` | `models` | Models directory. |

### Examples

Generate tests for a single model:

```bash
rocky ai-test fct_revenue
```

```json
{
  "version": "1.30.0",
  "command": "ai-test",
  "results": [
    {
      "model": "fct_revenue",
      "saved": false,
      "tests": [
        {
          "name": "net_revenue_is_not_negative",
          "description": "Net revenue should never be negative after refunds",
          "sql": "SELECT COUNT(*) FROM fct_revenue WHERE net_revenue < 0"
        },
        {
          "name": "customer_id_not_null",
          "description": "Every revenue row must have a customer",
          "sql": "SELECT COUNT(*) FROM fct_revenue WHERE customer_id IS NULL"
        },
        {
          "name": "no_duplicate_customer_months",
          "description": "Each customer should have at most one row per month",
          "sql": "SELECT COUNT(*) FROM (SELECT customer_id, revenue_month, COUNT(*) AS cnt FROM fct_revenue GROUP BY 1, 2 HAVING cnt > 1)"
        }
      ]
    }
  ]
}
```

Each test is an assertion query: it passes when the query returns 0 rows. Rocky's test SQL references models by bare name (no Jinja), matching how the compiler resolves refs.

Generate and save tests for all models (`saved: true` per model, full test bodies elided here):

```bash
rocky ai-test --all --save
```

```json
{
  "version": "1.30.0",
  "command": "ai-test",
  "results": [
    { "model": "fct_revenue",   "saved": true, "tests": [ /* 3 assertions */ ] },
    { "model": "dim_customers", "saved": true, "tests": [ /* 2 assertions */ ] },
    { "model": "fct_orders",    "saved": true, "tests": [ /* 4 assertions */ ] }
  ]
}
```

With `--save`, each assertion is written out as a `.sql` file under `tests/` (one file per model) so `rocky test` picks them up.

Generate tests from a custom models directory:

```bash
rocky ai-test fct_revenue --models src/transformations --save
```

### Related Commands

- [`rocky test`](/reference/commands/modeling/#rocky-test) -- run the generated tests via DuckDB
- [`rocky ai-explain`](#rocky-ai-explain) -- generate intent that improves test quality
- [`rocky ci`](/reference/commands/modeling/#rocky-ci) -- compile + test in CI

---

## `rocky mcp`

Run a [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server over stdio, exposing Rocky's verification, data-grounding, and draft-generation surface to any MCP-capable agent harness (Claude Desktop, Claude Code, your own client). The server is long-running: it serves until the client disconnects.

```bash
rocky mcp [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--config <PATH>` | `PathBuf` | `rocky.toml` | Pipeline config file the server resolves the project from. The models directory is resolved as `<config-dir>/models`. |

The server is **stateless**: every tool call resolves the project from the config + models dir and compiles fresh, so it always reflects the current on-disk files. Logging goes to stderr (stdout is reserved for the MCP wire protocol).

### Local and bring-your-own-key

`rocky mcp` runs entirely **locally against your own infrastructure**. There is no Rocky-hosted service:

- Warehouse-touching tools hit **your own warehouse** with the credentials in your `rocky.toml`.
- The draft generators call the Anthropic API using **your own `ANTHROPIC_API_KEY`** from the server environment. Without the key set, those tools return a null/empty draft and a `message` explaining why.

What leaves your machine is bounded: warehouse queries go to your warehouse; the generators send your model's SQL and schema to **your own** Anthropic key, and for `draft_contract` only aggregate column counts (row / null / distinct), **never raw cell values**.

### Safety model: read-only and propose-only

The server **never materializes anything**. Materialization stays human-gated:

- The generators (`draft_contract`, `generate_tests`, `explain_model`) return **drafts** and mutate nothing; you save them to disk and run `compile` / `test` yourself.
- `governance_preview` and `drift_preview` are **read-only** previews.
- The `propose` tool only writes an **AI-authored plan**; a human runs `rocky review <plan_id> --approve` then `rocky apply <plan_id>`. The server never approves on the user's behalf.

### Tools

**Verify and ground** (read-only; the typed surface a raw shell can't reproduce):

| Tool | What it does |
|---|---|
| `compile` | Type-check the project and return diagnostics (errors / warnings). |
| `plan_preview` | Preview the exact SQL Rocky would execute. Computed offline; no warehouse I/O. |
| `lineage` | Column-level lineage for a model (or a single column). |
| `test` | Run the project's DuckDB-backed local tests (contracts + assertions). |
| `list` | List project entities (`models`, `pipelines`, `adapters`, `sources`). |
| `inspect_schema` | Typed columns of every model and source table — works at cold start, before anything is materialized. |
| `catalog` | The project-wide asset catalog (every model + source) in one call. |
| `breaking_change` | Classify semantic breaking changes between the working tree and a base ref. |
| `dependents` | List the downstream models that depend on a given model. |
| `history` | Read run history from the state store. |
| `metrics` | Read a model's quality-metric snapshots from the state store. |
| `optimize` | Cost-model materialization recommendations from run history. |
| `sample_rows` | Sample real rows from a model's target table or a qualified `schema.table`. Hits your warehouse. |
| `profile_column` | Profile one column of a target table or qualified `schema.table`. Hits your warehouse. |
| `governance_preview` | DRY-RUN of the classification / masking / retention actions a `rocky run` would reconcile. Computed offline; no warehouse I/O. |
| `drift_preview` | Source-vs-target schema drift between two warehouse tables. Hits your warehouse. |

**Draft generators** (mutate nothing; require `ANTHROPIC_API_KEY`):

| Tool | What it does |
|---|---|
| `draft_contract` | Draft a `.contract.toml` from a model's aggregate per-column profile, compile-verified against its inferred schema. Sends only aggregate statistics — never raw cell values. |
| `generate_tests` | Draft SQL test assertions (not-null, grain uniqueness, ranges, referential integrity) for a model. |
| `explain_model` | Draft an intent description for a model from its SQL and schema. |
| `suggest_freshness_block` | Draft a `[freshness]` TOML block for a model with temporal columns. |

**Propose** (the one write: a plan, not a materialization; no Anthropic key required):

| Tool | What it does |
|---|---|
| `propose` | Record an **AI-authored plan** for materializing a model. Writes a plan only — it compiles the project and records the plan offline (no LLM call, no warehouse write). A human must run `rocky review <plan_id> --approve` then `rocky apply <plan_id>`. |

### Prompts (guided trajectories)

The server also exposes MCP prompts that orchestrate the tools above into a guided workflow. Every trajectory **stops at the propose / human-gate step**; it never applies.

| Prompt | What it guides |
|---|---|
| `build_model` | Author one model from a plain-language intent through Rocky's inspect → sample → write → compile-loop → propose authoring loop. |
| `find_untested_models` | Catalog the project, find models with no declarative tests, and draft tests for them. |
| `add_tests_to_pks` | Add uniqueness + not-null tests to a model's primary-key / unique columns. |
| `summarize_project` | Produce a structured, read-only summary of the project (uses only read-only tools). |
| `fix_failing_test` | Run `test`, then for each failure diagnose and draft a fix — stopping at the proposal. |

### Examples

Start the server against the default config:

```bash
rocky mcp
```

Start it against a specific project config:

```bash
rocky mcp --config pipelines/prod.toml
```

Register it with an MCP client (Claude Code example):

```bash
claude mcp add rocky -- rocky mcp --config rocky.toml
```

### Related Commands

- [`rocky ai`](#rocky-ai) -- one-shot model generation from the CLI (no MCP client needed)
- [`rocky apply`](/reference/commands/core-pipeline/#rocky-run) -- execute an approved AI-authored plan (a human runs `rocky review <plan_id> --approve` first)
