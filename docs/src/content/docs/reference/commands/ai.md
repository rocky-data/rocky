---
title: AI Commands
description: Generate a model from a description, reconcile it with the warehouse, explain it, draft its tests and contract, and serve Rocky's tools to an agent over MCP
sidebar:
  order: 3
---

These commands use an AI model to draft, reconcile, explain and test models, and to draft contracts. They read `ANTHROPIC_API_KEY` from the environment, never from `rocky.toml`. The token budget is [`[ai]`](/reference/configuration/#ai). None of them materializes anything: a person runs `rocky apply`. Task steps: [AI Features](/guides/ai-features/).

---

## `rocky ai`

Generate a model from a natural language description. Rocky writes the body file and a matching `.toml` sidecar (strategy and target) to the models directory.

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
| `--models <PATH>` | `string` | `models` | Models directory. Grounds the prompt in real schemas and receives the body and sidecar. |
| `--materialization <STRATEGY>` | `string` | `full_refresh` | Materialization strategy written into the sidecar `[strategy]` block. One of `full_refresh` or `merge`. Two values fail before any LLM call: `incremental`, because on a transformation model it re-inserts every row on each run (`E037`), and `ephemeral`, because a new model has no consumer to inline it into. |
| `--unique-key <COLUMNS>` | `string` | | Upsert key for `merge`. Maps to `[strategy].unique_key` (an array) in the sidecar. Takes a comma-separated list (`--unique-key id,created_at`) or repeated flags. Required with `merge`: without it `rocky run` rejects the sidecar. `time_interval`, `delete_insert`, `microbatch`, `materialized_view`, `dynamic_table` and `content_addressed` are not offered. |
| `--target <FQN>` | `string` | `generated.ai.<name>` | Target table coordinates as `catalog.schema.table`. Written into the sidecar `[target]` block. |
| `--overwrite` | `bool` | `false` | Overwrite an existing body or sidecar. Without it the command fails. |

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
  "source": "from stg_orders\njoin stg_refunds ...",
  "attempts": 1,
  "body_path": "models/fct_monthly_revenue_by_customer.rocky",
  "sidecar_path": "models/fct_monthly_revenue_by_customer.toml"
}
```

Generate a merge model keyed on `order_date`, into a non-default target:

```bash
rocky ai "daily order facts from stg_orders" \
  --materialization merge --unique-key order_date \
  --target analytics.marts.fct_orders_daily
```

The sidecar carries the parsed materialization, key and target. The result is shown in [AI Features](/guides/ai-features/#pick-a-materialization--target). `--materialization incremental` and `--materialization ephemeral` fail before any LLM call, with the reasons in the flag table. `--watermark` does not exist: passing it is an argument error.

Generate raw SQL instead:

```bash
rocky ai "top 10 customers by lifetime value" --format sql
```

With `--format sql`, `source` holds plain SQL with bare model references that the compiler resolves.

### Related Commands

- [`rocky ai-explain`](#rocky-ai-explain) -- generate intent descriptions for existing models
- [`rocky ai-test`](#rocky-ai-test) -- generate tests from model intent
- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- compile the generated model

---

## `rocky ai-sync`

Detect schema changes in upstream sources and propose model updates that keep each model's declared intent.

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

Each proposal carries a unified `diff` and the full `proposed_source`.

`--model fct_revenue --apply` syncs one model and writes `proposed_source` after the proposal passes the compile-verify loop.

### Related Commands

- [`rocky ai-explain`](#rocky-ai-explain) -- add intent to models before syncing
- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- recompile after applying changes
- [`rocky lineage`](/reference/commands/modeling/#rocky-lineage) -- understand column dependencies affected by changes

---

## `rocky ai-explain`

Generate plain-language intent descriptions from model SQL. Save them to the model's TOML sidecar for `ai-sync`.

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

`rocky ai-explain --all --save` explains every model without intent and sets `saved: true` on each entry. `--models <PATH>` selects another directory.

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
        }
      ]
    }
  ]
}
```

Each test is an assertion query that passes when it returns 0 rows. It references models by bare name.

With `--save`, each assertion is written as a `.sql` file under `tests/`. `rocky test` and `rocky ci` do not read that directory: see [AI Features](/guides/ai-features/#run-the-generated-tests).

### Related Commands

- [`rocky ai-explain`](#rocky-ai-explain) -- generate intent that improves test quality

---

## `rocky ai-contract`

Draft a data contract from a model's observed data. Rocky profiles the target table, sends the shape to Anthropic, and prints the draft. It works on any configured warehouse. The profile reads the whole table, once per column, which costs money on a metered warehouse such as Snowflake, BigQuery or Databricks.

```bash
rocky ai-contract <model> [flags]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `model` | `string` | **(required)** | Model whose target table to profile. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--save` | `bool` | `false` | Write the draft to `<model>.contract.toml` in the models directory instead of printing it. |
| `--with-data` | `bool` | `false` | Also send observed cell **values** (min/max and samples of low-cardinality columns). Without it, only the schema and the row, null and distinct counts leave the machine. |
| `--models <PATH>` | `string` | `models` | Models directory, and the destination for `--save`. |

### Examples

```bash
rocky ai-contract fct_orders           # print the draft
rocky ai-contract fct_orders --save    # write models/fct_orders.contract.toml
```

### Related Commands

- [`rocky profile`](/reference/cli/#rocky-profile) -- the per-column profile the draft starts from
- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- check the model against the saved contract

---

## `rocky mcp`

Run a [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server over stdio. It exposes Rocky's verification, grounding and draft-generation tools to any MCP-capable agent harness. The server is long-running: it serves until the client disconnects.

```bash
rocky mcp [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--config <PATH>` | `PathBuf` | `rocky.toml` | Pipeline config the server resolves the project from. The models directory is `<config-dir>/models`. |
| `--profile <PROFILE>` | `default` \| `approver` \| `worker` | `default` | Tool surface to serve. `default`: all 31 tools, approve refused. `approver`: the same 31 tools, approve allowed. `worker`: a minimal drafting allowlist. See [Profiles](/concepts/mcp-authoring/#profiles). |

The server is **stateless**: every tool call compiles the current on-disk files. Logging goes to stderr. Stdout is reserved for the MCP wire protocol.

The server never materializes anything. No tool runs SQL that changes your warehouse, and no tool runs `rocky apply`. Tool list, bring-your-own-key and data egress: [MCP Authoring](/concepts/mcp-authoring/). Gates and the approval marker: [Operating Rocky with Agents](/concepts/operating-rocky-with-agents/).

### Examples

```bash
rocky mcp                                          # default config
rocky mcp --config pipelines/prod.toml             # a specific project
claude mcp add rocky -- rocky mcp --config rocky.toml   # register with Claude Code
```

### Related Commands

- [`rocky ai`](#rocky-ai) -- one-shot model generation from the CLI
- [`rocky apply`](/reference/commands/core-pipeline/#rocky-apply) -- execute an AI-authored plan (`rocky review <plan_id> --approve` writes the marker it requires first)
