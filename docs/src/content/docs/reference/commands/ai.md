---
title: AI Commands
description: Generate a model from a description, reconcile it with the warehouse, explain it, draft its tests and contract, and serve Rocky's tools to an agent over MCP
sidebar:
  order: 3
---

These commands use an AI model to draft, reconcile, explain and test models, and to draft contracts. They read `ANTHROPIC_API_KEY` from the environment. `rocky mcp` serves the same abilities to an agent. None of them materializes anything: a person runs `rocky apply`.

---

## `rocky ai`

Generate a model from a natural language description. Rocky writes the body file and a matching `.toml` sidecar (strategy and target) to the models directory, so the next `rocky apply` loads it with no manual edit.

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
| `--materialization <STRATEGY>` | `string` | `full_refresh` | Materialization strategy written into the sidecar `[strategy]` block. One of `full_refresh` or `merge`. Two values fail before any LLM call: `incremental`, because on a transformation model it re-inserts every row on each run (`E037`), and `ephemeral`, because a new model has no consumer to inline it into. |
| `--unique-key <COLUMNS>` | `string` | | Upsert key for `--materialization=merge`. Maps to `[strategy].unique_key` (an array) in the sidecar. Accepts a comma-separated list (`--unique-key id,created_at`) or repeated flags. Required with `merge`: without it the sidecar is incomplete and `rocky run` rejects it. `time_interval`, `delete_insert`, `microbatch`, `materialized_view`, `dynamic_table` and `content_addressed` are not offered. |
| `--target <FQN>` | `string` | `generated.ai.<name>` | Target table coordinates as `catalog.schema.table`. Written into the sidecar `[target]` block. |
| `--overwrite` | `bool` | `false` | Overwrite an existing body or sidecar file at the destination. Without this flag, the command fails loudly rather than silently clobber user-authored models. |

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

The emitted sidecar (`models/fct_orders_daily.toml`) carries the parsed materialization, key and target:

```toml
name = "fct_orders_daily"

[strategy]
type = "merge"
unique_key = ["order_date"]

[target]
catalog = "analytics"
schema = "marts"
table = "fct_orders_daily"
```

`--materialization incremental` and `--materialization ephemeral` fail before any LLM call, with the reasons in the flag table. `--watermark` does not exist: passing it is an argument error.

Generate raw SQL instead (still emits both `.sql` body and `.toml` sidecar):

```bash
rocky ai "top 10 customers by lifetime value" --format sql
```

With `--format sql`, `source` holds plain SQL (no Jinja, no templating) with bare model references that the compiler resolves against project models.

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

Each proposal carries a unified `diff` and the full `proposed_source`. The command is a dry run by default.

`--model fct_revenue --apply` syncs one model and writes `proposed_source` to disk after the proposal passes the compile-verify loop. `--with-intent --models src/models` checks only models that have intent metadata.

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
        },
        }
      ]
    }
  ]
}
```

Each test is an assertion query: it passes when the query returns 0 rows. Rocky's test SQL references models by bare name (no Jinja), matching how the compiler resolves refs.

With `--all --save`, each model's assertions are written as one `.sql` file under `tests/`, and `rocky test` picks them up. `--models <PATH>` selects another directory.

### Related Commands

- [`rocky test`](/reference/commands/modeling/#rocky-test) -- run the generated tests via DuckDB
- [`rocky ai-explain`](#rocky-ai-explain) -- generate intent that improves test quality
- [`rocky ci`](/reference/commands/modeling/#rocky-ci) -- compile + test in CI

---

## `rocky ai-contract`

Draft a data contract from a model's observed data, instead of writing the column list by hand. Rocky profiles the target table, sends the shape to Anthropic, and prints the draft. It works on any configured warehouse. The profile reads the whole table, once per column, which costs money on a metered warehouse such as Snowflake, BigQuery or Databricks.

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

Rocky reads the API key from the `ANTHROPIC_API_KEY` environment variable, never from `rocky.toml`. See [`[ai]`](/reference/configuration/#ai) for the token budget.

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

Run a [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server over stdio, exposing Rocky's verification, data-grounding, and draft-generation surface to any MCP-capable agent harness (Claude Desktop, Claude Code, your own client). The server is long-running: it serves until the client disconnects.

```bash
rocky mcp [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--config <PATH>` | `PathBuf` | `rocky.toml` | Pipeline config file the server resolves the project from. The models directory is resolved as `<config-dir>/models`. |
| `--profile <PROFILE>` | `default` \| `approver` \| `worker` | `default` | Which tool surface to serve. `default` serves all 31 tools but refuses `review_queue`'s approve action (listing the queue still works). `approver` serves the same 31 tools and allows that action, so the server can write approval markers. `worker` serves the minimal drafting allowlist for an untrusted worker. |

The server is **stateless**: every tool call resolves the project from the config + models dir and compiles fresh, so it always reflects the current on-disk files. Logging goes to stderr (stdout is reserved for the MCP wire protocol).

### Local and bring-your-own-key

`rocky mcp` runs locally. There is no Rocky-hosted service.

- Warehouse-touching tools use the credentials in your `rocky.toml`.
- The draft generators call the Anthropic API with your own `ANTHROPIC_API_KEY` from the server environment. Without it, those tools return an empty draft and a `message` that says why.
- The generators send your model's SQL and schema to your key. `ai_contract` sends only aggregate column counts (row, null, distinct), never raw cell values.

### Safety model: the server does not materialize

The server **never materializes anything**. No tool runs SQL that changes your warehouse. An agent can write project files and record a plan. One tool, `review_queue`, can also write the approval marker for a plan that is already in the pending review queue — but only on `rocky mcp --profile approver`. The default server refuses that call.

```
   through rocky mcp                       through the CLI
   ─────────────────                       ───────────────
   draft_model     ──writes──► models/<name>.sql + sidecar
   draft_contract  ──writes──► models/<model>.contract.toml
   draft_check     ──writes──► [[tests]] in the sidecar
        │
        ▼
   propose         ──writes──► .rocky/plans/<plan-id>.json
                                        │
                                        ▼
                              rocky review <plan-id> --approve
                              writes the approval marker. The
                              review_queue MCP tool writes it
                              too, with confirm: true — but ONLY
                              on --profile approver. The default
                              server refuses (approve_not_enabled).
                                        │
                                        ▼
                              rocky apply <plan-id> ──► warehouse
                              no MCP tool runs this step
```

Three rules keep that boundary in place:

- The generators (`ai_contract`, `ai_test`, `explain_model`) return **drafts** and mutate nothing. Hand a draft to `draft_contract` or `draft_check`. After review and apply, run sidecar checks with `test` and `declarative = true`.
- `governance_preview` and `drift_preview` are **read-only** previews.
- The server does not apply. `rocky apply` is the only step that WRITES to the warehouse, and no MCP tool runs it. Some tools do read the warehouse: `sample_rows`, `profile_column`, `inspect_schema`, and `drift_preview` issue queries against it. `review_queue` can write a plan's approval marker, but only on `rocky mcp --profile approver` — the default server refuses the call with `approve_not_enabled` and writes nothing. Where it is served it still needs `confirm: true` from the caller, and it still refuses a plan that is not already in the pending review queue. `rocky mcp --profile worker` does not serve `review_queue` at all. So treat approval as a step the server can take **only on a server you started for that purpose**; the profile is chosen at launch and an agent cannot change it mid-session.

### Tools

**Verify and ground** (read-only; the typed surface a raw shell can't reproduce):

| Tool | What it does |
|---|---|
| `compile` | Type-check the project and return diagnostics (errors / warnings). |
| `plan_preview` | Render the SQL Rocky generates, offline, with no warehouse I/O. Not the whole plan: a model it cannot render offline is skipped and is not named in the result. Skipped by construction: Snowflake dynamic tables, time-interval models, and content-addressed models. |
| `lineage` | Column-level lineage for a model (or a single column). |
| `test` | Run local model and fixture tests by default. Set `declarative = true` to run sidecar `[[tests]]` against the configured warehouse after apply. |
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

**LLM generators** (mutate nothing; require `ANTHROPIC_API_KEY`; named for the `rocky ai-*` verbs):

| Tool | What it does |
|---|---|
| `ai_contract` | Draft a `.contract.toml` from a model's aggregate per-column profile, compile-verified against its inferred schema. Sends only aggregate statistics — never raw cell values. |
| `ai_test` | Draft SQL test assertions (not-null, grain uniqueness, ranges, referential integrity) for a model. |
| `explain_model` | Draft an intent description for a model from its SQL and schema. |
| `suggest_freshness_block` | Draft a `[freshness]` TOML block for a model with temporal columns. |

**Write path** (write into `models/`, compile in the same call, policy-gated; no Anthropic key required):

| Tool | What it does |
|---|---|
| `draft_model` | Write `models/<name>.sql` + sidecar and compile it. |
| `draft_contract` | Write `models/<model>.contract.toml` (your `spec`) and compile-validate it against the model's schema. |
| `draft_check` | Merge declarative `[[tests]]` checks (your `spec`) into the model's sidecar and compile. |
| `draft_metadata` | Parse-merge a structured freshness / classification patch into the model's sidecar and compile. The policy check runs against the sidecar as patched. |

A `draft_*` call made without its content `spec` returns an actionable error pointing at the matching `ai_*` generator.

**Governor** (read the ledger and the scheduler; one safe-direction write):

| Tool | What it does |
|---|---|
| `estate_brief` | The governor's digest, as [`rocky brief`](/reference/commands/governance-reclamation/#rocky-brief) prints it. Read-only. |
| `audit_query` | The custody chain for a model, a run id or a plan id, as `rocky audit --for`. Read-only. |
| `scorecard` | Acceptance, denial and review rates per principal, rule or scope, as `rocky audit --scorecard`. Read-only. |
| `schedule_status` | The scheduler's stored state per pipeline and the queued webhook demands. Read-only: it does not evaluate demand. |
| `pause_schedule` | Pause a pipeline's schedule. Requires `confirm: true`. There is no resume tool: resume is human-only. |
| `review_queue` | List the pending-review queue. On `--profile approver` only, it can also write an approval marker. See the safety model above. |

**Propose** (the one plan write: not a materialization; no Anthropic key required):

| Tool | What it does |
|---|---|
| `propose` | Record an **AI-authored plan** for materializing a model. Writes a plan only — it compiles the project and records the plan offline (no LLM call, no warehouse write). `rocky apply <plan_id>` then runs it only once `rocky review <plan_id> --approve` has written the approval marker. |

### Prompts (guided trajectories)

The server also exposes MCP prompts that orchestrate the tools above into a guided workflow. Every trajectory **stops at the propose step**; it never applies.

| Prompt | What it guides |
|---|---|
| `build_model` | Author one model from a plain-language intent through Rocky's inspect → sample → write → compile-loop → propose authoring loop. |
| `find_untested_models` | Catalog the project, find models with no declarative tests, and draft tests for them. |
| `add_tests_to_pks` | Add uniqueness + not-null tests to a model's primary-key / unique columns. |
| `summarize_project` | Produce a structured, read-only summary of the project (uses only read-only tools). |
| `fix_failing_test` | Run `test`, then for each failure diagnose and draft a fix — stopping at the proposal. |

### Examples

```bash
rocky mcp                                          # default config
rocky mcp --config pipelines/prod.toml             # a specific project
claude mcp add rocky -- rocky mcp --config rocky.toml   # register with Claude Code
```

### Related Commands

- [`rocky ai`](#rocky-ai) -- one-shot model generation from the CLI (no MCP client needed)
- [`rocky apply`](/reference/commands/core-pipeline/#rocky-apply) -- execute an AI-authored plan (`rocky review <plan_id> --approve` writes the marker it requires first)
