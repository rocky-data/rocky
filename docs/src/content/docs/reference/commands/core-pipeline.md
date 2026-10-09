---
title: Core Pipeline Commands
description: "Start a project, check it, plan the SQL, apply it, and read the state store back"
sidebar:
  order: 1
---

These commands cover a Rocky pipeline's whole life. You create the project, check the config, and see what the source holds. Then you plan the SQL, apply the plan, and read back what ran.

The two commands to know first are `rocky plan` and `rocky apply`. `rocky plan` prints the SQL it generates and writes a plan file with an id. For a `run` plan that file holds the settings the run needs, not the SQL. `rocky apply` reads the file and recompiles the project to run it. [`rocky run`](#rocky-run) fuses both into one step for local work.

## Global Flags

The global flags (`--config`, `--output`, `--state-path`, `--state-namespace`, `--principal`, `--cache-ttl`) apply to every command. Put `--config`, `--state-path` and `--state-namespace` before the subcommand: `rocky --config prod.toml run`, not `rocky run --config prod.toml`. See [Global Flags in the CLI Reference](/reference/cli/#global-flags) for the canonical list, defaults, and the `--state-path` resolution order.

### `--state-namespace`

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--state-namespace <KEY>` | `string` | (none) | Route this invocation to its own `<models>/.rocky-state/<KEY>.redb` so concurrent fan-out runs (one per pipeline / client / tenant) don't serialize on redb's single-writer lock. **Opt-in, default-off**: with neither this flag nor `[state] namespacing` set, behavior is byte-identical to the single global state file. `<KEY>` must be a SQL identifier (`^[a-zA-Z0-9_]+$`). An explicit `--state-path` disables namespacing for that invocation; otherwise this flag wins over the `[state] namespacing` config. See [State namespacing](/reference/configuration/#state-namespacing). |

---

## `rocky init`

Create a new Rocky project: a starter `rocky.toml` plus a `models/` directory.

```bash
rocky init [path] [flags]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `path` | `string` | `.` | Directory where the project will be created. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--template <NAME>` | `string` | `duckdb` | Scaffold template. One of `duckdb`, `databricks-fivetran`, `snowflake`, `bigquery`, `trino`. Each template emits a runnable `rocky.toml` with the matching adapter wired up via `${VAR}` env-var placeholders (never inline secrets) plus a `models/welcome.{sql,toml}` that compiles with no source tables. |

### Examples

Create a project in the current directory (default DuckDB template):

```bash
rocky init
```

```
Created rocky.toml
Created models/
Rocky project initialized.
```

Create a Trino-targeted project in a new directory:

```bash
rocky init acme-trino --template trino
```

The emitted `rocky.toml` wires the `trino` adapter to `${TRINO_HOST}` / `${TRINO_USER}` / `${TRINO_PASSWORD}` (HTTP Basic) or `${TRINO_JWT}` (JWT bearer), with inline TOML comments documenting both auth modes.

### Related Commands

- [`rocky validate`](#rocky-validate) -- check the generated config
- [`rocky playground`](/reference/commands/development/#rocky-playground) -- create a sample project with DuckDB (no credentials needed)

---

## `rocky validate`

Check the pipeline configuration. `rocky validate` connects to no external API and exits non-zero if any check fails.

```bash
rocky validate [flags]
```

### Flags

No command-specific flags. Uses [global flags](#global-flags) only.

### Checks Performed

| Check | Description |
|-------|-------------|
| TOML syntax | The config file parses without errors as v2 (named adapters + named pipelines). |
| Adapters | Each `[adapter.NAME]` is a recognized type (`databricks`, `snowflake`, `duckdb`, `fivetran`, `bigquery`, `trino`, `airbyte`, `iceberg`, `manual`) with the required fields populated. For Databricks, at least one of `token` or `client_id`/`client_secret` must be set. The known-types list is driven directly off the adapter registry, so new first-party adapters propagate without a follow-up edit. |
| Pipelines | Each `[pipeline.NAME]` references existing adapters for source, target, and (optional) discovery, and its `schema_pattern` parses. |
| DAG validation | If `models/` exists, loads all models and checks for dependency cycles. |

### Examples

Validate the default config:

```bash
rocky validate
```

```
ok  Config syntax valid (v2 format)
ok  adapter.fivetran: fivetran
ok  adapter.prod: databricks (auth configured)
ok  pipeline.bronze: schema pattern parseable
ok  pipeline.bronze: replication / incremental -> warehouse / stage__{source}

Validation complete.
```

Validate a specific config file:

```bash
rocky -c pipelines/prod.toml validate
```

```
ok  Config syntax valid (v2 format)
ok  adapter.fivetran: fivetran
!!  adapter.prod: no auth configured (token or client_id/secret)
ok  pipeline.bronze: schema pattern parseable
ok  pipeline.bronze: replication / incremental -> warehouse / stage__{source}

Validation complete.
```

### Related Commands

- [`rocky plan`](#rocky-plan) -- preview SQL without execution
- [`rocky run`](#rocky-run) -- execute the full pipeline

---

## `rocky discover`

List the connectors and tables the configured source exposes. Discover reads metadata only. It reports which schemas and tables exist. It moves no data.

```bash
rocky discover [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--pipeline <NAME>` | `string` | | Pipeline name (required if multiple pipelines are defined). |
| `--with-schemas` | `bool` | `false` | Warm the schema cache for every discovered source. Rocky issues one `batch_describe_schema` call per `(catalog, schema)` pair and stores the columns in the state store. Later `rocky compile` and `rocky lsp` runs read those entries instead of typing leaf models as `Unknown`. A source that errors is logged and skipped, so one bad source does not abort the warm-up. |
| `--no-cache` | `bool` | `false` | Takes effect only together with `--emit-fivetran-state-to`. On its own the flag changes nothing about discovery. It makes Rocky fetch the Fivetran state envelope straight from the API and skip the read caches configured under `[adapter.<name>.cache]`. A successful fetch still writes back to that cache. Use it when you suspect that cache is stale, for example after rotating a Fivetran credential. It does not touch the schema cache or the state store, and an open circuit breaker still short-circuits the fetch. |
| `--emit-fivetran-state-to <PATH>` | `PathBuf` | | Write a canonical Fivetran state envelope for every Fivetran adapter in the config. See [Emitting the Fivetran state envelope](#emitting-the-fivetran-state-envelope). |

### Examples

Discover all sources with JSON output:

```bash
rocky discover
```

```json
{
  "version": "1.6.0",
  "command": "discover",
  "sources": [
    {
      "id": "connector_abc123",
      "components": { "tenant": "acme", "regions": ["us_west"], "source": "shopify" },
      "source_type": "fivetran",
      "last_sync_at": "2026-03-30T10:00:00Z",
      "tables": [
        { "name": "orders", "row_count": null },
        { "name": "customers", "row_count": null }
      ]
    },
    {
      "id": "connector_def456",
      "components": { "tenant": "acme", "regions": ["eu_central"], "source": "stripe" },
      "source_type": "fivetran",
      "last_sync_at": "2026-03-29T22:15:00Z",
      "tables": [
        { "name": "charges", "row_count": null },
        { "name": "refunds", "row_count": null }
      ]
    }
  ]
}
```

Discover with table output:

```bash
rocky -o table discover
```

```
connector_id      | components                          | tables
------------------+-------------------------------------+-------
connector_abc123  | acme / us_west / shopify            | 12
connector_def456  | acme / eu_central / stripe          | 8
```

Discover a specific pipeline when multiple are defined:

```bash
rocky discover --pipeline shopify_us
```

### Emitting the Fivetran state envelope

`--emit-fivetran-state-to <PATH>` writes one state envelope per Fivetran adapter declared in `rocky.toml`. The file layout depends on how many Fivetran adapters the config declares.

```
one Fivetran adapter          two or more Fivetran adapters
──────────────────────        ─────────────────────────────────────────────
<PATH>                        <STEM>.<account_hash>.<destination_id>.json
<PATH>.blake3                 <STEM>.<account_hash>.<destination_id>.json.blake3
   │                                        │            │
   │ content hash                           │            │ Fivetran
   │ of the envelope                        │            │ destination id
   └─ rewritten only when                   └─ short token derived from the
      the hash changes                         account, so two adapters that
                                               share a destination id name do
                                               not race on the same file
```

`<STEM>` is `<PATH>` with a trailing `.json` removed. The per-destination segments land before the extension. So `--emit-fivetran-state-to state.json` writes `state.<account_hash>.<destination_id>.json`, never `state.json.<account_hash>.<destination_id>.json`.

The write is idempotent. The sibling `.blake3` file records the envelope's content hash. If the freshly computed hash matches the value on disk, Rocky leaves the JSON file alone. A `stat(2)` watcher therefore only fires when the upstream Fivetran state actually changed.

A connector whose `connectors/{id}/schemas` endpoint returns 404 is left out of the envelope's `schemas` map and logged at WARN. It still appears under `connectors` with its status fields. Discover exits non-zero only when every connector returns 404, so the envelope's connector count does not always match the Fivetran UI total.

### New sources and cross-source collisions

Two opt-in discover-time signals help catch onboarding problems before any catalog is created. Both are configured under [`[pipeline.NAME.source.discovery]`](/reference/configuration/#pipelinenamesourcediscovery) and appear as extra fields on the JSON output (omitted entirely when not enabled).

- **`new_sources`** — set `report_new_sources = true` to diff the discovered inventory against the prior persisted snapshot. First-seen source schemas are listed here; the first discover of a pipeline records the baseline and reports nothing.
- **`collision_candidates`** — set `on_collision = "warn"` (or `"error"`) to flag the same external object onboarded under more than one schema. Each entry pairs the shared `external_object_id` with the `sources` (schemas) it resolves to. With `"error"`, discover also exits non-zero. Only adapters that resolve external object ids (e.g. Fivetran) populate this.

```json
{
  "command": "discover",
  "sources": [ /* … */ ],
  "new_sources": ["src__acme__ca_central__shopify"],
  "collision_candidates": [
    {
      "external_object_id": "act_1234567890",
      "sources": ["src__acme__us_west__shopify", "src__acme__eu_central__shopify"]
    }
  ]
}
```

`collision_candidates` is the **preventive** half of cross-source duplicate detection; its **detective** counterpart, [`cross_source_overlap`](/concepts/data-quality-checks/#cross-source-overlap), runs at `rocky run` time against the materialized tables.

### Related Commands

- [`rocky plan`](#rocky-plan) -- generate SQL from discovered sources
- [`rocky run`](#rocky-run) -- discover and execute in one step

---

## `rocky plan`

Generate the replication SQL Rocky would run, without running it. The command needs a replication pipeline; it refuses a transformation-only project. It connects to the source to discover tables, then prints the copy SQL for each one. It does not print a transformation model's SQL. Add `--model <name>` for that, or use [`rocky emit-sql`](/reference/commands/modeling/#rocky-emit-sql). Rocky writes the plan to `.rocky/plans/<plan-id>.json` and prints the `plan_id`. For a `run` plan that file holds the settings the run needs, not the SQL. A reviewer reads the printed SQL. Then [`rocky apply <plan-id>`](#rocky-apply) recompiles the project and executes it with those settings.

`rocky plan` plus `rocky apply` is the canonical path for production and for gating a pull request. Nothing touches the warehouse between the two steps. For local iteration, [`rocky run`](#rocky-run) does the same work in one command and writes no plan file.

With `--model`, a compile error in that model or one reached through `depends_on` or a bare-name read refuses the plan. Without `--model`, Rocky compiles the whole models directory. An error in a model from another pipeline also refuses the plan. Rocky prints each relevant model and diagnostic code, exits with code `1`, and writes no plan file or `plan_id`. This applies to text and JSON output. Warnings alone do not stop the plan.

```bash
rocky plan [flags]
rocky plan promote <branch> [flags]
```

### The plan, review, apply lifecycle

```
   models/ + rocky.toml
          │
          ▼
  ┌──────────────┐  writes the plan   ┌────────────────────────────┐
  │  rocky plan  │───────────────────►│ .rocky/plans/<plan-id>.json│
  └───────┬──────┘  prints plan_id    │ run plan: settings, not SQL│
          │                           └─────────────┬──────────────┘
   prints replication SQL                           │
          │                                         │
          └──► a human reads it ────────────────────┤
                                                    ▼
                                      ┌───────────────────────────┐
                    approval gate ───►│ rocky review <plan-id>    │
                    (policy only)     │               --approve   │
                                      └─────────────┬─────────────┘
                                                    │ review marker
                                                    ▼
                                      ┌───────────────────────────┐
                                      │ rocky apply <plan-id>     │
                                      └─────────────┬─────────────┘
                                                    ▼
                                               warehouse
```

On this path the gate is optional. `rocky plan` writes a `run` or a `replication` plan, and `rocky plan promote` writes a `promote` plan. None of those three is gated by its kind. A `[policy]` rule that resolves to `require_review` turns the gate on. Without a `[policy]` block, these plans go straight from `rocky plan` to `rocky apply`.

Four plan kinds are always gated, whatever the policy: `ai_authored`, `backfill`, `gc`, and `restore`. Other commands write them: the MCP `propose` tool, `rocky backfill`, `rocky gc --derivable`, and `rocky restore`. See [`rocky review`](/reference/commands/governance-reclamation/#rocky-review).

### Flags

Every flag below applies to the default `rocky plan` form, not to `rocky plan promote`. The set overlaps [`rocky run`](#rocky-run) without matching it. `rocky plan` adds `--semantic`, `--intent` and `--base`, which `rocky run` does not have. `rocky run` has several flags that `rocky plan` does not, including `--watch`, a re-run loop with no plan to persist. `--parallel` also defaults to `1` here, against `4` for a `rocky run` without `--dag` and no default at all for one with it.

Rocky records the execution flags in the plan file, so `rocky apply` replays the same intent. The recorded set is:

- selection: `--filter`, `--pipeline`, `--model`, `--models`, `--all`, `--env`
- partitions: `--partition`, `--from`, `--to`, `--latest`, `--missing`, `--lookback`, `--parallel`
- routing: `--shadow`, `--shadow-suffix`, `--shadow-schema`, `--branch`, `--dag`
- other: `--governance-override`, `--resume`, `--resume-latest`, `--idempotency-key`

`--semantic` and `--base` are not recorded. They only add the `breaking_verdict` field to the `rocky plan` output, so `rocky apply` never repeats that classification.

`--intent` is recorded as the `intent` key of the plan. The verdict that checks it is not recorded. It goes only into the `intent_check` field of the `rocky plan` output. A plan without `--intent` has no `intent` key, so its `plan_id` does not change.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--filter <key=value>` | `string` | | Filter sources by component value (e.g., `client=acme`). |
| `--pipeline <NAME>` | `string` | | Pipeline name (required if multiple pipelines are defined). |
| `--model <NAME>` | `string` | | Plan a single compiled model by name and skip replication. An alternative to `--filter` for model-only execution. |
| `--select <SELECTOR>...`, `-s` / `--exclude <SELECTOR>...` / `--state-ref <REF>` | `string` | | [Node selection](/reference/node-selection/). The selection must resolve to exactly one model, which is then planned like `--model`. |
| `--models <PATH>` | `PathBuf` | | Models directory for transformation execution. |
| `--all` | `bool` | `false` | Plan both replication and compiled models. |
| `--governance-override <JSON>` | `string` | | Additional governance config as inline JSON or `@file.json`, merged with the defaults. Resolved at plan time and stored in the plan. |
| `--resume <RUN_ID>` | `string` | | Resume a specific previous replication run from its last checkpoint. Mints a new `run_id` and records the prior one as `resumed_from`. Rejected with `--dag`, which does not replay the resume into its sub-runs (rejected at parse time). |
| `--resume-latest` | `bool` | `false` | Resume the most recent failed replication run from its last checkpoint. Which run that is gets resolved at apply time, not plan time. Rejected with `--dag`, which does not replay the resume into its sub-runs (rejected at parse time). |
| `--shadow` | `bool` | `false` | Write to shadow targets instead of production. |
| `--shadow-suffix <SUFFIX>` | `string` | `_rocky_shadow` | Requires `--shadow`. Appends a suffix to table names. Conflicts with `--branch`. `--shadow` alone uses `_rocky_shadow`. |
| `--shadow-schema <NAME>` | `string` | | Requires `--shadow`. Overrides the schema for shadow tables. Conflicts with `--branch`. |
| `--branch <NAME>` | `string` | | Plan against a branch created with `rocky branch create`. Equivalent to `--shadow --shadow-schema <branch.schema_prefix>`. Conflicts with `--shadow`, `--shadow-schema`, and `--shadow-suffix`. |
| `--partition <KEY>` | `string` | | Plan one partition by its canonical key (`2026-04-07` for daily, `2026-04` for monthly). Errors if the format does not match the model's granularity. Mutually exclusive with `--from`, `--to`, `--latest`, `--missing`. |
| `--from <KEY>` | `string` | | Lower bound of a closed partition range, inclusive. Requires `--to`. Both bounds must align to the model's grain. |
| `--to <KEY>` | `string` | | Upper bound of a closed partition range, inclusive. Requires `--from`. |
| `--latest` | `bool` | `false` | Plan the partition containing now (UTC). The default for a `time_interval` model when no other selection flag is given. |
| `--missing` | `bool` | `false` | Plan the partitions missing from the state store, computed from the model's `first_partition` up to now. Errors if `first_partition` is unset. Resolved against the state store at apply time. |
| `--lookback <N>` | `integer` | | Also recompute the previous N partitions. The flag overrides the model's TOML `lookback`. This is the standard handling for late-arriving data. |
| `--parallel <N>` | `integer` | `1` | Run N partitions at a time. Warehouse-query parallelism only: state writes serialize through the state store. |
| `--dag` | `bool` | `false` | Plan all pipelines as one DAG in dependency order. Each pipeline is a node, cross-pipeline `depends_on` edges set the order, and layers run in parallel. Mutually exclusive with `--resume` / `--resume-latest`: the DAG runner does not consume them, so a plan carrying both would apply as a fresh run of every pipeline. With `--principal agent`, the plan's fingerprint and review cover every pipeline's models. |
| `--idempotency-key <KEY>` | `string` | `$ROCKY_IDEMPOTENCY_KEY` | Opaque caller-supplied key that dedups this run against prior runs with the same key. Supported on the `local`, `valkey`, and `tiered` state backends; an `s3`-only or `gcs`-only backend errors when the flag is parsed. Keys are stored verbatim, so never put a secret in one. |
| `--env <NAME>` | `string` | | Scope the governance preview (`mask_actions`) to one environment, so `[mask.<env>]` overrides overlay the workspace `[mask]` defaults. Classification tagging and retention policies are the same in every environment and are previewed regardless. |
| `--semantic` | `bool` | `false` | Also run the breaking-change classifier against `--base` and attach the change-impact verdict under `breaking_verdict`. Decision-support only — never gates the plan and never changes the exit code. |
| `--intent <INTENT>` | `string` | | **Experimental.** State what the change is meant to do, and check it on the data. The only value is `refactor`. See [Check a refactor with `--intent`](#check-a-refactor-with---intent). |
| `--base <ref>` | `string` | `main` | Git ref the working tree is compared against. `--semantic` and `--intent` use it. The change classification that every run plan carries uses it too. |

> The `--semantic` verdict diffs **output schema** only and is **blind to schema-stable value changes** (a `WHERE` / `JOIN`-key / `CASE` rewrite that changes values but not the schema). An empty `findings` list is not a safety signal: the verdict's `caveat` field states this verbatim. See the [CI/CD guide](/guides/ci-cd/#semantic-breaking-change-findings-and-the-promote-gate) for the full flow and the [`plan` schema](https://github.com/rocky-data/rocky/blob/main/schemas/plan.schema.json) for the `SemanticPlanVerdict` shape.

### Check a refactor with `--intent`

**Experimental.** `--intent refactor` says that a change keeps the output of every changed model. Rocky checks that claim on the data and reports one verdict per model. A changed model is one whose compiled form differs between `--base` and the working tree.

```
  --base ref ──► base SQL ──┐      one DuckDB transaction per model
                            ├──►  build base 6 times, head 2 times
  working tree ► head SQL ──┘     compare schema, then rows
                                  roll back: nothing is kept
```

The check passes when the schema is the same and the two outputs hold the same rows. The schema is the column names, types and order. Rows are compared as multisets (a duplicate row counts). Equality is exact, with no tolerance for floats.

| Verdict | Reason | Meaning |
|---|---|---|
| `match` | | The same schema and the same rows on the current data. |
| `mismatch` | `schema_differs` | A column name, type or position changed. Both schemas are in the output. |
| `mismatch` | `rows_differ` | The rows differ. The output gives the row counts and the rows only on each side. |
| `mismatch` | `model_removed` | The model exists at `--base` and not in the working tree. A renamed model shows its old name here and its new name as `no_base`. |
| `unverified` | `nondeterministic` | The SQL reads the clock, a random value, a UUID, a sequence, the session or the environment. Or it has a `LIMIT` with no `ORDER BY`. Or two builds of the same SQL differ. |
| `unverified` | `no_base` | The model is new. |
| `unverified` | `base_unavailable` | Rocky could not read or compile the models at `--base`. |
| `unverified` | `adapter_unsupported` | The target is not DuckDB. Rocky runs no query. |
| `unverified` | `unsupported_strategy` | The model is `incremental`, `time_interval`, `microbatch` or `ephemeral`. |
| `unverified` | `build_failed` | A build or a compare query failed, or the SQL holds more than one statement. `detail` has the reason. |

Know these limits before you trust a `match`:

- A `match` means no difference on the current data. It is not a proof for other data. A `WHERE` change that no current row reaches still gives `match`.
- Rocky checks the `SELECT` output only. It does not check masks, classifications, targets, the strategy or other model config. When the target or the strategy also changed, a `match` says so in `detail`.
- When both outputs are empty, a `match` says so in `detail`. The current data did not exercise the model.
- Each model reads the upstream tables that already exist in the warehouse. When an upstream model also changed, the verdict lists it in `upstream_changed`. Per-model matches do not add up to a match for the whole chain.
- The verdict is report-only. It relaxes no gate, it does not replace review, and it never changes the exit code. The flag itself can fail the command: with `--dag`, with no models directory, or when the working tree does not compile.
- `rocky plan` needs a replication pipeline in `rocky.toml`, so the check runs only in a project that has one. It pays for the extra builds on your DuckDB database.
- The plan records `intent: refactor` as the author's claim. The plan does not record the verdict, so a stored plan never shows the claim as checked.

The `--intent` flag is not the free-text `intent = "..."` key in a model sidecar. That key describes a model for the AI commands. `--intent` states what one change does.

```bash
rocky plan --intent refactor --base main --output json
```

### `rocky plan promote`

Plan a branch promotion. Rocky runs the approval gate and the breaking-change gate now, then stores a promote plan that `rocky apply <plan-id>` executes later. The gates are **not** re-run at apply time, which is what makes "plan in the pull request, apply on merge" work.

```bash
rocky plan promote <branch> [flags]
```

#### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `branch` | `string` | **(required)** | Branch name to promote. |

#### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--base <ref>` | `string` | `main` | Git ref the breaking-change gate diffs against. |
| `--allow-breaking` | `bool` | `false` | Bypass the breaking-change gate. Always records a `BreakingChangesAllowed` audit event in the plan, so the override leaves a paper trail. |
| `--filter <key=value>` | `string` | | Filter the promote targets. A transformation pipeline supports the keys `table`, `model`, `catalog`, and `schema`. |
| `--pipeline <NAME>` | `string` | | Pipeline to plan against. Required when `rocky.toml` defines more than one pipeline. |
| `--models <PATH>` | `PathBuf` | `models` | Models directory used by the breaking-change gate. |

On success the output is a `PlanOutput` with `plan_kind: "promote"` and the `plan_id` you pass to `rocky apply`.

### Examples

Plan all SQL for a specific tenant:

```bash
rocky plan --filter client=acme
```

```json
{
  "version": "1.6.0",
  "command": "plan",
  "filter": "client=acme",
  "statements": [
    {
      "purpose": "create_catalog",
      "target": "acme_warehouse",
      "sql": "CREATE CATALOG IF NOT EXISTS acme_warehouse"
    },
    {
      "purpose": "create_schema",
      "target": "acme_warehouse.staging__us_west__shopify",
      "sql": "CREATE SCHEMA IF NOT EXISTS acme_warehouse.staging__us_west__shopify"
    },
    {
      "purpose": "incremental_copy",
      "target": "acme_warehouse.staging__us_west__shopify.orders",
      "sql": "SELECT *, CAST(NULL AS STRING) AS _loaded_by FROM source_catalog.src__acme__us_west__shopify.orders WHERE _fivetran_synced > TIMESTAMP '1970-01-01 00:00:00'"
    }
  ]
}
```

A model excluded from the preview or refused by compilation is listed in `skipped`:

```json
  "skipped": [
    {
      "model": "stg_events",
      "reason": "invalid SQL generation request: model 'stg_events': `type = \"ephemeral\"` renders no statement of its own — it is inlined as a CTE into each model that reads it, so there is nothing to build (E038 when selected directly)"
    }
  ]
```

The `reason` gives the compiler diagnostic or SQL generation error. An `ephemeral` model is always there: its SQL appears inside each consumer's statement instead. A model that needs a live warehouse, such as a Snowflake dynamic table, is there too. The MCP `plan_preview` tool reports preview exclusions. The key is absent when nothing was skipped.

Plan with table output and a custom config:

```bash
rocky -c pipelines/prod.toml -o table plan --filter client=acme
```

```
purpose           | target                                             | sql (truncated)
------------------+----------------------------------------------------+--------------------------
create_catalog    | acme_warehouse                                     | CREATE CATALOG IF NOT...
create_schema     | acme_warehouse.staging__us_west__shopify            | CREATE SCHEMA IF NOT...
incremental_copy  | acme_warehouse.staging__us_west__shopify.orders     | SELECT *, CAST(NULL...
incremental_copy  | acme_warehouse.staging__us_west__shopify.customers  | SELECT *, CAST(NULL...
```

Plan for a specific pipeline:

```bash
rocky plan --filter client=acme --pipeline shopify_us
```

Plan one model, then execute the stored plan. `--filter` is optional, so a model-scoped plan needs no source filter:

```bash
rocky plan --model fct_revenue --models models/
rocky apply <plan-id>
```

### Related Commands

- [`rocky apply`](#rocky-apply) -- execute a stored plan
- [`rocky run`](#rocky-run) -- plan and execute in one step
- [`rocky emit-sql`](/reference/commands/modeling/#rocky-emit-sql) -- read a transformation model's SQL, which `rocky plan` does not print
- [`rocky validate`](#rocky-validate) -- check config before planning
- [`rocky discover`](#rocky-discover) -- see available sources
- [`rocky review`](/reference/commands/governance-reclamation/#rocky-review) -- sign off on a gated plan

---

## `rocky apply`

Execute a plan that `rocky plan`, `rocky compact`, `rocky archive`, `rocky backfill`, `rocky gc`, or `rocky restore` already generated. Rocky reads `.rocky/plans/<plan-id>.json` and dispatches on the plan's kind.

```bash
rocky apply <plan-id>
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `plan-id` | `string` | **(required)** | Plan identifier: the 64-character blake3 hex string the planning command printed. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--expect-spec-digest <DIGEST>` | `string` | *(none)* | Refuse unless the plan payload's `spec_digest` equals this value. The compare is an exact string compare; Rocky never parses the spec itself. |

The check is fail-closed in both directions, and it runs before any policy gate. A plan that carries a product identity (`product_id` + `spec_digest` in its payload) refuses a bare `rocky apply` — the flag is required for it. The flag against a plan with no `spec_digest` refuses too. A mismatch refuses and names both digests. A plan without product fields, applied without the flag, behaves exactly as before.

### What each plan kind executes

The plan file carries its kind. Apply reads that kind and takes one path.

| `plan_kind` | Generated by | What apply does |
|---|---|---|
| `run` | `rocky plan` | Re-executes the full pipeline with the flags the plan captured. |
| `replication` | `rocky plan` on a project with no compiled models | Re-runs discovery and compares the result against the plan-time snapshot, then executes. See the staleness rules below. |
| `promote` | `rocky plan promote` | Runs the promote statements. The approval and breaking-change gates ran at plan time and are not repeated. |
| `compact` | `rocky compact` | Runs the `OPTIMIZE` and `VACUUM` statements. |
| `archive` | `rocky archive` | Runs the `DELETE` and `VACUUM` statements. |
| `ai_authored` | an AI agent, via the `propose` MCP tool | Same execution path as a `run` plan, but only after a review marker exists. |
| `backfill` | `rocky backfill` | Rebuilds the scoped model set. Always needs a review marker. |
| `gc` | `rocky gc --derivable` | Evicts each artifact. Always needs a review marker. |
| `restore` | `rocky restore` | Rebuilds each evicted artifact. Always needs a review marker. |

### What apply checks for staleness

Staleness checking is per plan kind. The three kinds below differ in what they compare, and in whether a difference stops the apply.

**`replication`.** Apply re-runs discovery and compares the result against the snapshot taken at plan time. A difference aborts an unfiltered apply and tells you to re-plan. When the plan carries a `--filter`, a difference outside the filter scope logs a warning and the apply continues. A difference inside the filter scope still aborts.

**`run`.** Apply reloads `rocky.toml` and recompiles the current models, then executes those. It does not replay SQL stored in the plan. So it first checks the plan's models. A plan records a fingerprint of the models it runs, and a second one of those models plus the config they run under. If a model was added, removed or edited after the plan was made, apply refuses with `plan_models_changed`: "models changed since this plan was made; plan again". The same check covers `ai_authored` and `backfill` plans. It runs after the policy and review gates and before any warehouse statement.

- A person's apply compares the models only. A plan made in one shell and applied from another environment (for example a `rocky serve` job), or after an edit to another pipeline, still applies. It does not re-check that config (`[mask]`, `[run]`, `[hook]`, governance), so a config edit made after approval applies too. A `${VAR}` inside a model's SQL or sidecar is part of the models: a different value refuses with `plan_models_changed`. The models can still change between this check and the run's own compile.
- An agent's apply (`--principal agent`, or `ROCKY_PRINCIPAL=agent`) compares the models and the config. If only the config changed, it refuses with `plan_config_changed`. It also checks the routing identity, and repeats the fingerprint check when the models execute. `ai_authored` and `backfill` plans always apply as an agent.
- A plan whose models did not compile at plan time has no fingerprint and is not checked. Nor is a snapshot, load, quality or replication-only plan.
- A plan made before this check refuses with `plan_snapshot_missing`. Plan again.

A review-gated `--dag` plan (agent-authored, or AI-authored) is checked across the whole DAG, against the models and the config they run under. Its fingerprint and its `rocky review` approval cover every model the DAG runs: each transformation pipeline's models, from that pipeline's own directory and file glob. The fingerprint also covers the project's seeds (data, sidecar and seed hooks). Before the DAG runs, apply recomputes that fingerprint and refuses if any model or seed was added, removed or changed. An agent applying a `--dag` plan is still refused, because the DAG's sub-runs are not yet policy-gated.

**`promote`.** The plan stores the branch-state hash that `rocky plan promote` computed. Apply does not recompute it, and it does not re-run the approval or breaking-change gates. Those ran at plan time, which is what makes "plan in the pull request, apply on merge" work. The `[policy]` gate described below still runs at apply time.

`rocky apply` gates on the identity the command runs under. Set that identity with the global `--principal` flag, which takes `human` or `agent` and defaults to `human`. The `ROCKY_PRINCIPAL` environment variable sets it too, and may only raise it to `agent`. Rocky combines the runtime identity most restrictively with the plan kind's own default. The kinds `ai_authored`, `backfill`, and `gc` default to `agent`, so a policy rule scoped to agents applies to them even when nobody set a principal. That includes an interactive `rocky gc` run by a person. It never trusts the principal field stored inside the plan file. An agent running `rocky apply` is gated as an agent whatever that field says. Without a `[policy]` block in `rocky.toml`, the principal changes nothing.

### Examples

Plan, read the SQL, then apply:

```bash
rocky plan --filter client=acme
rocky apply <plan-id>
```

Apply an AI-authored plan. The bare apply is refused until an approval marker names that plan:

```bash
rocky review <plan-id> --approve
rocky apply <plan-id>
```

### Output

`rocky apply` writes no wrapping envelope. Each plan kind's apply path prints its own output. Read the top-level `command` field to tell them apart.

| `command` field | Plan kinds | Shape |
|---|---|---|
| `run` | `run`, `replication`, `ai_authored`, `backfill` | `RunOutput` |
| `compact apply` | `compact` | `CompactApplyOutput` |
| `archive apply` | `archive` | `ArchiveApplyOutput` |
| `branch promote` | `promote` | `BranchPromoteOutput` |
| `apply` (has `evicted`) | `gc` | `GcApplyOutput` |
| `apply` (has `restored`) | `restore` | `RestoreApplyOutput` |

The `gc` and `restore` outputs share the `command` value `apply`. Tell them apart by which marker field is present.

### Related Commands

- [`rocky plan`](#rocky-plan) -- generate a run plan
- [`rocky run`](#rocky-run) -- plan and apply in one step
- [`rocky review`](/reference/commands/governance-reclamation/#rocky-review) -- sign off on a gated plan
- [`rocky history`](/reference/commands/administration/#rocky-history) -- see what an apply recorded

---

## `rocky run`

> `rocky run` does in one step what [`rocky plan`](#rocky-plan) plus [`rocky apply`](#rocky-apply) do in two. Use it for local iteration and automation. Use the two-step form for production and for gating a pull request, where someone needs to read the SQL first.

Run the whole pipeline end to end: discover the sources, detect schema drift, create catalogs and schemas, copy the data, apply governance, and run the quality checks.

```bash
rocky run [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--filter <key=value>` | `string` | | Filter sources by component value (e.g., `client=acme`). |
| `--pipeline <NAME>` | `string` | | Pipeline name (required if multiple pipelines are defined). |
| `--model <NAME>` | `string` | | Execute a single compiled model by name and skip replication. An alternative to `--filter` for model-only execution. |
| `--select <SELECTOR>...`, `-s` / `--exclude <SELECTOR>...` / `--state-ref <REF>` | `string` | | Build the [selected models](/reference/node-selection/) and skip replication. Unselected upstreams are read as they exist. Not with `--dag`, `--watch`, `--all`, `--filter`, `--contracts`, or `--resume`. |
| `--contracts <PATH>` | `PathBuf` | | Check the selected model against its contract in this directory during the run's own compile. Requires `--model` and `--pipeline`. |
| `--governance-override <JSON>` | `string` | | Additional governance config as inline JSON or `@file.json`, merged with defaults. |
| `--models <PATH>` | `PathBuf` | | Models directory for transformation execution. |
| `--all` | `bool` | `false` | Execute both replication and compiled models. |
| `--resume <RUN_ID>` | `string` | | Resume a specific previous replication run from its last checkpoint; mints a new `run_id` and records the prior one as `resumed_from`. Rejected with `--dag` or `--shadow`. |
| `--resume-latest` | `bool` | `false` | Resume the most recent failed replication run from its last checkpoint; mints a new `run_id` and records the prior one as `resumed_from`. Rejected with `--dag` or `--shadow`. |
| `--shadow` | `bool` | `false` | Write to shadow targets, compare them with production, then drop them after any completed verdict. A failed threshold exits non-zero. |
| `--keep-shadow` | `bool` | `false` | Requires `--shadow` and conflicts with `--watch`. Keep shadow objects for a separate `rocky compare`. |
| `--shadow-suffix <SUFFIX>` | `string` | `_rocky_shadow` | Requires `--shadow`. Appends a suffix to table names. Conflicts with `--branch`. `--shadow` alone uses `_rocky_shadow`. |
| `--shadow-schema <NAME>` | `string` | | Requires `--shadow`. Overrides the schema for shadow tables. Conflicts with `--branch`. |
| `--branch <NAME>` | `string` | | Execute against a named branch previously registered with `rocky branch create`. Applies the branch's `schema_prefix` to every target (internally equivalent to `--shadow --shadow-schema <branch.schema_prefix>`). Conflicts with `--shadow`, `--shadow-schema`, and `--shadow-suffix`. The run records `<NAME>` as `rocky_branch` in [run history](/reference/commands/administration/#rocky-history). |

| `--watch` | `bool` | `false` | Wrap the run in a filesystem watcher: re-execute the pipeline on every change to `rocky.toml` or any file under `models/`, debounced to 200 ms so editor save bursts coalesce into a single re-run. Failed runs do not exit the loop; Ctrl-C exits cleanly between runs. **v0 limitations:** mutually exclusive with `--dag`, `--resume`, `--resume-latest`, `--idempotency-key`, and `--model` (rejected at parse time). |
| `--defer` | `bool` | `false` | Build only the `--model`-selected models locally, resolving unbuilt upstream models to an existing (production) schema — the dbt-Core-style defer convenience. Takes effect **only together with `--model`**: a full run builds everything, so the flag is inert. Applies to transformation models; mutually exclusive with `--dag`. See the limitation note below. |
| `--defer-to <SCHEMA>` | `string` | | Schema the deferred upstream models resolve to. Requires `--defer`. Defaults to each unbuilt upstream's own configured target schema (its production home); pass this to point every deferred reference at a single schema instead (catalog + table are preserved). |
| `--skip-unchanged` | `bool` | `false` | Turn on the model-skip gate for this invocation regardless of the `[run] skip_unchanged` config: skip re-materializing a transformation model whose logic and every upstream's data both appear unchanged. **Best-effort optimization, not a result-equivalence guarantee** — non-deterministic SQL and models without provably-complete lineage (CTEs, subqueries, `PIVOT`/`UNNEST`, set operations) always rebuild. See [`[run]`](/reference/configuration/#run) for the full eligibility rules. |
| `--force-rebuild` | `bool` | `false` | Force every selected model to build, bypassing the `--skip-unchanged` gate entirely. The escape hatch for a guaranteed rebuild after a non-logic change the IR hash can't see (a UDF redefinition, a session-setting change). |
| `--full-refresh` | `bool` | `false` | Rebuild transformation `incremental` models with `CREATE OR REPLACE TABLE ... AS`. Every `@incremental_filter` becomes `TRUE`, so the table holds the model's full result. Other strategies are unaffected: a `merge` or `delete_insert` model's SQL often selects only recent rows, and rebuilding from it would drop history. The flag also turns off the `--skip-unchanged` gate. |
| `--refuse-hooks` | `bool` | `false` | Refuse the run when the config defines any `[hook]` command or `[hook.webhooks]` entry that would fire, before anything fires or any model builds. For CI jobs that run a config they do not trust, such as a pull request's `rocky.toml`. Not supported with `--watch`. |
| `--var <name=value>` | `string` (repeatable) | | Bind a per-run variable substituted into model SQL wherever an `@var(name)` / `@var(name, default)` marker appears. Repeat for multiple variables. Distinct from config-time `${ENV}` substitution: `@var()` resolves the run's logical inputs at compile time, `${ENV}` resolves connection/config values while parsing `rocky.toml`. A model that references `@var(name)` with no `--var` binding and no inline default fails to compile, naming the missing variable. See [`@var()` run variables](/reference/model-format/#var-run-variables). |
| `--parallel <N>` | `integer` | `4` without `--dag` | Models in a topological layer (and partitions of a `time_interval` model) run up to N at a time. Pass `--parallel 1` to run one model or partition at a time. Under `--dag` the flag bounds how many pipeline **nodes** run at once, and it has no default there: left unset, a `--dag` run keeps its unbounded node fan-out. It does **not** bound a replication pipeline's table fan-out, which comes from that pipeline's `[execution] concurrency` (default 32), so `--parallel 1` alone does not make a replication run serial. DuckDB always runs serially regardless of this flag (its adapter holds a single connection mutex); Snowflake and Databricks parallelize up to N. |

:::caution[`--defer` SQL-rewrite limitation]
`--defer` rewrites each selected model's SQL to qualify deferred upstream references, and the rewrite parses the model with the Databricks dialect. Constructs the parser does not support (`SELECT * EXCEPT (...)`, trailing-comma select lists, and `STRUCT(...)` literals) cannot be rewritten and fail with a clear error. Build those models without `--defer`. With `--defer` off (the default), runs are byte-identical to before the flag existed.
:::

### Recovering replication checkpoints

`--resume` and `--resume-latest` refuse a checkpoint whose every planned table
copied but whose terminal run record is missing. Resuming that checkpoint would
skip its post-copy checks. Incomplete crash checkpoints remain resumable.

Follow the refusal's recovery route. A matching fresh run copies targets from
a complete checkpoint without a run record, even with unchanged source markers.
It runs checks and writes a new run record. Rocky then marks the old checkpoint
superseded, so the next run can prune unchanged targets. A recorded check
failure also supersedes it.

Matching requires the same target endpoint and a plan containing every target
in the checkpoint. An old checkpoint alone never fails a fresh run. With
supported recovery records, Rocky re-derives target watermarks before copying.
#2235 stays open for remaining recovery gaps.

For checkpoints from Rocky 1.75.0 or earlier, set `strategy = "full_refresh"`
for affected tables. Run `rocky run --pipeline <name> --no-prune` without a
resume flag. That replaces their data without duplicate appends. Keep full
refresh until `rocky state reconcile-watermark --pipeline <name>` sets the
cursor from the replacement target's maximum timestamp. Use `--dry-run` to
preview it and repeat `--table catalog.schema.table` to select targets. A crash
under Rocky 1.75.0 or earlier may leave a stale cursor after an INSERT. The
first run after upgrading may append those rows again. Repair the cursor before
that run if the old flush is uncertain. See
[Interrupted replication](/concepts/incremental/#recovering-an-interrupted-replication)
for recovery routes, supported adapters and remote durability limits.

### Guard one model with a contract

Use `--contracts` when you build one `full_refresh` transformation model:

```bash
rocky run --pipeline transform --model int_order_lines --contracts contracts -o json
```

The directory must exist and contain `int_order_lines.contract.toml`. Rocky checks that contract in the compile that supplies the model SQL for this run. An error such as `E010` fails the run before Rocky replaces the selected table. The failure JSON includes the compile error and has no materialization for that model.

This first route does not cover a whole pipeline, `--dag`, `--all`, `rocky apply`, a branch, or a deferred run. It refuses selected models that use a strategy other than `full_refresh` or add a `surrogate_key` after compilation. It also refuses idempotency and skip options that could report success without rebuilding. A failed contract can still update run history or state synchronization. `I003` means Rocky could not infer a declared column type and did not check that type; required-column checks still run.

### Pipeline Stages

```
  ┌──────────────┐ ── enumerate the sources and their tables
  │   discover   │
  └──────┬───────┘
         ▼
  ┌──────────────┐ ── one catalog or schema at a time: create
  │  governance  │    catalogs, apply tags, bind workspaces,
  │    setup     │    grant permissions, create schemas
  └──────┬───────┘
         ▼
  ┌──────────────┐ ── up to `execution.concurrency` tables at
  │    tables    │    once: drift detection, incremental copy,
  │  (parallel)  │    tag application, watermark update
  └──────┬───────┘
         ▼
  ┌──────────────┐ ── row count, column match, and freshness,
  │    checks    │    batched into one pass
  └──────┬───────┘
         ▼
  ┌──────────────┐ ── failed tables only, one at a time, up to
  │    retry     │    `execution.table_retries`
  └──────────────┘
```

### Examples

Run the pipeline for a specific tenant:

```bash
rocky run --filter client=acme
```

```json
{
  "version": "1.6.0",
  "command": "run",
  "filter": "client=acme",
  "duration_ms": 45200,
  "tables_copied": 20,
  "tables_failed": 0,
  "materializations": [
    {
      "asset_key": ["fivetran", "acme", "us_west", "shopify", "orders"],
      "rows_copied": null,
      "duration_ms": 2300,
      "metadata": {
        "strategy": "incremental",
        "watermark": "2026-03-30T10:00:00Z",
        "target_table_full_name": "acme_warehouse.staging__us_west__shopify.orders"
      }
    }
  ],
  "check_results": [],
  "errors": [],
  "excluded_tables": [],
  "permissions": { "grants_added": 3, "grants_revoked": 0, "catalogs_created": 0, "schemas_created": 1 },
  "drift": { "tables_checked": 20, "tables_drifted": 1, "actions_taken": [] },
  "anomalies": [],
  "partition_summaries": []
}
```

Run with a governance override file:

```bash
rocky run --filter client=acme --governance-override @overrides/acme.json
```

Run both replication and model transformations:

```bash
rocky run --filter client=acme --models models/ --all
```

Resume the most recent failed replication run from its last checkpoint:

```bash
rocky run --filter client=acme --resume-latest
```

Run in [shadow mode](/reference/glossary/). Rocky compares each shadow target with production before it drops the shadow object:

```bash
rocky run --filter client=acme --shadow
```

Keep the shadow objects when you need a separate comparison:

```bash
rocky run --filter client=acme --shadow --keep-shadow
rocky compare --filter client=acme
```

`--keep-shadow` retains objects after a failed verdict too. A write or comparison query error may leave an object whose state Rocky cannot confirm.

### When a shadow or branch run is refused

Shadow mode is only useful if it truly isolates the run from production. Rocky refuses the run rather than write a target it cannot isolate. A shadow or branch run fails closed in any of these cases.

- The selected transformation set contains a `content_addressed` or `time_interval` model. Both need extra storage or partition-state isolation that shadow mode does not give them.
- The chosen suffix or schema would collide with a production target, or with another selected shadow target.
- All three of the following hold at once:
  - the dialect treats identifier case as part of object identity (Snowflake and BigQuery);
  - the run routes more than one model;
  - a routed target differs from any other model's target only by case.

That last case deserves a word. To such a warehouse the two targets are distinct objects. Rocky matches upstream references case-insensitively, so a read of either could land on the wrong one. The refusal does not depend on whether a model spells such a read today. Rename one of the targets, or scope the run so it routes only one of them.

This rule is not about whether the dialect quotes identifiers. Rocky renders Trino targets double-quoted, yet treats two Trino targets that differ only by case as one object, so such a run proceeds there.

`--shadow` and `--branch` isolate `rocky run` for transformation pipelines, and for replication pipelines in either mode. Rocky refuses both flags on `rocky run --dag`, and on quality, snapshot and load pipelines. A refused quality, snapshot or load run stops before it opens a warehouse adapter, writes to the state store, or claims an idempotency key, so it writes nothing. See [Shadow mode](/concepts/shadow-mode/) for the reasons and for how to quality-check a branch.

Or run against a named branch:

```bash
rocky branch create fix_price --description "testing reprice migration"
rocky run --filter client=acme --branch fix_price
```

Run in watch mode for the inner-loop developer workflow, where every save re-materializes the pipeline against the local DuckDB warehouse:

```bash
rocky run --watch
```

`--watch` watches the parent directory of `rocky.toml` (filtered to `rocky.toml` itself) plus the resolved `models/` directory recursively. The directory watch is FSEvents-safe on macOS: atomic-rename saves (vim's `:w`, VSCode's default) trigger correctly where a file-level watch can miss the new inode. Banner / "detected change" lines go to `stderr` so `stdout` stays parseable; with `--output json`, each iteration emits one `RunOutput` JSON object on `stdout` (newline-delimited).

### Related Commands

- [`rocky plan`](#rocky-plan) -- preview SQL before execution
- [`rocky state`](#rocky-state) -- inspect watermarks after a run
- [`rocky branch`](#rocky-branch) -- manage named branches
- [`rocky history`](/reference/commands/administration/#rocky-history) -- view past runs

---

## `rocky state`

Show the [watermarks](/reference/glossary/) stored in the embedded state file. A watermark is the newest source value Rocky has already loaded for a table, so the next run knows where to resume. The output lists every tracked table, its last watermark value, and the time Rocky recorded it.

```bash
rocky state [flags]
```

### Flags

No command-specific flags. Uses [global flags](#global-flags) only.

### Examples

Show watermarks with JSON output:

```bash
rocky state
```

```json
{
  "version": "1.6.0",
  "command": "state",
  "watermarks": [
    {
      "table": "acme_warehouse.staging__us_west__shopify.orders",
      "last_value": "2026-03-30T10:00:00Z",
      "updated_at": "2026-03-30T10:01:32Z"
    },
    {
      "table": "acme_warehouse.staging__us_west__shopify.customers",
      "last_value": "2026-03-30T09:55:00Z",
      "updated_at": "2026-03-30T10:01:32Z"
    }
  ]
}
```

Show watermarks with table output using a custom state path:

```bash
rocky -o table --state-path /var/rocky/state.redb state
```

```
table                                                | last_value                | updated_at
-----------------------------------------------------+---------------------------+---------------------------
acme_warehouse.staging__us_west__shopify.orders       | 2026-03-30T10:00:00Z      | 2026-03-30T10:01:32Z
acme_warehouse.staging__us_west__shopify.customers    | 2026-03-30T09:55:00Z      | 2026-03-30T10:01:32Z
acme_warehouse.staging__eu_central__stripe.charges    | 2026-03-29T22:15:00Z      | 2026-03-30T10:01:32Z
```

### Related Commands

- [`rocky run`](#rocky-run) -- update watermarks by executing the pipeline
- [`rocky history`](/reference/commands/administration/#rocky-history) -- view run history

---

## `rocky freshness`

Check how fresh each declared source and model is, against the warehouse. This is Rocky's form of `dbt source freshness`. It reads data only and writes nothing.

```bash
rocky freshness [flags]
```

Rocky checks two kinds of declaration:

- **Sources.** Each [`[[pipeline.NAME.sources]]`](/reference/configuration/#pipelinenamesources) entry with a `freshness` block. Rocky runs `SELECT COUNT(*), MAX(loaded_at_field) FROM <table> [WHERE (<filter>)]`.
- **Models.** Each model with a `[freshness]` block. With a `time_column`, Rocky reads `MAX(time_column)` from the model's target table. Without one, Rocky uses the model's last successful build from the state store.

The age is the check time minus the newest load time. Rocky grades it:

| Status | When |
|--------|------|
| `pass` | The age is within every threshold. |
| `warn` | The age is greater than `warn_after`. Also when the newest load time is more than 60 seconds in the future: the age cannot be trusted. |
| `error` | The age is greater than `error_after`. |
| `runtime_error` | Rocky could not measure: invalid config, a failed query, or a value that does not read as a timestamp. |

A table with no rows, or with only NULL load times, counts as never loaded. The worst threshold it declares applies. A model's `max_lag_seconds` is its warn threshold. Under `severity = "error"` it is the error threshold instead.

**Exit code.** `1` when any check is `error` or `runtime_error`. `0` otherwise, including when checks only `warn`.

`rocky run` does not call this command. To stop a run on stale sources, run `rocky freshness` first and stop on a non-zero exit.

A model can inherit `time_column` from `_defaults.toml` or the project `[freshness]` block. When Rocky cannot read an inherited column on that model, it measures the last successful build instead and says so in `message`. A `time_column` in the model's own sidecar gets no fallback: a failed read is `runtime_error`.

**Limits.**

- Run history records a model build under its bare target table name. Two models whose targets share a table name in different schemas share that history. For a model without a `time_column`, the newer build of the two can hide a stale one. Set a `time_column` to measure the table itself.
- With `[state] namespacing = "pipeline"`, the state-store fallback reads the global state file, so it may find no build.
- Exit `1` covers both a stale check and a command failure (for example a config that does not load). A command failure prints no JSON report.
- A load time with no time zone is read as UTC. A column filled in local time is off by the zone's offset. A SQL Server `DATETIME` filled with `GETDATE()` is the common case. Fill it with `SYSUTCDATETIME()`, or use a zoned type such as `DATETIMEOFFSET`.
- Snowflake temporal cells arrive as epoch numbers and are decoded as such. That decoding follows the documented SQL API format and is not yet verified against a live account.

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--pipeline <NAME>` | `string` | | Check one transformation pipeline. By default Rocky checks every transformation pipeline. A replication pipeline is refused: it checks freshness with `[checks.freshness]` during `rocky run`. |

### Examples

```bash
rocky freshness
```

```json
{
  "version": "1.76.0",
  "command": "freshness",
  "checked_at": "2026-10-04T12:00:00Z",
  "sources": [
    {
      "name": "raw.orders",
      "pipeline": "silver",
      "table": "raw.orders",
      "loaded_at_field": "_loaded_at",
      "measured_from": "warehouse",
      "max_loaded_at": "2026-10-03T23:00:00Z",
      "age_seconds": 46800,
      "warn_after_seconds": 43200,
      "error_after_seconds": 86400,
      "status": "warn"
    }
  ],
  "models": [],
  "summary": { "pass": 0, "warn": 1, "error": 0, "runtime_error": 0 }
}
```

### Related Commands

- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- checks the declarations (`E050`, `W050`)
- [`rocky run`](#rocky-run) -- builds the models; does not check freshness

---

## `rocky branch`

Manage named virtual branches. A branch is the persistent, named form of shadow mode. Creating one records a `schema_prefix` in the state store. Every later run that names the branch applies that prefix to each model target. That holds whether you run `rocky plan --branch <name>` plus `rocky apply <plan-id>` or the one-step `rocky run --branch <name>`. Schema-prefix branches behave the same on every adapter today. Warehouse-native clones (Delta `SHALLOW CLONE`, Snowflake zero-copy `CLONE`) are a follow-up.

`rocky plan` accepts replication pipelines only. A transformation pipeline uses `rocky run --branch <name>`.

```bash
rocky branch create <name> [--description <text>]
rocky branch delete <name>
rocky branch list
rocky branch show <name>
rocky branch compare <name> [--filter <key=value>] [--pipeline <name>]
rocky branch approve <name> [--message <text>] [--out <path>]
rocky branch promote <name> [--allow-breaking] [--base-ref <ref>]
                            [--models <path>] [--skip-approval]
                            [--pipeline <name>] [--filter <key=value>]
rocky branch promote <name> --plan <plan-id> [--pipeline <name>]   # canonical: plan + apply
```

Branch names accept 1–64 `[A-Za-z0-9_]` characters. Rocky refuses other characters at the command entry point and suggests underscores. The default schema prefix is `branch__<name>`. Deleting a branch removes its state-store entry but leaves its warehouse tables.

**Target names have their own limit.** `branch promote` quotes each name in the strategy's `CREATE OR REPLACE TABLE` or `VIEW` statement. Quoting is not escaping. Promote refuses a name that contains the warehouse's identifier quote and names the character.

| Warehouse | Identifier quote | Also refused |
|---|---|---|
| DuckDB, Snowflake, Trino | `"` | |
| Databricks | `` ` `` | |
| BigQuery | `` ` `` | `\` — BigQuery reads escape sequences inside a quoted identifier, so a trailing backslash consumes the closing quote |

**No other character is refused by this quote check.** A hyphen or a dot in a target name passes it. Branch names follow the stricter rule above. A backslash passes it everywhere except BigQuery.

Two limits sit outside this check and still apply:

- A transformation model that takes its schema from its group's `schema_template` follows `[A-Za-z0-9_]` too. This applies with or without a placeholder. A hyphen or a dot is refused while Rocky builds the plan, before promote quotes anything.
- Promote also refuses a plan in which two steps replace the same production table, whatever the names look like.

The check runs when a promote plan is built and again when one is applied, because a plan stores its statement as ready-made text.

### `branch compare` flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--filter <key=value>` | `string` | (none) | Filter the compared targets. See [the shared filter reference](/reference/filters/). |
| `--pipeline <name>` | `string` | (none) | Which pipeline to compare, in a multi-pipeline project. Required when the project defines more than one pipeline. |

### `branch approve` flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--message <text>` | `string` | (none) | Optional free-form note persisted in the approval artifact. |
| `--out <path>` | `PathBuf` | `./.rocky/approvals/<branch>/<approval_id>.json` | Override the artifact destination path. |

Writes a content-addressed approval artifact that binds the approver's git identity to the exact bytes of the branch's models and project config. Editing, adding, or renaming any model after approval voids that approval, so `rocky branch promote` refuses to run unless the on-disk approvals still match the current state and satisfy the `[branch.approval]` policy.

> **Upgrade note (engine v1.43):** approvals created before v1.43 bound to the project config only, not the model bytes. They no longer satisfy the gate after upgrading. Run `rocky branch approve <name>` once to re-sign each branch against its current model contents.

### `branch promote` flags

> Note: as of engine v1.33, the canonical form is [`rocky plan promote <name>`](#rocky-plan-promote) followed by `rocky apply <plan-id>` (or `rocky branch promote <name> --plan <plan-id>`). The bare `rocky branch promote <name>` form still works as an alias. It prints a one-line `[deprecated]` notice to stderr, which `ROCKY_SUPPRESS_DEPRECATION=1` silences.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--allow-breaking` | flag | off | Bypass the semantic breaking-change gate. Always emits a `breaking_changes_allowed` audit event so the override leaves a paper trail. |
| `--base-ref <ref>` | `string` | `main` | Git ref to diff against for the breaking-change gate. |
| `--models <path>` | `PathBuf` | `models` | Models directory used by the breaking-change gate. |
| `--skip-approval` | flag | off | Bypass the approval gate. Always emits an `approval_skipped` audit event so the bypass leaves a paper trail. |
| `--pipeline <name>` | `string` | (none) | Which pipeline to promote, in a multi-pipeline project. Optional when the project defines a single pipeline, or when `--plan` names a promote plan that already recorded one — omit it to use the plan's pipeline. A value that disagrees with the plan's recorded pipeline is refused. |
| `--filter <key=value>` | `string` | (none) | Filter the promote targets. Replication pipelines filter sources by schema-pattern component (e.g. `--filter client=acme`); transformation pipelines filter models by `table`, `model`, `catalog`, or `schema`. |

`rocky branch promote` enumerates the pipeline's production targets. A replication pipeline finds the source connector's tables through the schema-pattern templates. A transformation pipeline walks the configured `models` glob. Rocky runs the optional `[branch.approval]` gate and the semantic breaking-change gate against `--base-ref`.

Rocky promotes `full_refresh` tables by copying their branch results. It creates production views from the model SQL with production upstreams. It refuses all other strategies because their promotion is undefined. Replication promotion supports `full_refresh` only. Quality and snapshot pipelines are unsupported.

Before the first replacement, Rocky checks every branch source and production destination. It verifies that each destination schema exists. It also checks the kind of each existing destination object. These checks work on DuckDB, Databricks, Snowflake, BigQuery, and Trino. If a later write fails, Rocky does not roll back earlier replacements. Text and JSON output identify replaced, failed, and unattempted targets. The command exits non-zero.

The breaking-change gate vetoes the promote and exits non-zero when any finding has `severity == "breaking"`, unless you pass `--allow-breaking`. Rocky records every gate decision in the audit trail: a block, an allow via override, and a fail-open when the gate could not run. To surface the same findings on every pull request without blocking, use [`rocky ci-diff --semantic`](/reference/commands/modeling/#rocky-ci-diff).

### Examples

Create, list, run against, and delete a branch:

```bash
rocky branch create fix_price --description "testing reprice migration"
```

```json
{
  "version": "1.11.0",
  "command": "branch create",
  "branch": {
    "name": "fix_price",
    "schema_prefix": "branch__fix_price",
    "created_by": "hugo",
    "created_at": "2026-04-20T14:22:11+00:00",
    "description": "testing reprice migration"
  }
}
```

```bash
rocky branch list
```

```json
{
  "version": "1.11.0",
  "command": "branch list",
  "total": 2,
  "branches": [
    { "name": "fix_price", "schema_prefix": "branch__fix_price", "created_by": "hugo", "created_at": "2026-04-20T14:22:11+00:00", "description": "testing reprice migration" },
    { "name": "ingest_v2", "schema_prefix": "branch__ingest_v2", "created_by": "ci",   "created_at": "2026-04-18T09:05:00+00:00", "description": null }
  ]
}
```

```bash
rocky run --filter client=acme --branch fix_price
rocky branch delete fix_price
```

Diff a branch's materialized tables against production (row counts + schemas):

```bash
rocky branch compare fix_price
rocky branch compare fix_price --pipeline shopify_us   # multi-pipeline project
```

Internally this is `rocky compare` pointed at the branch's `schema_prefix` via `ShadowConfig.schema_override`, the same mechanism `rocky run --branch` uses for writes, so compare always hits exactly the tables the branch produced. Accepts the shared [`--filter`](/reference/filters/) flag, and `--pipeline <name>` to select the pipeline in a multi-pipeline project.

When Rocky confirms that production has no target, it reports `verdict: "no_baseline"`. That target does not fail the run. An uncertain metadata read reports `verdict: "error"` and fails the command. Its unreadable row count is `null`, never `0`. See [`rocky compare`](/reference/cli/#rocky-compare).

### Related Commands

- [`rocky run`](#rocky-run) -- execute a pipeline against a branch via `rocky run --branch`
- [`rocky compare`](/reference/cli/#rocky-compare) -- diff an ad-hoc shadow against production (the generic form `rocky branch compare` specialises)
