---
title: Core Pipeline Commands
description: "Start a project, check it, plan the SQL, apply it, and read the state store back"
sidebar:
  order: 1
---

These commands cover a pipeline's life: create the project, check the config, see the source, plan the SQL, apply the plan, and read back what ran.

A plan command plus `rocky apply` is the production path. `rocky plan` prints the SQL and writes a plan file with an id. For a `run` plan the file holds the settings the run needs, not the SQL. `rocky apply` recompiles the project and runs it. [`rocky run`](#rocky-run) fuses both steps for local work.

`rocky plan` needs a replication pipeline. A transformation project reviews its changes through a branch:

```bash
rocky branch create pr1
rocky run --branch pr1          # builds into a branch schema, not production
rocky plan promote pr1          # writes a promote plan and prints its plan_id
rocky apply <plan-id>           # copies the reviewed branch tables to production
```

The branch run runs each model's `[[tests]]` because `[run] tests_on_run` defaults to `enforce`. [`rocky plan promote`](#the-failing-tests-gate) refuses a branch with a recorded failing test or with no passing test evidence.

## Global Flags

The global flags (`--config`, `--output`, `--state-path`, `--state-namespace`, `--principal`, `--principal-id`, `--cache-ttl`) apply to every command. Put `--config`, `--state-path` and `--state-namespace` before the subcommand: `rocky --config prod.toml run`, not `rocky run --config prod.toml`. See [Global flags](/reference/cli/#global-flags) for the defaults.

### `--state-namespace`

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--state-namespace <KEY>` | `string` | (none) | Route this invocation to its own `<models>/.rocky-state/<KEY>.redb`, so concurrent runs (one per pipeline, client or tenant) do not wait on redb's single-writer lock. Off by default. `<KEY>` must match `^[a-zA-Z0-9_]+$`. An explicit `--state-path` disables namespacing; otherwise this flag wins over `[state] namespacing`. See [State namespacing](/reference/configuration/#state-namespacing). |

---

## `rocky init`

Create a new Rocky project from a template. The command refuses when the directory already holds a `rocky.toml`.

```bash
rocky init [path] [--template <NAME>]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `path` | `string` | `.` | Project directory. Rocky creates it when it does not exist. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--template <NAME>` | `string` | `duckdb` | One of the templates below. Connection values are `${VAR}` placeholders, never inline secrets. |

| Template | What it writes |
|---|---|
| `duckdb` | `rocky.toml`, `models/_defaults.toml`, a sample `models/stg_orders` model, and `seeds/seed.sql` sample data. |
| `databricks-fivetran` | `rocky.toml` for a Fivetran-to-Databricks pipeline, and an empty `models/`. |
| `snowflake` | `rocky.toml` for Snowflake, and an empty `models/`. |
| `bigquery` | `rocky.toml`, `models/_defaults.toml`, and a `models/welcome` model. |
| `trino` | `rocky.toml`, `models/_defaults.toml`, and a `models/welcome` model. |

### Examples

```bash
rocky init
```

```
Initialized Rocky project in .

  rocky.toml           — pipeline config (DuckDB)
  models/_defaults.toml — shared model defaults
  models/stg_orders.*  — sample transformation model
  seeds/seed.sql       — sample data

Quick start:
  duckdb playground.duckdb < seeds/seed.sql
  rocky validate
  rocky compile --models models/
```

Create a Trino project in a new directory:

```bash
rocky init acme-trino --template trino
```

The Trino `rocky.toml` reads `${TRINO_HOST}`, `${TRINO_USER}` and `${TRINO_PASSWORD}` (HTTP Basic), or `${TRINO_JWT}` (a JWT bearer token). Comments in the file describe both modes.

### Related Commands

- [`rocky validate`](#rocky-validate) -- check the generated config
- [`rocky playground`](/reference/commands/development/#rocky-playground) -- a sample DuckDB project with no credentials

---

## `rocky validate`

Check the pipeline configuration. `rocky validate` connects to no external API and exits non-zero if any check fails.

```bash
rocky validate [flags]
```

### Flags

No command-specific flags. Uses [global flags](#global-flags) only.

### Checks Performed

The main checks are below. Each finding carries a `V` code.

| Check | Description |
|-------|-------------|
| TOML syntax | The config file parses without errors as v2 (named adapters + named pipelines). |
| Adapters | Each `[adapter.NAME]` has a known type with its required fields set. The known types are `databricks`, `duckdb`, `snowflake`, `bigquery`, `trino`, `postgres`, `redshift`, `clickhouse`, `sqlserver`, `spark`, `fivetran`, `airbyte`, `iceberg` and `manual`. An unknown type is `V017`. For Databricks, set `token` or `client_id`/`client_secret`. |
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

Validate a specific config file with `rocky -c pipelines/prod.toml validate`. A finding prints as `!!  adapter.prod: no auth configured (token or client_id/secret)`.

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
| `--with-schemas` | `bool` | `false` | Warm the schema cache: one `batch_describe_schema` call per `(catalog, schema)` pair, stored in the state store. Later `rocky compile` and `rocky lsp` runs read it instead of typing leaf models as `Unknown`. A source that errors is logged and skipped. |
| `--no-cache` | `bool` | `false` | Only with `--emit-fivetran-state-to`. Fetch the Fivetran state envelope from the API and skip the read caches under `[adapter.<name>.cache]`. A successful fetch still writes back to that cache. Use it when the cache may be stale, for example after a credential rotation. An open circuit breaker still short-circuits the fetch. |
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
    }
  ]
}
```

Use `-o table` for a table, and `--pipeline <name>` when the project defines several pipelines.

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

`<STEM>` is `<PATH>` without a trailing `.json`, so `--emit-fivetran-state-to state.json` writes `state.<account_hash>.<destination_id>.json`.

The write is idempotent. Rocky leaves the JSON file alone when its content hash matches the `.blake3` sibling, so a `stat(2)` watcher fires only when Fivetran state changed.

A connector whose `connectors/{id}/schemas` endpoint returns 404 is left out of the `schemas` map and logged at WARN. It still appears under `connectors`. Discover exits non-zero only when every connector returns 404.

### New sources and cross-source collisions

Two opt-in signals catch onboarding problems before any catalog is created. Set them under [`[pipeline.NAME.source.discovery]`](/reference/configuration/#pipelinenamesourcediscovery). The fields are omitted from the JSON output when not enabled.

- **`new_sources`**: set `report_new_sources = true` to diff the inventory against the prior snapshot. The first discover records the baseline and reports nothing.
- **`collision_candidates`**: set `on_collision = "warn"` (or `"error"`) to flag the same external object onboarded under more than one schema. Each entry pairs `external_object_id` with its `sources`. With `"error"`, discover also exits non-zero. Only adapters that resolve external object ids (such as Fivetran) fill it.

```json
{
  "command": "discover",
  "new_sources": ["src__acme__ca_central__shopify"],
  "collision_candidates": [
    { "external_object_id": "act_1234567890",
      "sources": ["src__acme__us_west__shopify", "src__acme__eu_central__shopify"] }
  ]
}
```

`collision_candidates` is the preventive half of duplicate detection. [`cross_source_overlap`](/concepts/data-quality-checks/#cross-source-overlap) is the detective half, at `rocky run` time.

### Related Commands

- [`rocky plan`](#rocky-plan) -- generate SQL from discovered sources
- [`rocky run`](#rocky-run) -- discover and execute in one step

---

## `rocky plan`

Generate the replication SQL Rocky would run, without running it. The command needs a replication pipeline and refuses a transformation-only project. It connects to the source, then prints the copy SQL for each table. For a transformation model, add `--model <name>` or use [`rocky emit-sql`](/reference/commands/modeling/#rocky-emit-sql).

Rocky writes the plan to `.rocky/plans/<plan-id>.json` and prints the `plan_id`. A reviewer reads the SQL. Then [`rocky apply <plan-id>`](#rocky-apply) recompiles the project and runs it. Nothing touches the warehouse between the two steps. For local work, [`rocky run`](#rocky-run) does both in one command and writes no plan file.

A compile error in the `--model` model, or in a model it reaches through `depends_on` or a bare-name read, refuses the plan. Without `--model`, an error in any model, including one from another pipeline, refuses it. Rocky prints each model and diagnostic code, exits `1` and writes no plan file. Warnings do not stop the plan.

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

The gate is optional for `run`, `replication` and `promote` plans. A `[policy]` rule that resolves to `require_review` turns it on. Without `[policy]`, these plans go straight from `rocky plan` to `rocky apply`.

Four plan kinds are always gated: `ai_authored`, `backfill`, `gc` and `restore`. They come from the MCP `propose` tool, `rocky backfill`, `rocky gc --derivable` and `rocky restore`. See [`rocky review`](/reference/commands/governance-reclamation/#rocky-review).

### Flags

These flags apply to the default `rocky plan` form, not to `rocky plan promote`. They match [`rocky run`](#rocky-run) except as noted. `rocky plan` adds `--semantic`, `--intent`, `--base` and `--cost-estimate`. `rocky run` adds flags such as `--watch` that need no plan file. `--parallel` defaults to `1` here.

Rocky records the execution flags in the plan file, so `rocky apply` replays the same intent:

- selection: `--filter`, `--pipeline`, `--model`, `--models`, `--all`, `--env`
- partitions: `--partition`, `--from`, `--to`, `--latest`, `--missing`, `--lookback`, `--parallel`
- routing: `--shadow`, `--shadow-suffix`, `--shadow-schema`, `--branch`, `--dag`
- other: `--governance-override`, `--resume`, `--resume-latest`, `--idempotency-key`

`--semantic` and `--base` are not recorded. They only add `breaking_verdict` to the plan output. `--intent` is recorded as the plan's `intent` key, which leaves the `plan_id` of a plan without it unchanged. The verdict that checks it goes only into the `intent_check` output field.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--filter <key=value>` | `string` | | Filter sources by component value (e.g., `client=acme`). |
| `--pipeline <NAME>` | `string` | | Pipeline name (required if multiple pipelines are defined). |
| `--model <NAME>` | `string` | | Plan one compiled model and skip replication. |
| `--select <SELECTOR>...`, `-s` / `--exclude <SELECTOR>...` / `--state-ref <REF>` | `string` | | [Node selection](/reference/node-selection/). It must resolve to exactly one model, planned like `--model`. |
| `--models <PATH>` | `PathBuf` | | Models directory for transformation execution. |
| `--all` | `bool` | `false` | Plan both replication and compiled models. |
| `--governance-override <JSON>` | `string` | | Governance config as inline JSON or `@file.json`, merged with the defaults. Stored in the plan. |
| `--resume <RUN_ID>` / `--resume-latest` | `string` / `bool` | | Resume a failed replication run from its last checkpoint. Rocky mints a new `run_id` and records the prior one as `resumed_from`. `--resume-latest` picks the run at apply time. Rejected with `--dag` at parse time. |
| `--shadow`, `--shadow-suffix <SUFFIX>`, `--shadow-schema <NAME>`, `--branch <NAME>` | | | Same as [`rocky run`](#rocky-run). `--branch` is `--shadow --shadow-schema <branch.schema_prefix>`. `--shadow-suffix` defaults to `_rocky_shadow`. `--branch` conflicts with the three shadow flags. |
| `--partition <KEY>`, `--from <KEY>`, `--to <KEY>`, `--latest`, `--missing`, `--lookback <N>` | | | Same as `rocky run`. `--missing` resolves against the state store at apply time. |
| `--parallel <N>` | `integer` | `1` | Run N partitions at a time. Warehouse queries only: state writes serialize through the state store. |
| `--dag` | `bool` | `false` | Plan all pipelines as one DAG in dependency order. Rejected with `--resume` / `--resume-latest`. With `--principal agent`, the plan's fingerprint and review cover every pipeline's models. |
| `--idempotency-key <KEY>` | `string` | `$ROCKY_IDEMPOTENCY_KEY` | Opaque key that dedups this run against prior runs with the same key, on every state backend. See [`rocky run`](#rocky-run). Never put a secret in a key. |
| `--env <NAME>` | `string` | | Scope the governance preview (`mask_actions`) to one environment, so `[mask.<env>]` overrides overlay the `[mask]` defaults. |
| `--semantic` | `bool` | `false` | Run the breaking-change classifier against `--base` and attach the verdict as `breaking_verdict`. Decision support only: it never gates the plan or changes the exit code. |
| `--intent <INTENT>` | `string` | | **Experimental.** State what the change does and check it on the data. The only value is `refactor`. See [Check a refactor with `--intent`](#check-a-refactor-with---intent). |
| `--base <ref>` | `string` | `main` | Git ref the working tree is compared against. `--semantic`, `--intent` and the change classification of every run plan use it. |
| `--cost-estimate <MODE>` | `string` | `heuristic` | `heuristic` is offline. `adapter` asks the warehouse with `EXPLAIN`. See [Cost preview](#cost-preview). Not recorded in the plan. |

### Cost preview

When the plan covers transformation models, the JSON output has a `cost_preview` object. It shows what applying the plan would rebuild, and roughly what it would cost, before any model runs.

| Field | Meaning |
|---|---|
| `is_estimate` | Always `true`. Every figure is an estimate. |
| `source` | `heuristic`, `adapter`, or `mixed` when the adapter could not estimate some models. Each row in `models` has its own `source`. |
| `models_to_rebuild` | The rebuild scope: the number of models the plan rebuilds. |
| `estimated_bytes_scanned`, `estimated_cost_usd` | Sums over every model. A total is absent when any model lacks the figure. |
| `previous_cost_usd` | The observed cost of the same models in the last successful production run, priced from the state store. Absent when any model has no such run. |
| `cost_delta_usd` | `estimated_cost_usd - previous_cost_usd`. Set only when every model has an adapter estimate and a previous cost. |
| `notes` | Why a figure is missing, for example a failed `EXPLAIN`. |

The default `heuristic` uses the cost model of `cost_hint` in `rocky compile`, with placeholder source statistics, so its confidence is always `low`. Use it to compare plans, not to set a budget. A DuckDB target costs `0`. `--cost-estimate adapter` runs `EXPLAIN` per model, as [`rocky estimate`](/reference/cli/#rocky-estimate) does. A model the adapter cannot estimate falls back to the heuristic, with a note.

The preview is report-only. It never changes the planned models, the budget check or the exit code, and it is not part of `plan_id`.

> The `--semantic` verdict diffs output schema only. It cannot see a `WHERE`, `JOIN`-key or `CASE` rewrite that changes values but not the schema, so an empty `findings` list is not a safety signal. The `caveat` field says so. See the [CI/CD guide](/guides/ci-cd/#semantic-breaking-change-findings-and-the-promote-gate) for the full flow and the [`plan` schema](https://github.com/rocky-data/rocky/blob/main/schemas/plan.schema.json) for the `SemanticPlanVerdict` shape.

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

Limits of a `match`:

- It means no difference on the current data, not a proof for other data. A `WHERE` change that no current row reaches still gives `match`. When both outputs are empty, `detail` says so.
- Rocky checks the `SELECT` output only, not masks, classifications, targets, strategy or other config. When the target or strategy also changed, `detail` says so.
- Each model reads the upstream tables that already exist in the warehouse. A changed upstream is listed in `upstream_changed`, and per-model matches do not add up to a match for the chain.
- The verdict is report-only. It relaxes no gate and never changes the exit code. The flag itself can fail the command: with `--dag`, with no models directory, or when the working tree does not compile.
- The check needs a replication pipeline in `rocky.toml`. It pays for the extra builds on your DuckDB database.
- The plan records `intent: refactor` as the author's claim, never the verdict.

The `--intent` flag is not the free-text `intent = "..."` key in a model sidecar, which describes a model for the AI commands.

```bash
rocky plan --intent refactor --base main --output json
```

### `rocky plan promote`

Plan a branch promotion. Rocky runs the approval gate, the breaking-change gate and the failing-tests gate now, then stores a promote plan for `rocky apply <plan-id>`. The gates do not re-run at apply time, which is what makes "plan in the pull request, apply on merge" work.

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
| `--allow-failing-tests` | `bool` | `false` | Bypass the failing-tests gate, including a refusal for missing test evidence. Always records a `FailingTestsAllowed` audit event in the plan, and prints a warning. |
| `--filter <key=value>` | `string` | | Filter the promote targets. A transformation pipeline supports the keys `table`, `model`, `catalog`, and `schema`. |
| `--pipeline <NAME>` | `string` | | Pipeline to plan against. Required when `rocky.toml` defines more than one pipeline. |
| `--models <PATH>` | `PathBuf` | `models` | Models directory used by the breaking-change gate. |

On success the output is a `PlanOutput` with `plan_kind: "promote"` and the `plan_id` you pass to `rocky apply`.

#### The failing-tests gate

After the breaking-change gate, Rocky reads the runs of the branch (`rocky run --branch <name>`) since its creation. Each model's newest evidence decides: a run that recorded a test result for the model, or a run in which every execution of the model succeeded. A run that did not touch a model, or failed any part of its build, leaves that model's evidence as it was.

```
run 1  --tests enforce   orders FAILS a test
run 2  --model status    builds only status
promote                  refused: orders' newest evidence is run 1
```

Rocky refuses the promote (exit `1`, no plan, a `FailingTestsBlocked` audit event naming each reason) when:

- A model's newest evidence holds an error-severity `[[tests]]` entry that failed or could not run.
- The branch has no run, or its newest run failed.
- A model with error-severity tests was last built with no recorded test result (for example `--tests off`).
- Rocky cannot load the models.

Fix the data and re-run `rocky run --branch <name>`, or pass `--allow-failing-tests`. The override prints a `WARNING` and records `FailingTestsAllowed` and one `TestEvidenceMissing` event per gap.

Two cases only warn, with a `WARNING` on stderr and a `TestEvidenceMissing` event:

- The branch has no run in the local store and `[state]` uses a remote backend (`s3`, `gcs`, `valkey` or `tiered`). A local run is still checked. With a local backend, a branch with no run refuses.
- A transformation pipeline's models directory does not exist.

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

Plan one model, then execute the stored plan:

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

The check fails closed and runs before any policy gate. A plan that carries a product identity (`product_id` + `spec_digest`) refuses a bare `rocky apply`: the flag is required. The flag against a plan with no `spec_digest` refuses too. A mismatch names both digests.

### What each plan kind executes

The plan file carries its kind. Apply reads that kind and takes one path.

| `plan_kind` | Generated by | What apply does |
|---|---|---|
| `run` | `rocky plan` | Re-executes the full pipeline with the flags the plan captured. |
| `replication` | `rocky plan` on a project with no compiled models | Re-runs discovery and compares the result against the plan-time snapshot, then executes. See the staleness rules below. |
| `promote` | `rocky plan promote` | Runs the promote statements. The approval and breaking-change gates ran at plan time and are not repeated. The HTTP API refuses a promote plan unless `[branch.approval] required = true` and the plan holds the approvals the config needs. |
| `compact` | `rocky compact` | Runs the `OPTIMIZE` and `VACUUM` statements. |
| `archive` | `rocky archive` | Runs the `DELETE` and `VACUUM` statements. The HTTP API (`POST /api/v1/jobs/apply`, so the browser UI too) refuses an archive plan with `archive_apply_terminal_only`. Apply it in a terminal. |
| `ai_authored` | an AI agent, via the `propose` MCP tool | Same execution path as a `run` plan, but only after a review marker exists. |
| `backfill` | `rocky backfill` | Rebuilds the scoped model set. Always needs a review marker. |
| `gc` | `rocky gc --derivable` | Evicts each artifact. Always needs a review marker. |
| `restore` | `rocky restore` | Rebuilds each evicted artifact. Always needs a review marker. |

### What apply checks for staleness

Each plan kind compares different things.

**`replication`.** Apply re-runs discovery and compares it with the plan-time snapshot. A difference aborts an unfiltered apply and tells you to re-plan. With `--filter`, a difference outside the filter scope logs a warning. A difference inside it aborts.

**`run`.** Apply reloads `rocky.toml`, recompiles the models and runs those. It does not replay stored SQL. The plan records a fingerprint of its models, and a second one of the models plus the config they run under. If a model was added, removed or edited after the plan, apply refuses with `plan_models_changed`. The check also covers `ai_authored` and `backfill` plans. It runs after the policy and review gates and before any warehouse statement.

| Who applies | What apply compares | Refusal |
|---|---|---|
| A person | The models only. A config edit after approval still applies. Exception: where the run applies masks (a replication plan with `--all` or `--models`), a changed strategy of a tag those models classify with, for the plan's `--env`, refuses. | `plan_models_changed` |
| An agent (`--principal agent` or `ROCKY_PRINCIPAL=agent`) | The models and the config. It also checks the routing identity and repeats the fingerprint check when the models execute. `ai_authored` and `backfill` plans always apply as an agent. | `plan_config_changed` when only the config changed |
| `rocky review --approve` | The models only, for every plan kind. A plan approved from another environment (such as `rocky serve`) passes when its models did not change. | "the models changed since this plan was written" |

A `${VAR}` inside a model's SQL or sidecar is part of the models: a different value refuses with `plan_models_changed`. A plan whose models did not compile at plan time has no fingerprint and is not checked, nor is a snapshot, load, quality or replication-only plan. A plan made before this fingerprint existed refuses with `plan_snapshot_missing`: plan it again.

A review-gated `--dag` plan is checked across the whole DAG: every pipeline's models, from that pipeline's own directory and file glob, plus the project's seeds (data, sidecar and seed hooks). Apply refuses if any model or seed was added, removed or changed, and with `plan_config_changed` if the config changed. A credential such as `token` or `password` is not checked. A connection field such as `host` or `path` is, by its resolved `${VAR}` value. An agent applying a `--dag` plan is still refused, because the DAG's sub-runs are not yet policy-gated.

**`promote`.** Apply does not recompute the branch-state hash, and it does not re-run the approval or breaking-change gates. The `[policy]` gate still runs.

`rocky apply` gates on the identity it runs under. The global `--principal` flag sets it to `human` (default) or `agent`. `ROCKY_PRINCIPAL` may only raise it to `agent`. Rocky combines the runtime identity most restrictively with the plan kind's default. The kinds `ai_authored`, `backfill` and `gc` default to `agent`, so an agent-scoped policy rule applies to them even for an interactive `rocky gc`. Rocky never trusts the principal field inside the plan file. Without a `[policy]` block, the principal changes nothing.

`rocky run` reads the same `--principal` and `ROCKY_PRINCIPAL`. An agent's run in a project with `[policy]` is evaluated under the `run` capability, and a `deny` exits `1`. An invalid `ROCKY_PRINCIPAL` fails the run. See [`[policy]`](/reference/configuration/#policy).

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
| `--select <SELECTOR>...`, `-s` / `--exclude <SELECTOR>...` / `--state-ref <REF>` / `--state-working-tree` | `string` | | Build the [selected models](/reference/node-selection/) and skip replication. Unselected upstreams are read as they exist. Not with `--dag`, `--watch`, `--all`, `--filter`, or `--resume`. |
| `--contracts <PATH>` | `PathBuf` | | Contracts directory for this run. Default: the project `contracts/` directory. With `--model` and `--pipeline`, the selected model must have a contract there. Not with `--watch`. |
| `--governance-override <JSON>` | `string` | | Additional governance config as inline JSON or `@file.json`, merged with defaults. |
| `--models <PATH>` | `PathBuf` | | Models directory for transformation execution. |
| `--all` | `bool` | `false` | Execute both replication and compiled models. |
| `--resume <RUN_ID>` | `string` | | Resume a specific previous replication run from its last checkpoint; mints a new `run_id` and records the prior one as `resumed_from`. Rejected with `--dag` or `--shadow`. |
| `--resume-latest` | `bool` | `false` | Resume the most recent failed replication run from its last checkpoint; mints a new `run_id` and records the prior one as `resumed_from`. Rejected with `--dag` or `--shadow`. |
| `--tests <MODE>` | `off`, `report`, `enforce` | config, else `enforce` | Evaluate each built model's `[[tests]]` after it builds. See [Tests during a run](#tests-during-a-run). |
| `--retry-failed [<RUN_ID>]` | `string` | | Rerun the failed and never-built models of a failed transformation run, and the models downstream of them. Without a value, the run is the latest production run of the pipeline (or of `--branch`). A model is not rerun when that run built it with the SQL it has now. Refuses when the run succeeded, is not found, is not a production run of this pipeline, nothing is left to retry, or `[state]` uses a remote backend. Rejected with `--resume`, `--resume-latest`, `--watch`, `--dag`, `--model`, `--select`, `--exclude`, `--filter`, `--all`, `--shadow`, `--shadow-schema` and the `--defer` flags. See [Retrying a failed run](#retrying-a-failed-run). |
| `--shadow` | `bool` | `false` | Write to shadow targets, compare them with production, then drop them after any completed verdict. A failed threshold exits non-zero. |
| `--keep-shadow` | `bool` | `false` | Requires `--shadow` and conflicts with `--watch`. Keep shadow objects for a separate `rocky compare`. |
| `--shadow-suffix <SUFFIX>` | `string` | `_rocky_shadow` | Requires `--shadow`. Appends a suffix to table names. Conflicts with `--branch`. `--shadow` alone uses `_rocky_shadow`. |
| `--shadow-schema <NAME>` | `string` | | Requires `--shadow`. Overrides the schema for shadow tables. Conflicts with `--branch`. |
| `--branch <NAME>` | `string` | | Execute against a named branch previously registered with `rocky branch create`. Applies the branch's `schema_prefix` to every target (internally equivalent to `--shadow --shadow-schema <branch.schema_prefix>`). Conflicts with `--shadow`, `--shadow-schema`, and `--shadow-suffix`. The run records `<NAME>` as `rocky_branch` in [run history](/reference/commands/administration/#rocky-history). |
| `--partition <KEY>` | `string` | | Run one partition by its canonical key (`2026-04-07` for daily, `2026-04` for monthly). Mutually exclusive with `--from`, `--to`, `--latest` and `--missing`. |
| `--from <KEY>` / `--to <KEY>` | `string` | | Closed partition range, both bounds inclusive. Each requires the other. Both bounds must align to the model's grain. |
| `--latest` | `bool` | `false` | Run the partition that contains now (UTC). The default for a `time_interval` model when no other partition flag is given. |
| `--missing` | `bool` | `false` | Run the partitions missing from the state store, from the model's `first_partition` up to now. Errors if `first_partition` is unset. |
| `--lookback <N>` | `integer` | | Also recompute the previous N partitions, for late-arriving data. Overrides the model's TOML `lookback`. |
| `--dag` | `bool` | `false` | Run all pipelines as one DAG in dependency order. Each pipeline is a node, cross-pipeline `depends_on` edges set the order, and layers run in parallel. Rejected with `--resume` and `--resume-latest`. |
| `--idempotency-key <KEY>` | `string` | `$ROCKY_IDEMPOTENCY_KEY` | Skip this run when a run with the same key already succeeded (`status = "skipped_idempotent"`) or holds the key now (`skipped_in_flight`). Both exit `0`. Works on every state backend (`local`, `valkey`, `tiered`, `s3`, `gcs`), and only one caller can claim a key or adopt a stale claim. `valkey` and `tiered` need a server that allows `EVAL`. Rejected with `--resume` and `--resume-latest`. Keys are stored as given: never put a secret in one. See [`[state.idempotency]`](/reference/configuration/). |
| `--env <NAME>` | `string` | | Apply the `[mask.<env>]` overrides on top of the `[mask]` defaults in the post-run governance pass. Classification tags and retention are the same in every environment. |
| `--watch` | `bool` | `false` | Re-run the pipeline on every change to `rocky.toml` or a file under the watched models directories, debounced to 200 ms. A failed run does not exit the loop. Ctrl-C exits between runs. Rejected at parse time with `--dag`, `--resume`, `--resume-latest`, `--idempotency-key`, and `--model` (rejected at parse time). |
| `--defer` | `bool` | `false` | Build only the `--model`-selected models, resolving unbuilt upstream models to an existing (production) schema. Inert without `--model`. Transformation models only. Rejected with `--dag`. See the note below. |
| `--defer-to <SCHEMA>` | `string` | | Requires `--defer`. Schema every deferred upstream resolves to (catalog and table are kept). Default: each upstream's own target schema. |
| `--defer-to-state <PATH>` | `PathBuf` | | Saved production state store file the deferred upstreams resolve from. Requires `--defer`; conflicts with `--defer-to`. Each unbuilt upstream a selected model reads resolves to the table the newest successful production run recorded for it. Opened read-only. Refuses before any write when the store is missing, has an incompatible state schema version, or has no recorded table for a needed upstream. See [Defer to a saved production state](/guides/skip-and-defer/#defer-to-a-saved-production-state). |
| `--defer-run-id <RUN_ID>` | `string` | | Read deferred upstreams only from this production run in the `--defer-to-state` store. Requires `--defer-to-state`. |
| `--skip-unchanged` | `bool` | `false` | Turn on the model-skip gate for this run, whatever `[run] skip_unchanged` says: skip a transformation model whose logic and every upstream's data appear unchanged. Best effort, not an equivalence guarantee. Non-deterministic SQL and models without provably complete lineage (CTEs, subqueries, `PIVOT`/`UNNEST`, set operations) always rebuild. See [`[run]`](/reference/configuration/#run) for the full eligibility rules. |
| `--force-rebuild` | `bool` | `false` | Force every selected model to build. Bypasses the `--skip-unchanged` gate and the content-addressed column-level skip. Use it after a change the IR hash cannot see, such as a UDF redefinition or a session setting. |
| `--no-reuse` | `bool` | `false` | Turn off content-addressed reuse for this run, even when `[reuse]` is on. Every content-addressed model builds. Also turns off the `[reuse] column_level` skip. |
| `--no-prune` | `bool` | `false` | Force a full replication pass: turn off `prune_unchanged` for this run, so every table is copied again. Use it after a manual change to a target. No effect when `prune_unchanged` is off. |
| `--no-sync-trigger` | `bool` | `false` | Do not start a source sync for this run, even when `[pipeline.NAME.source.discovery.sync] trigger = true`. The run still waits for a sync that is already running. It does not change `sync_trigger` in `rocky discover`; set `trigger = false` in the config for that. See [Fivetran adapter](/reference/adapters/fivetran/#start-a-sync). |
| `--full-refresh` | `bool` | `false` | Rebuild transformation `incremental` models with `CREATE OR REPLACE TABLE ... AS`. Every `@incremental_filter` becomes `TRUE`, so the table holds the model's full result. Other strategies are unaffected: a `merge` or `delete_insert` model's SQL often selects only recent rows, and rebuilding from it would drop history. The flag also turns off the `--skip-unchanged` gate. |
| `--refuse-hooks` | `bool` | `false` | Refuse the run when the config defines any `[hook]` command or `[hook.webhooks]` entry that would fire, before anything fires or any model builds. For CI jobs that run a config they do not trust, such as a pull request's `rocky.toml`. Not supported with `--watch`. |
| `--var <name=value>` | `string` (repeatable) | | Bind a per-run variable for the `@var(name)` / `@var(name, default)` markers in model SQL. Repeat for several. `@var()` resolves at compile time. `${ENV}` resolves while parsing `rocky.toml`. A `@var(name)` with no binding and no default fails to compile, naming the variable. Works with `--dag` and `--watch`. See [`@var()` run variables](/reference/model-format/#var-run-variables). |
| `--assume-fresh-state` | `bool` | `false` | Disaster recovery only, and audited. Treat a failed download of remote state as a deliberate fresh start, so the run proceeds with an empty ledger and uploads turned back on. Requires a remote `[state]` backend (`s3`, `gcs`, `valkey` or `tiered`). Rejected with `--dag` and `--watch`. Without it, a failed download refuses auto-applies and suppresses the run's state uploads. |
| `--parallel <N>` | `integer` | `4` without `--dag` | Models in a topological layer (and partitions of a `time_interval` model) run up to N at a time. Under `--dag` it bounds the pipeline nodes that run at once, with no default (unbounded). It does not bound a replication pipeline's table fan-out, which is `[execution] concurrency` (default 32). DuckDB always runs serially. Snowflake and Databricks parallelize up to N. |

:::caution[`--defer` SQL-rewrite limitation]
`--defer` rewrites each selected model's SQL with the Databricks dialect parser. Unsupported constructs (trailing-comma select lists and `STRUCT(...)` literals) fail with a clear error. Build those models without `--defer`.
:::

### Retrying a failed run

`--retry-failed` reruns a failed transformation run. It picks the models again. It keeps no checkpoint, so it is not `--resume`, which stays replication-only.

```bash
rocky run --pipeline shop                 # status_counts fails, customer_orders never starts
rocky run --pipeline shop --retry-failed  # reruns status_counts and customer_orders
```

A model stays out of the retry when the run holds a `success` row for it with the same `sql_hash` as the SQL the model generates now. Every model downstream of a rerun model is rerun too, because it must read the rebuilt table. Rocky rebuilds when it is in doubt:

- A model whose SQL depends on the warehouse (an incremental model), on `--var` or on a Snowflake warehouse name is always rerun.
- A model the failed run skipped as unchanged or reused has no `success` row, so it is rebuilt.
- A retry of a failed retry looks only at the rows of that retry. Models the first run built are rebuilt.

The retry is recorded under the same pipeline with `retry_of` set to the run it retried. A second `--retry-failed` after a good retry refuses. A retry never counts as the pipeline's latest successful run for `after` and `freshness` schedules, and it does not feed the duration estimate.

`--retry-failed` reads run history from the local state store. With a remote `[state]` backend, run the failed models with `--select`.

### Per-model status

### Tests during a run

`--tests <off|report|enforce>` runs each built model's sidecar `[[tests]]` against the table the run just wrote. The tests run before the model's downstream models build. It overrides `[run] tests_on_run`, which defaults to `enforce`.

| Mode | What happens |
|---|---|
| `off` | No test query runs. The output has no `test_results`. |
| `report` | Tests run and are listed under `test_results`. A failing test never fails the model. |
| `enforce` | As `report`. An error-severity test that fails, or whose query cannot run, fails the model. Its downstream models do not build. |

A `severity = "warning"` test is listed in every mode and never fails a model. The table a failed model wrote stays in place. Nothing is rolled back. Tests run on the table the run wrote, so a `--shadow` or `--branch` run tests the shadow or branch table. A `relationships` test's `to_table` is used as written, so on such a run it checks the branch or shadow table against the `to_table` you named, usually the production parent. `content_addressed` models write no warehouse table and are not tested. The results are saved on the run record, where [`rocky plan promote`](#the-failing-tests-gate) reads them. `rocky test --declarative` is unchanged.

`--output json` carries `model_statuses` when at least one model did not build. It covers transformation pipelines and model-only runs (`--model`, `--select`, `--retry-failed`). A replication run never has it, even with `--all`. It is a roll-up of `materializations`, `errors`, `model_decisions` and `contained`.

| Field | Meaning |
|---|---|
| `model` | The model name. |
| `status` | `built`, `failed`, `skipped` or `not_started`. |
| `reason` | For `skipped` and `not_started`: `unchanged`, `reused`, `upstream_failed`, `frozen` or `run_stopped`. |
| `blocked_by` | For `upstream_failed`: the failed upstream models. |

A fail-fast run (`[resilience] contain_failures = false`) stops at the first failure. Models that had not started are `not_started` with reason `run_stopped`. The text summary lists them and hints at `[resilience] contain_failures = true`, which keeps building models that do not depend on the failure. The hint only appears when a project sets `false`, because `true` is the default.

### Recovering replication checkpoints

`--resume` and `--resume-latest` refuse a checkpoint whose planned tables all copied but whose terminal run record is missing, because resuming it would skip the post-copy checks. Incomplete crash checkpoints stay resumable.

Follow the refusal's route. A matching fresh run copies the targets again, runs the checks and writes a new run record. Rocky then marks the old checkpoint superseded. Matching needs the same target endpoint and a plan that holds every checkpoint target. An old checkpoint alone never fails a fresh run. #2235 stays open for remaining recovery gaps.

For checkpoints from Rocky 1.75.0 or earlier, set `strategy = "full_refresh"` for the affected tables and run `rocky run --pipeline <name> --no-prune` with no resume flag. Keep full refresh until `rocky state reconcile-watermark --pipeline <name>` sets the cursor from the replacement target's maximum timestamp (`--dry-run` previews it, repeat `--table catalog.schema.table` to select targets). A crash under 1.75.0 or earlier may leave a stale cursor after an INSERT, so the first run after an upgrade may append those rows again. Repair the cursor before that run. See [Interrupted replication](/concepts/incremental/#recovering-an-interrupted-replication) for routes, adapters and remote durability limits.

### Contracts on every run

Every `rocky run` reads the `contracts/` directory beside the models directory, with no flag. `rocky compile`, `rocky ci` and `rocky test` read it too.

The run checks each contract in the compile that supplies the model SQL, for `--dag`, `--pipeline`, `--all`, `--select` and `--model`. On a contract error (`E010` to `E013`):

- Rocky does not write that model. Its existing table stays.
- Rocky does not run the models downstream of it.
- The run exits non-zero. Under `--dag`, the failed node's `error` names the code.

When the source table has no known schema, Rocky cannot infer a declared column type, and the compile reports `I003`. The run reads that source's columns from the warehouse and compiles again. If the warehouse cannot describe the source, the type stays unchecked. With `[contracts] strict = true`, that is `E059` and the run refuses before it writes. A `nullable = false` contract on a column from such a source is `E012`, because Rocky reads an unknown source column as nullable. The run drops the `E012` when the warehouse reports the column as NOT NULL.

`--contracts <DIR>` replaces the project directory for one run. In the project directory, a contract for a model the compile omits is skipped. In an explicit `--contracts` directory, it is the `W011` warning.

### Guard one model with a contract

To build one `full_refresh` transformation model and require its contract:

```bash
rocky run --pipeline transform --model int_order_lines --contracts contracts -o json
```

The directory must hold `int_order_lines.contract.toml`. An error such as `E010` fails the run before Rocky replaces the table, and the failure JSON has no materialization for that model. The guard does not cover a branch or deferred run. It refuses models whose strategy is not `full_refresh`, models that add a `surrogate_key` after compilation, and idempotency and skip options that could report success without rebuilding. A failed contract can still update run history or state sync. `I003` means the declared type was not checked. Required-column checks still run.

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
      "duration_ms": 2300,
      "metadata": { "strategy": "incremental", "watermark": "2026-03-30T10:00:00Z" }
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

Shadow mode must isolate the run from production. Rocky refuses a shadow or branch run that it cannot isolate, in any of these cases:

- The selected transformation set holds a `content_addressed` or `time_interval` model. Both need storage or partition-state isolation that shadow mode lacks.
- The suffix or schema would collide with a production target or another shadow target.
- The dialect treats identifier case as part of object identity (Snowflake and BigQuery), the run routes more than one model, and two routed targets differ only by case. Rocky matches upstream references case-insensitively, so a read could land on the wrong object. Rename a target or scope the run to one of them. Trino quotes targets but treats case-only differences as one object, so it proceeds.

`--shadow` and `--branch` isolate `rocky run` for transformation and replication pipelines. Rocky refuses both on `rocky run --dag` and on quality, snapshot and load pipelines, before it opens an adapter, writes state or claims an idempotency key. See [Shadow mode](/concepts/shadow-mode/).

Or run against a named branch:

```bash
rocky branch create fix_price --description "testing reprice migration"
rocky run --filter client=acme --branch fix_price
```

Run in watch mode for the inner-loop developer workflow, where every save re-materializes the pipeline against the local DuckDB warehouse:

```bash
rocky run --watch
```

`--watch` watches the parent directory of `rocky.toml` (filtered to that file) and the models directories recursively:

- With `--models <dir>`, it watches that directory when it exists.
- Without `--models`, it watches each transformation pipeline's models directory, or only the `--pipeline` one. The directory is the part of the `models` glob before the first wildcard, so `models/**` and `models/*.sql` both give `models/`.
- A directory is watched once. If the pipeline's directory is the `rocky.toml` directory (`models = "./*.sql"`), it is watched once, recursively. If that watch fails (for example the Linux inotify limit), Rocky warns and watches `rocky.toml` only.
- A file under a directory that starts with `.` (`.git`, `.venv`), or is `target` or `node_modules`, does not start a re-run.
- A directory that does not exist at startup, or a pipeline added later, is watched only after a restart.
- A replication pipeline runs models only with `--all` or `--models`. Otherwise, and for a quality or snapshot pipeline, only an edit to `rocky.toml` re-runs it. A load pipeline's `source_dir` is not watched. At startup, `--watch` prints a line saying so.

Atomic-rename saves (vim's `:w`, VSCode) trigger correctly on macOS. Banner lines go to `stderr`. With `--output json`, each iteration emits one `RunOutput` object on `stdout` (newline-delimited).

### Related Commands

- [`rocky plan`](#rocky-plan) -- preview SQL before execution
- [`rocky state`](#rocky-state) -- inspect watermarks after a run
- [`rocky branch`](#rocky-branch) -- manage named branches
- [`rocky history`](/reference/commands/administration/#rocky-history) -- view past runs

---

## `rocky state`

Show the [watermarks](/reference/glossary/) stored in the embedded state file. A watermark is the newest source value Rocky has already loaded for a table, so the next run knows where to resume. The output lists every tracked table, its last watermark value, and the time Rocky recorded it.

```bash
rocky state
rocky state show      # the same, named explicitly
```

`rocky state` also has maintenance subcommands: `reconcile-watermark`, `clear-schema-cache`, `retention sweep` and `schedule`. See [`rocky state` in Administration](/reference/commands/administration/#rocky-state).

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
    }
  ]
}
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

A table with no rows, or with only NULL load times, counts as never loaded. The worst threshold it declares applies. A model's `warn_after` is its warn threshold. Its `error_after` is its error threshold.

**Exit code.** `1` when any check is `error` or `runtime_error`. `0` otherwise, including when checks only `warn`.

`rocky run` does not call this command. To stop a run on stale sources, run `rocky freshness` first and stop on a non-zero exit.

A model can inherit `time_column` from `_defaults.toml` or the project `[freshness]` block. When Rocky cannot read an inherited column on that model, it measures the last successful build instead and says so in `message`. A `time_column` in the model's own sidecar gets no fallback: a failed read is `runtime_error`.

**Limits.**

- Run history records a model build under its bare target table name. Two models whose targets share a name in different schemas share history, so for a model with no `time_column` the newer build can hide a stale one. Set a `time_column`.
- With `[state] namespacing = "pipeline"`, the state-store fallback reads the global state file and may find no build.
- Exit `1` covers a stale check and a command failure (such as a config that does not load). A command failure prints no JSON report.
- A load time with no time zone is read as UTC. A SQL Server `DATETIME` filled with `GETDATE()` is off by the zone offset: fill it with `SYSUTCDATETIME()` or use `DATETIMEOFFSET`.
- Snowflake temporal cells arrive as epoch numbers and are decoded as such, following the documented SQL API format. This is not yet verified against a live account.

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

Manage named virtual branches. A branch is the persistent, named form of shadow mode. Creating one records a `schema_prefix` in the state store. Every later `rocky plan --branch <name>` or `rocky run --branch <name>` applies that prefix to each model target. Branches behave the same on every adapter.

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

**Target names.** `branch promote` quotes each name in the strategy's `CREATE OR REPLACE TABLE` or `VIEW` statement. Quoting is not escaping, so promote refuses a name that contains the warehouse's identifier quote and names the character.

| Warehouse | Identifier quote | Also refused |
|---|---|---|
| DuckDB, Snowflake, Trino | `"` | |
| Databricks | `` ` `` | |
| BigQuery | `` ` `` | `\` (BigQuery reads escape sequences, so a trailing backslash consumes the closing quote) |

A hyphen or a dot passes this check. Two limits sit outside it. A transformation model that takes its schema from its group's `schema_template` must follow `[A-Za-z0-9_]`, and a hyphen or dot is refused while Rocky builds the plan. Promote also refuses a plan in which two steps replace the same production table. The check runs when a promote plan is built and when it is applied.

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

### `branch promote` flags

> The canonical form is [`rocky plan promote <name>`](#rocky-plan-promote) followed by `rocky apply <plan-id>` (or `rocky branch promote <name> --plan <plan-id>`). The bare `rocky branch promote <name>` form still works as an alias. It prints a one-line `[deprecated]` notice to stderr, which `ROCKY_SUPPRESS_DEPRECATION=1` silences.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--plan <plan-id>` | `string` | (none) | Execute a promote plan written by `rocky plan promote`. The gates already ran at plan time. |
| `--allow-breaking` | flag | off | Bypass the semantic breaking-change gate. Always emits a `breaking_changes_allowed` audit event so the override leaves a paper trail. |
| `--allow-failing-tests` | flag | off | Bypass the failing-tests gate described under [`rocky plan promote`](#the-failing-tests-gate). Always emits a `failing_tests_allowed` audit event. |
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

Create, list, run against, and delete a branch (`rocky branch list` returns `total` and `branches`):

```bash
rocky branch create fix_price --description "testing reprice migration"
```

```json
{
  "version": "1.11.0",
  "command": "branch create",
  "branch": { "name": "fix_price", "schema_prefix": "branch__fix_price", "created_by": "alice", "created_at": "2026-04-20T14:22:11+00:00", "description": "testing reprice migration" }
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

This is `rocky compare` pointed at the branch's `schema_prefix`, the same mechanism `rocky run --branch` uses for writes.

When Rocky confirms that production has no target, it reports `verdict: "no_baseline"`. That target does not fail the run. An uncertain metadata read reports `verdict: "error"` and fails the command. Its unreadable row count is `null`, never `0`. See [`rocky compare`](/reference/cli/#rocky-compare).

### Related Commands

- [`rocky run`](#rocky-run) -- execute a pipeline against a branch via `rocky run --branch`
- [`rocky compare`](/reference/cli/#rocky-compare) -- diff an ad-hoc shadow against production (the generic form `rocky branch compare` specialises)
