---
title: Glossary
description: Plain definitions of the terms Rocky uses.
sidebar:
  order: 7
---

Short definitions of the terms that show up across Rocky's docs, CLI output, and config. Each links to the page that covers it in depth.

### Adapter

A plugin that connects Rocky to a system. Source adapters (Fivetran, Airbyte, DuckDB, Iceberg, manual) discover what tables exist. Warehouse adapters (Databricks, Snowflake, BigQuery, Trino, DuckDB) run the SQL. The core engine stays warehouse-agnostic. See [Adapters](/concepts/adapters/).

### Apply

The execution half of the plan/apply workflow. `rocky apply <plan_id>` runs a plan that was already built and reviewed: it creates schemas, applies drift, copies or materializes data, and runs checks. See [Plan](#plan) and the [core pipeline commands](/reference/commands/core-pipeline/).

### Branch

A named, isolated copy of your pipeline's output, written to its own schema. You develop and run against a branch, inspect the result, then promote it or drop it. Nothing touches production until you promote. See [Branches and replay](/getting-started/roadmap/) and the `06-branches-replay-lineage` POC.

### Bronze layer

The raw replication layer: a config-driven 1:1 copy of source tables into the warehouse, with no SQL to write. Defined by a `[pipeline]` of `type = "replication"`. See [Bronze layer](/concepts/bronze-layer/).

### Check

A data-quality assertion that runs inline during a run (row counts, column match, freshness, custom SQL), not as a separate test step. See [Data quality checks](/concepts/data-quality-checks/).

### Compile-time contract

A schema agreement Rocky enforces before any row is written. A missing required column or an unsafe type change becomes a compile error (`E010`, `E013`) that blocks the PR. See [Testing and contracts](/concepts/testing/).

### Config group

A `models/groups/<name>.toml` definition that a fan-out of models opts into by name (`group = "<name>"` in the sidecar). The group supplies shared routing (a `schema_template` filled per model from its `[args]`), a shared `strategy`, and shared `[tags]`. Precedence is per-model sidecar over group over `_defaults.toml`, so a model can still override what the group sets unless the group is enforced. See [Config groups](/reference/model-format/#config-groups) and [Enforced group](#enforced-group).

### Content-addressed

Identified by the hash of its contents rather than a name or timestamp. Rocky records each run's inputs, code, and outputs this way, which is what lets [replay](#replay) verify a past run against its record. See [Content-addressed writes](/concepts/content-addressed/).

### Diagnostic code

A stable identifier for a compiler finding: errors (`E###`), warnings (`W###`), portability lints (`P###`), and validation diagnostics (`V###`). Codes are searchable and map to a fix. See the [compiler](/concepts/compiler/).

### Drift

A mismatch between what your code expects and what the warehouse actually has, usually because a source column changed type or was added or dropped. Rocky detects it on every run and either recreates the target or blocks the PR. See [Schema drift](/concepts/schema-drift/).

### Enforced group

A [config group](#config-group) with `enforce = true`. The group's fields become binding rather than defaults: a member model that locally pins a field the group controls (its target `schema` or its `strategy`) fails the load instead of quietly routing or materializing itself differently from the rest of the group. Enforcement is opt-in; without it, groups stay overridable defaults. See [Enforced groups](/reference/model-format/#enforced-groups).

### IR (intermediate representation)

The typed graph the compiler builds from your models before it generates SQL. Every transformation in Rocky runs through one IR, which is where types, lineage, and drift checks live. See [Architecture](/concepts/architecture/).

### Lineage

The map of which columns feed which, traced through every transformation at compile time. `rocky lineage-diff` reports the per-column downstream blast radius of a change for PR review. See the [compiler](/concepts/compiler/).

### Materialization strategy

How a model's output lands in the warehouse: `view`, `table`, `incremental`, `merge`, and others. Set per model. See [Model format](/reference/model-format/).

### Model

A single transformation: a `.sql` file (plus an optional `.toml` sidecar) or a `.rocky` DSL file that produces one table or view. See [Silver layer](/concepts/silver-layer/).

### Pipeline

A unit of work declared in `rocky.toml`. Rocky has four types: `replication` (bronze copy), `transformation` (SQL models), `quality` (standalone checks), and `snapshot` (SCD2 history). See [Configuration](/reference/configuration/).

### Plan

A deterministic, reviewable record of what a run will do: compiled SQL, drift actions, and checks, keyed by a `plan_id`. Build it with `rocky plan`, inspect it, then `rocky apply` it. The two steps are the auditable path for production and PR gating; `rocky run` does both at once for local work. See the [core pipeline commands](/reference/commands/core-pipeline/).

### Replay

Inspecting and verifying a past run against its [content-addressed](#content-addressed) record: per-model SQL hashes, row counts, and bytes. `rocky replay <run_id>` checks the record against the ledger; re-executing a run bit-for-bit from the pinned record is on the roadmap. See [Roadmap](/getting-started/roadmap/).

### Shadow mode

Running a changed model alongside the current one and comparing the output, so you see what a change does to the data before you ship it. See [Shadow mode](/concepts/shadow-mode/).

### Silver layer

The transformation layer: SQL (or `.rocky` DSL) models that build on the bronze copy. Defined by a `[pipeline]` of `type = "transformation"`. See [Silver layer](/concepts/silver-layer/).

### State store

The embedded database (`redb`) where Rocky keeps run records, watermarks, and plans, with optional S3 or Valkey sync. There is no `manifest.json`. See [State management](/concepts/state-management/).

### Surrogate key

A computed column whose value is a deterministic MD5 hash over a set of input columns, declared in a `[[surrogate_key]]` sidecar block (`name` plus `columns`). Modeled on dbt-utils' `generate_surrogate_key`: each input is coerced to text and NULL-coalesced to a fixed sentinel before hashing, so on a given warehouse the output matches dbt-utils over the same columns. Rocky injects the hash into the model's SELECT at `rocky run` and on the emit-SQL path, so you don't hand-write it. See [`[[surrogate_key]]`](/reference/model-format/#surrogate_key).

### Trust plane

Rocky's role in your stack: the layer that owns the graph between your code and your data (types, lineage, drift, cost, contracts, governance) while storage and compute stay in your warehouse.

### Watermark

The high-water mark an incremental load stores so the next run only reads new rows (`INSERT … WHERE timestamp > watermark`). Kept in the [state store](#state-store). See [Incremental loads](/concepts/incremental/).
