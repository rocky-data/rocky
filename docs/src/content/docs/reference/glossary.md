---
title: Glossary
description: Plain definitions of the terms Rocky uses.
sidebar:
  order: 7
---

Short definitions of the terms that show up across Rocky's docs, CLI output, and config. Each links to the page that covers it in depth.

### Adapter

A plugin that connects Rocky to a system. Source adapters (Fivetran, Airbyte, DuckDB, Iceberg, BigQuery, manual) discover what tables exist. Warehouse adapters (Databricks, Snowflake, BigQuery, Trino, DuckDB) run the SQL. The core engine stays warehouse-agnostic. See [Adapters](/concepts/adapters/).

### Apply

The execution half of the plan/apply workflow. `rocky apply <plan_id>` runs a plan that was already built and reviewed: it creates schemas, applies drift, copies or materializes data, and runs checks. See [Plan](#plan) and the [core pipeline commands](/reference/commands/core-pipeline/).

### Backfill

Rebuilding a model over history rather than over the newest rows. `rocky backfill` rebuilds the models you name plus everything downstream of them, and applies a partition window to partitioned models. The plan it writes is always review-gated. See the [governance and reclamation commands](/reference/commands/governance-reclamation/).

### Blast radius

The set of downstream models a change can reach. Rocky computes it from [lineage](#lineage), so you see the cost of an edit before you make it. `rocky lineage-diff` reports it per column, and the `P002` lint warns when a `SELECT *` widens it. A `[policy]` rule can cap it with `max_downstreams`.

### Branch

A named, isolated copy of your pipeline's output, written to its own schema. You develop and run against a branch, inspect the result, then promote it or drop it. Nothing touches production until you promote. See [Branches and replay](/getting-started/roadmap/) and the `06-branches-replay-lineage` POC.

### Bronze layer

The raw replication layer: a config-driven 1:1 copy of source tables into the warehouse, with no SQL to write. Defined by a `[pipeline]` of `type = "replication"`. See [Bronze layer](/concepts/bronze-layer/).

### Canonical

One agreed written form for a value, so two things that mean the same produce the same bytes. Rocky canonicalizes a model's compiled [IR](#ir-intermediate-representation) before hashing it, which is what makes a [fingerprint](#fingerprint) comparable across machines and runs.

### Check

A data-quality assertion that runs inline during a run (row counts, column match, freshness, custom SQL), not as a separate test step. See [Data quality checks](/concepts/data-quality-checks/).

### Compile-time contract

A schema agreement Rocky enforces before any row is written. A missing required column or an unsafe type change becomes a compile error (`E010`, `E011`) that blocks the PR. See [Testing and contracts](/concepts/testing/).

### Config group

A `models/groups/<name>.toml` definition that a fan-out of models opts into by name (`group = "<name>"` in the sidecar). The group supplies shared routing (a `schema_template` filled per model from its `[args]`), a shared `strategy`, and shared `[tags]`. Precedence is per-model sidecar over group over `_defaults.toml`, so a model can still override what the group sets unless the group is enforced. See [Config groups](/reference/model-format/#config-groups) and [Enforced group](#enforced-group).

### Conformance

A shared test suite every warehouse adapter must pass, so the engine behaves the same whichever warehouse is underneath. Run it against an adapter with `rocky test-adapter`. See the [adapter SDK guide](/guides/adapter-sdk/).

### Content-addressed

Identified by the hash of its contents rather than a name or timestamp. Rocky records each run's inputs, code, and outputs this way, which is what lets [replay](#replay) verify a past run against its record. See [Content-addressed writes](/concepts/content-addressed/).

### CTE (common table expression)

A named subquery declared with `WITH <name> AS (…)` that the rest of the statement can read. An [ephemeral](#ephemeral) model becomes a CTE inside every model that reads it, instead of becoming its own table.

### DDL (data definition language)

The SQL that changes structure rather than rows: `CREATE`, `ALTER`, `DROP`. Rocky writes the DDL for you from a model's declared shape, so you never hand-maintain a `CREATE TABLE`.

### Declarative

You state the result you want; Rocky works out the statements that get there. A `[[grants]]` block names who should have access, and Rocky derives the `GRANT` and `REVOKE` to match. The opposite is imperative, where you write each step yourself. See [Reconcile](#reconcile).

### Deterministic

Same inputs, same output, every time. Rocky needs determinism for two things: skipping a model that has not changed, and verifying that a [replay](#replay) reproduces a past run. SQL that is not deterministic (`random()`, a bare `current_timestamp()`) is excluded from the skip gate and flagged on replay.

### Diagnostic code

A stable identifier for a compiler finding: errors (`E###`), warnings (`W###`), portability lints (`P###`), and validation diagnostics (`V###`). Codes are searchable and map to a fix. See the [compiler](/concepts/compiler/).

### Digest

A short fixed-length value computed from a much larger input, so two large things can be compared without reading both. Rocky uses BLAKE3 digests of model output to confirm that a replay reproduced the recorded bytes. See [Fingerprint](#fingerprint).

### Drift

A mismatch between what your code expects and what the warehouse actually has, usually because a source column changed type or was added or dropped. Rocky detects it on every run and either recreates the target or blocks the PR. See [Schema drift](/concepts/schema-drift/).

### Dry run

Doing every step except the write. `rocky plan` is the dry run of `rocky apply`: it produces the exact SQL without executing it. `rocky estimate` is a dry-run cost estimate that asks the warehouse to `EXPLAIN` a query rather than run it.

### Enforced group

A [config group](#config-group) with `enforce = true`. The group's fields become binding rather than defaults: a member model that locally pins a field the group controls (its target `schema` or its `strategy`) fails the load instead of quietly routing or materializing itself differently from the rest of the group. Enforcement is opt-in; without it, groups stay overridable defaults. See [Enforced groups](/reference/model-format/#enforced-groups).

### Ephemeral

A model that is never written to the warehouse. Rocky inlines its SQL as a [CTE](#cte-common-table-expression) in every model that reads it, and runs no DDL for it. Set it with `strategy = "ephemeral"`. See [Ephemeral](/reference/model-format/#ephemeral).

### Exit code

The number a command returns to the shell. Rocky uses `0` for success, `1` for a hard failure (bad config, unreachable warehouse), and `2` for partial success, where the run finished but some models failed. `rocky doctor` returns `3` when a check is critical. A partial run still writes valid JSON to stdout.

### Fingerprint

A hash that stands in for a whole definition. Rocky's `recipe_hash` is a BLAKE3 fingerprint of a model's compiled [IR](#ir-intermediate-representation) — its SQL, types, strategy, dependencies, and checks. A whitespace or alias edit changes it, so the model-skip gate compares a separate normalized hash instead. See [Digest](#digest).

### Freshness

How far behind the newest row in a table is. Declare `expected_lag_seconds` and a `time_column`, and Rocky reports a table that has fallen behind. Set it per model, or project-wide under `[freshness]`. See [`[freshness]`](/reference/configuration/#freshness).

### Idempotent

Running the same operation twice leaves the same result as running it once. `rocky run --idempotency-key <key>` uses this: a retry that carries the same key is deduplicated against the first run instead of doing the work again. See [`[state.idempotency]`](/reference/configuration/#stateidempotency).

### IR (intermediate representation)

The typed graph the compiler builds from your models before it generates SQL. Every transformation in Rocky runs through one IR, which is where types, lineage, and drift checks live. See [Architecture](/concepts/architecture/).

### Lineage

The map of which columns feed which, traced through every transformation at compile time. `rocky lineage-diff` reports the per-column downstream blast radius of a change for PR review. See the [compiler](/concepts/compiler/).

### Masking policy

A rule that hides a sensitive column's value. Tag the column with a `[classification]` in its sidecar, then map that classification to a strategy under `[mask]` — `hash`, `redact`, `partial`, or `none`. A `[mask.<env>]` block overrides the strategy per environment. See [`[mask]`](/reference/configuration/#mask).

### Materialization strategy

How a model's output lands in the warehouse: `view`, `table`, `incremental`, `merge`, and others. Set per model. See [Model format](/reference/model-format/).

### MCP (Model Context Protocol)

An open protocol that lets an AI agent call tools. `rocky mcp` serves Rocky's tools over it. The read-only ones let an agent check its work against the real project: compile, plan preview, lineage, test, schema inspection, row sampling. Others write into `models/`: `draft_model`, `draft_contract`, and `draft_check` go through the compiler and your `[policy]` rules. Applying an AI-authored plan still needs human approval. See [MCP authoring](/concepts/mcp-authoring/).

### MERGE

A [materialization strategy](#materialization-strategy) that matches incoming rows against the target on a key, updates the ones that already exist, and inserts the ones that do not. Also the name of the SQL statement Rocky generates for it. See [Upsert](#upsert) and [Merge](/reference/model-format/#merge).

### Model

A single transformation: a `.sql` file (plus an optional `.toml` sidecar) or a `.rocky` DSL file that produces one table or view. See [Silver layer](/concepts/silver-layer/).

### Nullable

A column that is allowed to hold `NULL`. Rocky carries nullability alongside the type of every column through the whole graph, so the compiler can tell where a `NULL` can appear rather than discovering it at runtime.

### OTLP (OpenTelemetry Protocol)

The wire format OpenTelemetry uses to ship traces and metrics. Rocky exports over it, so a run is visible in Grafana, Tempo, or any other OTLP backend without Rocky hosting a UI. See [Observability](/guides/observability/).

### Partition

A slice of a table identified by a column value, usually a date or an hour. A partitioned strategy lets Rocky rebuild one slice instead of the whole table, and [`rocky backfill`](#backfill) walks a range of them. See [Time interval](/reference/model-format/#time-interval).

### Pipeline

A unit of work declared in `rocky.toml`. Rocky has four types: `replication` (bronze copy), `transformation` (SQL models), `quality` (standalone checks), and `snapshot` (SCD2 history). See [Configuration](/reference/configuration/).

### Plan

A deterministic, reviewable record of what a run will do: compiled SQL, drift actions, and checks, keyed by a `plan_id`. Build it with `rocky plan`, inspect it, then `rocky apply` it. The two steps are the auditable path for production and PR gating; `rocky run` does both at once for local work. See the [core pipeline commands](/reference/commands/core-pipeline/).

### Plan store

Where Rocky keeps built plans between `rocky plan` and `rocky apply`, so a plan can be reviewed, approved, and applied later. `rocky plan` writes each one to `.rocky/plans/<plan-id>.json`, and `rocky apply` reads it back. See [Plan store v1 to v2](/concepts/plan-store-v1-to-v2/).

### Provenance

The record of where a result came from: which SQL produced it, which inputs it read, and which run wrote it. `rocky replay --execute` rebuilds a recipe from provenance rather than from your working tree, which is what makes the replay an independent check. See [Verify a run](/guides/verify-a-run/).

### Quarantine

Separating a model's failing rows from its passing ones, so bad data does not block the run or reach downstream readers. Turn it on under `[pipeline.<name>.checks.quarantine]` and pick a mode: split the rows into separate tables, tag them in place, or drop them. See [`[pipeline.NAME.checks]`](/reference/configuration/#pipelinenamechecks).

### Reconcile

Reading what the warehouse actually has, comparing it against what you declared, and issuing only the statements that close the gap. Rocky reconciles permissions this way: it reads the current grants, diffs them against your config, and emits just the `GRANT` and `REVOKE` it needs. See [Permissions](/reference/permissions/).

### Replay

Inspecting, auditing, and re-executing a past run against its [content-addressed](#content-addressed) record. `rocky replay <run_id>` surfaces per-model SQL hashes, row counts, and bytes; `rocky replay <run_id> --execute --verify` reconstructs each recipe from its provenance and re-runs it to reproduce the recorded output bit-for-bit, on a local DuckDB engine or, with `--warehouse`, on the live warehouse in an isolated replay schema. Re-execution covers deterministic content-addressed models; mutable-source models are classified `non_replayable` and non-deterministic recipes are flagged. See [Roadmap](/getting-started/roadmap/).

### SCD (slowly changing dimension)

A table that keeps a row's history instead of overwriting it. A `snapshot` pipeline builds SCD Type 2 history: each change closes the old version and opens a new one, tracked in `valid_from`, `valid_to`, and `is_current` columns. You can then query the table as it stood on any past date. See [`rocky snapshot`](/reference/cli/#rocky-snapshot).

### Schema evolution

Changing a table's columns over time without breaking the models that read it. When a source column disappears, Rocky can keep it in the target for a grace period and fill it with `NULL`, so consumers have time to adapt. See [`[schema_evolution]`](/reference/configuration/#schema_evolution).

### Seam

A point where Rocky enforces a decision rather than only computing one. The mutating seams are `rocky apply`, branch promote, and the MCP write tools — those are exactly the points the `[policy]` plane gates. See [Operating Rocky with agents](/concepts/operating-rocky-with-agents/).

### Shadow mode

Running a changed model alongside the current one and comparing the output, so you see what a change does to the data before you ship it. See [Shadow mode](/concepts/shadow-mode/).

### Sidecar

The `<model>.toml` file that sits next to `<model>.sql` and carries everything that is not SQL: the target table, the strategy, tags, tests, and column declarations. The `.sql` file stays plain SQL that any tool can read. See [Sidecar format](/reference/model-format/).

### Silver layer

The transformation layer: SQL (or `.rocky` DSL) models that build on the bronze copy. Defined by a `[pipeline]` of `type = "transformation"`. See [Silver layer](/concepts/silver-layer/).

### State store

The embedded database (`redb`) where Rocky keeps run records, watermarks, and plans, with optional S3 or Valkey sync. There is no `manifest.json`. See [State management](/concepts/state-management/).

### Structured-error envelope

A machine-readable failure body instead of a bare message. Every `rocky serve` route returns one on failure, carrying a stable `code`, a human `message`, and an optional `remediation_hint`, so a caller can branch on the code and show the hint to an operator. See [Embedding Rocky](/guides/embedding/).

### Surrogate key

A computed column whose value is a deterministic MD5 hash over a set of input columns, declared in a `[[surrogate_key]]` sidecar block. Compatible with dbt-utils' `generate_surrogate_key`, and injected into the model's SELECT automatically. See [`[[surrogate_key]]`](/reference/model-format/#surrogate_key).

### Topological order

An ordering of the DAG in which every model comes after the models it reads. Rocky computes it before a run, then groups models with no dependency between them into layers it can run in parallel. See [Architecture](/concepts/architecture/).

### Trust plane

Rocky's role in your stack: the layer that owns the graph between your code and your data (types, lineage, drift, cost, contracts, governance) while storage and compute stay in your warehouse.

### Upsert

Update the row when its key already exists, insert it when it does not. This is what the [`merge`](#merge) strategy does on every run.

### Watermark

The high-water mark an incremental load stores so the next run only reads new rows (`INSERT … WHERE timestamp > watermark`). Kept in the [state store](#state-store). See [Incremental loads](/concepts/incremental/).

### Zero-copy

Pointing a second table at bytes that already exist instead of copying them. Rocky uses it for branch tables where the warehouse supports it — Databricks `SHALLOW CLONE` is metadata-only — and falls back to a real copy where it does not. See [Preview internals](/concepts/preview-internals/).
