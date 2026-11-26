---
title: Roadmap
description: What Rocky ships today, what is still Beta, and what an integration handles instead.
sidebar:
  order: 4
---

The trust primitives (compiler, branches, run records, lineage, contracts, cost
attribution) are GA (live-tested) on Databricks.

**GA (live-tested)** means two things. The adapter has a live test suite that
runs against a real warehouse. And each feature in that section runs end to end
on it. It is a maturity grade for the code, not a claim about who uses Rocky.
**Beta** means the core run loop works, but conformance coverage is not complete.

## Shipped and GA (live-tested)

| Area | What ships |
|---|---|
| Databricks | The production target: SQL Statement API, Unity Catalog, OAuth M2M, adaptive concurrency, schema-prefix branches. `rocky preview create` copies tables with a metadata-only `SHALLOW CLONE`. |
| Compiler | Typed column-level inference, diagnostic codes and the language server. They behave the same in CI and in your editor. |
| Branches and run records | A named branch is an isolated schema. Each run writes a content-addressed record. `rocky replay --execute --verify` re-runs a deterministic model locally on DuckDB and reproduces the output bit for bit. `--warehouse` re-runs it in an isolated replay schema instead. |
| Cost attribution | Per-model cost on every run record. `[budget]` blocks fail a run on overspend. |
| AI compile-validate loop | `rocky ai` generates a model, compiles it and auto-fixes parse errors before it lands. |
| Surrogate keys | [`[[surrogate_key]]`](/reference/model-format/#surrogate_key) injects a dialect-correct hash column on DuckDB, Databricks, Snowflake and BigQuery. The value matches `dbt_utils.generate_surrogate_key` on the same warehouse. |
| Tests | Named tests ([`[[use_test]]`](/reference/model-format/#use_test)), inline [`[[tests]]`](/reference/model-format/#tests) and fixture-driven [`[[test]]`](/reference/model-format/#test) unit tests run in DuckDB with no warehouse. See [data quality checks](/concepts/data-quality-checks/#reusable-named-tests). |
| Config groups and tags | A [config group](/reference/model-format/#config-groups) fans shared routing and strategy out to its members. `enforce = true` makes it binding. [`[tags]`](/reference/model-format/#tags) carry governance attributes onto `rocky compile --output json`. |
| `rocky emit-sql` | Renders each transformation model as plain SQL, offline. Adopting Rocky is never a one-way door. See [No lock-in](/guides/no-lock-in/). |
| Agent policy plane | `[policy]` grades what a principal may do by capability and scope: allow, require review, or deny. It runs at `apply`, at `promote` and on the MCP write tools. Every decision goes to the ledger. `rocky policy freeze` is the kill switch. See [Operating Rocky with agents](/concepts/operating-rocky-with-agents/) and [Testing policies](/guides/testing-policies/). |
| Self-healing | A proven-transient failure is retried within a bounded budget, on by default. Containment withholds a failed model's downstream and builds the disjoint subgraphs. Auto-apply covers only provably additive drift, behind an opt-in and a policy grant. A backfill is always a review-gated plan. |
| Reclamation with proof | `rocky gc --derivable` evicts artifacts whose recorded recipe is bound to their exact bytes, behind review, and leaves a tombstone. `rocky restore` reinstates one only when the rebuilt bytes match the recorded hash. Restore covers unpartitioned recipes that read no recorded upstreams, and can still refuse on live state. |
| Governance surface | `rocky brief`, `rocky audit --for <table\|run\|plan>`, `rocky audit --scorecard` and `rocky review --queue`. A signal the ledger does not hold renders as *not recorded*. |
| Browser UI | `rocky serve --ui` shows the recorded data. It is read-only, or can make changes in operator mode on your own machine. See the [browser UI guide](/guides/browser-ui/). |
| Embeddable engine | `rocky serve` exposes `/api/v1` with outputs byte-identical to `--output json`, an async job model and a generated OpenAPI 3.1 document. See the [Embedding guide](/guides/embedding/). `rocky mcp` serves 31 tools. The [recipe-manifest spec](/reference/rocky-manifest-spec/) and the `rocky-verify` binary check a manifest offline. |

The custody ledger is best-effort: a failed ledger write warns and lets the
operation continue, and a refusal issued before the rules are evaluated writes
no row.

## Beta

- **Snowflake, BigQuery and Trino.** Connection, execution and the core run loop
  work. Conformance coverage is still growing. If you need one of them at GA,
  [open a discussion](https://github.com/rocky-data/rocky/discussions).
- **PostgreSQL, Redshift, ClickHouse, SQL Server and Apache Spark.**
  PostgreSQL, ClickHouse, SQL Server 2022 and Spark are tested against a live
  server. Redshift, Azure SQL and Fabric are tested by generated SQL only.
  Not every strategy runs on every warehouse. See
  [Connect a warehouse](/guides/connect-a-warehouse/).
- **Iceberg.** REST-catalog source discovery works. Content-addressed writes
  round-trip as Iceberg through Delta UniForm.

## Handled by an integration, not built in

- **Extraction from SaaS sources.** Use Fivetran, Airbyte, Stitch or
  warehouse-native CDC. Rocky discovers what they land.
- **Orchestration beyond Dagster.** Dagster has a native integration, and
  `rocky serve` covers small standalone teams. Airflow and Prefect call the
  `rocky` binary like any other CLI.
- **A semantic layer.** Use Cube, the dbt Semantic Layer or your metric store.

## Tell us where it hurts

If a gap blocks your team,
[open a discussion](https://github.com/rocky-data/rocky/discussions).
