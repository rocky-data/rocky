---
title: Feature Comparison
description: How Rocky's features line up against SQLMesh, Coalesce, and Dataform, feature by feature.
sidebar:
  order: 4.5
---

This page lists what Rocky does next to what SQLMesh, Coalesce, and Dataform do.
Features verified against official documentation and source code as of July
2026. Read the tables to find one feature. Read the notes under a table for the
detail a Yes/No cell cannot carry.

## How Rocky relates to the tools around it

**SQLMesh.** SQLMesh is the tool Rocky most resembles. It also analyzes SQL
statically, using SQLGlot, with no templating layer. SQLMesh pioneered several
primitives Rocky shares: virtual data environments, plan/apply with
breaking-change classification, and column-level lineage. Rocky does not claim
those as differentiators.

Rocky differs on the enforcement plane. It ships declarative governance in the
open-source build (roles, masking, and classification, deepest on Databricks).
It ships `[budget]` blocks that fail the build on overspend. Neither is in
SQLMesh OSS. Rocky also detects source-schema drift, the case a code-diff plan
cannot see. A column type changes in the warehouse, under a model that is
already materialized. And `P001` flags warehouse-specific SQL at PR time, where
SQLMesh instead transpiles between dialects with SQLGlot. That is a different
bet, not a worse one.

Be clear about where SQLMesh leads. It has more years, more funding, and more
adoption. It ships native Python models and an open-source CI/CD bot. Its
virtual environments are more proven than Rocky's schema-prefix branches. Rocky
keeps SQL as the default surface; SQLMesh leans Python-first.

**Warehouse-native pipelines (Databricks LakeFlow, Snowflake Dynamic Tables).**
These are coupled to one warehouse and come free with the platform. Rocky stays
warehouse-neutral and ships a compiler. If portability and tooling matter to
you, Rocky is the better fit. If they do not, the warehouse-native option may be
enough. Adopting Rocky is not a one-way door either: `rocky emit-sql` reduces
your transformation models to plain runnable SQL in dependency order, so leaving
is one command rather than a rewrite. See [No lock-in](/guides/no-lock-in/).

**Observability tools (Datafold, Monte Carlo, Anomalo).** These are not
competitors. Rocky prevents what they detect. They stay useful for the failure
modes Rocky does not model. Integrate them, do not replace them.

## Architecture

| Feature | Rocky | SQLMesh | Coalesce | Dataform |
|---|---|---|---|---|
| **Language** | Rust | Python (SQLGlot) | TypeScript | TypeScript |
| **Open source** | Apache 2.0 | Apache 2.0 (LF) | No (SaaS) | Partial |
| **Distribution** | Binary | pip | Cloud SaaS | GCP managed |
| **Config format** | TOML | YAML + Python | GUI | SQLX |
| **Manifest** | None (in-memory) | Snapshots | Cloud | Cloud |

## Warehouse Support

| Warehouse | Rocky | SQLMesh | Coalesce | Dataform |
|---|:---:|:---:|:---:|:---:|
| Databricks | **Yes** | Yes | Yes | No |
| Snowflake | Beta | Yes | Yes | No |
| BigQuery | Beta | Yes | Planned | **Yes** |
| Trino | Beta | Yes | No | No |
| DuckDB | **Yes** | Yes | No | No |
| Redshift | Planned | Yes | Planned | No |
| PostgreSQL | Planned | Yes | No | No |

## Materialization Strategies

A materialization strategy decides how Rocky writes a model's rows: replace the
table, append to it, merge into it, or something else. See
[glossary](/reference/glossary/).

| Strategy | Rocky | SQLMesh | Coalesce | Dataform |
|---|:---:|:---:|:---:|:---:|
| Table (full refresh) | Yes | Yes | Yes | Yes |
| View | Yes | Yes | Yes | Yes |
| Incremental (append) | Yes | Yes | Yes | Yes |
| Merge (upsert) | Yes | Yes | Yes | No |
| Snapshot (SCD2) | Yes | Yes | Yes | No |
| Materialized View | Yes | No | No | Yes |
| Dynamic Table | **Yes** | No | No | No |
| Time Interval | **Yes** | No | No | No |
| Ephemeral (CTE) | Yes | No | No | No |
| Microbatch | Yes | No | No | No |
| Delete+Insert | Yes | No | No | No |

**Rocky-unique:** Time Interval materialization runs one execution per
partition. `--lookback` re-reads recent partitions for late-arriving data,
`--missing` finds gaps, and `--parallel N` processes partitions concurrently.
Dynamic Tables (Snowflake) refresh on a declared lag.

## Type Checking & Compilation

| Feature | Rocky | SQLMesh |
|---|:---:|:---:|
| Static type inference | **Yes** | Yes |
| Column type tracking | **Yes** | Yes |
| Compile-time diagnostics | **35+** | Partial |
| Safe type widening | **Yes** | No |
| NULL-safe equality | **Yes** | No |
| Data contracts | **Yes** | Yes |
| SELECT * expansion | **Yes** | Yes |
| Parallel type checking | **Yes** | No |

**Rocky:** a contract (a declared promise about a model's columns and their
types) holds inside a project and across teams. `rocky publish-ir` writes a
snapshot of one project's compiled types to a JSON file. A consuming project
vendors that file and points at it from an `[imports.<name>]` block. The
consumer's own `rocky compile` then fails when the producer breaks a column the
consumer reads: `E030` for a dropped column, `E031` for a narrowed type, `E032`
for a nullability change. `rocky imports update` is how a consumer accepts a
reviewed change.

## Column-Level Lineage

Lineage traces where each output column came from, through every transformation
between the source and the model.

| Feature | Rocky | SQLMesh |
|---|:---:|:---:|
| Column-level lineage | **Yes** | Yes |
| CLI-accessible | **Yes** | Yes |
| Graphviz export | **Yes** | No |
| Compile-time | **Yes** | Yes |

## Schema Drift Detection

Drift is a change to a source table that nobody declared: a new column, a
dropped column, or a type that widened. Rocky detects drift automatically at
run time, widens types where widening is safe, and grades its response to the
severity of the change. Shadow mode runs the new shape beside the old one so you
can compare before you commit. SQLMesh ships none of these four.

## IDE / Language Server

| Feature | Rocky | SQLMesh |
|---|:---:|:---:|
| LSP | **Yes** | Preview |
| VS Code extension | **Yes** | Preview |
| Go-to-definition | **Yes** | Yes |
| Find references | **Yes** | No |
| Hover | **Yes** | Yes |
| Completions | **Yes** | Yes |
| Code actions | **Yes** | No |
| Inlay hints | **Yes** | No |
| Rename | **Yes** | No |
| Signature help | **Yes** | No |
| Diagnostics (live) | **Yes** | Partial |

## Orchestration

| Feature | Rocky | SQLMesh |
|---|:---:|:---:|
| Dagster | **Native** | Community (dagster-sqlmesh) |
| Airflow | Via CLI | Yes |
| Dagster Pipes protocol | **Yes** | No |
| Typed output models | **Yes** (87 schemas) | No |

## Data Quality

| Check | Rocky | SQLMesh |
|---|:---:|:---:|
| Row count | **Yes** | Partial |
| Column match | **Yes** | No |
| Freshness | **Yes** | No |
| Null rate (TABLESAMPLE) | **Yes** | No |
| Custom SQL | **Yes** | Yes |
| Anomaly detection | **Yes** | No |
| Inline (not separate step) | **Yes** | No |
| Unit tests / fixture-driven tests | **Yes** | Yes |
| Reusable named / generic tests | **Yes** | Yes (audits) |

**Rocky:** two kinds of test ship, and neither is bolted on.
[Unit tests](/concepts/testing/) put mock input rows under a `[[test]]` block,
assert on the output rows, and run locally on DuckDB with plain `rocky test`.
[Reusable named tests](/concepts/data-quality-checks/#reusable-named-tests) let
you define an assertion once in `models/test_definitions.toml` and apply it by
name with `[[use_test]]`. References resolve when the project loads, so a typo
fails the load instead of silently dropping a check. Named tests run against
the warehouse with `rocky test --declarative`. See the
[`[[test]]`](/reference/model-format/#test) and
[`[[use_test]]`](/reference/model-format/#use_test) blocks in the model format.

## Modeling utilities

Rocky generates surrogate keys; SQLMesh does not. A
[`[[surrogate_key]]`](/reference/model-format/#surrogate_key) block injects a
deterministic hash column at materialization, written in dialect-correct SQL for
each warehouse, so you never hand-write the hash.

On a given warehouse the value is identical to what
`dbt_utils.generate_surrogate_key` produces over the same columns, because NULL
inputs coalesce to the same sentinel. A Rocky key therefore joins against the
matching key in an upstream dbt model, and it survives a migration in either
direction.

## Governance

Rocky manages the warehouse objects around your models, which SQLMesh leaves to
you. It creates and lifecycles catalogs, declares roles and reconciles
GRANT/REVOKE to match, isolates workspaces, and supports multi-tenant layouts.
SQLMesh ships none of these five.

## Provenance & Agent Governance

Rocky records what produced every table and controls what an agent may change.
Each run writes a content-addressed record: the artifacts are keyed by the hash
of their bytes. From that record, Rocky replays a deterministic model
bit-for-bit. It restores an evicted artifact only when the rebuilt bytes match
the recorded hash. A third party can verify a manifest offline, with no engine
installed. The policy plane grades an agent's proposed change as allow, require
review, or deny; `rocky policy test` runs pinned scenarios in CI so a loosened
rule fails the build. `rocky audit`, `rocky brief`, and the review queue are the
custody trail. SQLMesh ships none of these seven.

One honest boundary. The warehouse platforms govern the adjacent layer:
Databricks Unity AI Gateway and Snowflake Agent Identity control which systems
an agent may *access* at run time. Rocky governs what an agent may *change* in
the transformation program. Those are different questions, and the two layers
compose rather than compete.

## AI Features

| Feature | Rocky | SQLMesh | Coalesce |
|---|:---:|:---:|:---:|
| Model generation | **Yes** | No | Copilot |
| Schema sync | **Yes** | No | No |
| Code explanation | **Yes** | No | No |
| Test generation | **Yes** | No | No |
| Agent authoring surface | **MCP (30 tools)** | No | No |

## CLI Commands

| Command | Rocky | SQLMesh |
|---|:---:|:---:|
| Init / compile / run / test | Yes | Yes |
| Source discovery | **Yes** | No |
| Schema drift check | **Yes** | No |
| Cost analysis | **Yes** | No |
| AI generation | **Yes** | No |
| dbt migration | **Yes** | Yes |
| Migration validation | **Yes** | No |
| Shadow comparison | **Yes** | No |
| Quality metrics + trends | **Yes** | No |
| Storage profiling | **Yes** | No |
| Partition archival | **Yes** | No |
| Table compaction | **Yes** | No |
| Benchmarks | **Yes** | No |
| HTTP API / LSP | **Yes** | Yes |
| Hook management | **Yes** | No |
| **Total** | **65+** | ~20 |

## Performance

Rocky compiles 10,000 models in about one second and holds peak memory near
150 MB. See [benchmarks](/getting-started/benchmarks/) for the full numbers, the
methodology, and the commands to reproduce them.

## Where Each Tool Excels

| Tool | Best for |
|---|---|
| **Rocky** | Production-critical, multi-tenant pipelines where a silent failure costs real money. Databricks-first; Snowflake and BigQuery are Beta. Compile-time contracts, column-level lineage at PR time, branches plus replay, and per-model cost attribution. |
| **SQLMesh** | Teams that want correctness checks at the planner level, Python-first ergonomics, and virtual environments. The closest tool to Rocky in intent: both analyze SQL statically and ship column-level lineage and branch-style environments. Rocky differs on the enforcement plane, and defaults to SQL. |
| **Coalesce** | Visual, low-code transformation for Snowflake-first organizations with less technical analysts. A different buyer than Rocky. |
| **Dataform** | BigQuery-only shops that want tight GCP integration with minimal tooling. |
