# Rocky

**Rocky is the typed graph between your code and whichever warehouse, table format, or query engine you've chosen.** A typed compiler that sits above Databricks, Snowflake, BigQuery, or DuckDB and owns the graph between your code and your data — named branches, deterministic replay, column-level lineage, compile-time contracts, per-model cost attribution. The trust plane for your data; a single static Rust binary; storage and compute stay where they are.

**Rocky is not a warehouse, not a table format, not a query engine, not a templating layer.** It's a real compiler with type inference, diagnostic codes, and an IDE — so the failures that quietly cost data teams the most (silent schema drift, column rename blast radius, dialect divergence, cost spikes nobody can attribute) become compile errors and blocked PRs, not pages and post-mortems.

No Jinja. No manifest. No parse step.

## Why Rocky exists

The expensive failures in modern data platforms aren't slow queries. They're trust failures:

- A source column type changes upstream and a revenue dashboard quietly diverges for three days.
- An engineer renames a column on `stg_orders` and 47 downstream models break in production.
- A `SELECT *` pulls a new column nobody designed for; a downstream join silently double-counts.
- A Snowflake-only function lands in a Databricks-targeted project and only fails in prod.
- Warehouse spend doubles in a month and nobody can attribute which model caused it.
- An auditor asks who changed `fct_revenue.amount`, when, and why — and the answer involves `git blame` and screenshots.

dbt Core, by design, is a templating engine — it can't catch any of these at compile time. dbt Fusion (dbt Labs' Rust rewrite of dbt Core, in public beta since 2025-05-28) catches some compile-time issues but still uses Jinja templating (`dbt-jinja` is a Rust port of mini-jinja), and doesn't ship named branches, content-addressed deterministic replay, per-model cost attribution, dialect-portability lint across warehouses, or declarative governance + masking outside dbt platform paid tiers. SQLMesh moved correctness to the planner. Rocky owns the trust dimensions all of them leave open — each failure above maps to a Rocky diagnostic code, a CI gate, or a content-addressed replay artifact.

## Scope on the ELT spectrum

| Stage | Rocky | Notes |
|---|---|---|
| Extract (SaaS sources) | — | Use Fivetran, Airbyte, Stitch, or warehouse-native CDC |
| Extract (files) | ✅ | `rocky load` — CSV / Parquet / JSONL from a directory |
| Load (bronze replication) | ✅ | Config-driven replication pipelines |
| Transform | ✅ | Compiled SQL models |
| Quality | ✅ | Inline assertions during `rocky run` |
| Orchestration | Partial | First-class Dagster integration; `rocky serve` standalone |

## The seven trust dimensions

1. **SQL as a typed, compiled language.** Real type inference, real diagnostic codes (`E001`–`E026`, `W001`–`W011`, `P001`–`P002`), real LSP. Not text macros, not runtime checks — a compiler. This is the moat.
2. **Compile-time column-level lineage.** Rocky knows the lineage of every column before a row is written. Block a PR when a downstream contract breaks. `rocky lineage-diff main` per-changed-column at PR time.
3. **Branches + deterministic replay.** `rocky branch create`, `rocky run --branch`, `rocky replay <run_id>` — branches are isolated schemas, replays are content-addressed artifacts. Inputs + code → outputs is a pure function, recorded.
4. **Per-model cost attribution.** Cost is a column on every run record — not a dashboard you have to opt into. `[budget]` blocks fail the run; `budget_breach` fires the hook; `rocky preview cost` projects spend at PR time.
5. **AI gated through the compiler.** Every AI suggestion goes through type-check before it lands. The `Attempts: 2` retry loop on `rocky ai` is the signature: generate → type-check → auto-fix → land.
6. **Dialect-divergence lint.** Cross-warehouse teams write SQL once; `P001` catches Snowflake-only constructs in a Databricks project at compile time. Useful the day you start a migration; essential the day you finish one.
7. **Declarative governance.** RBAC as code with GRANT/REVOKE diffing, Unity Catalog tags, workspace isolation, masking strategies bound to classification tags. Compliance becomes a CI check, not a quarterly fire drill.

## Quick start

```bash
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
rocky playground my-first-project
cd my-first-project
rocky compile       # Type-check all models
rocky test          # Run assertions locally with DuckDB
rocky run           # Execute the pipeline
```

The playground is self-contained: sample models, contracts, and a DuckDB backend. No credentials needed.

## Features

| Category | Capabilities |
|----------|-------------|
| **Compiler** | Type checking, column-level lineage, data contracts, DAG resolution, diagnostics with suggestions |
| **Branches** | `rocky branch create`/`delete`/`list`/`show`, `rocky run --branch`, `rocky replay <run_id>` |
| **Cost** | Per-model cost attribution on every run, `[budget]` blocks, `budget_breach` hook event |
| **Observability** | `rocky trace` Gantt output, OpenTelemetry OTLP export, structured JSON events |
| **Portability** | Dialect-divergence lint across Databricks / Snowflake / BigQuery / DuckDB |
| **DSL** | Pipeline-oriented `.rocky` syntax — optional, models stay plain SQL by default |
| **AI** | Intent metadata, schema-sync, intent extraction, test generation |
| **IDE** | VS Code extension, full LSP (completion, hover, go-to-def, rename, code actions, inlay hints) |
| **Quality** | Pipeline-level checks + 13 declarative assertions with severity, filters, and row quarantine |
| **Execution** | DuckDB (local), Databricks (prod), Snowflake + BigQuery (beta) |
| **Optimization** | Cost-based materialization, storage profiling, compaction, partition archival |
| **Governance** | Unity Catalog tags, workspace isolation, declarative RBAC with GRANT/REVOKE diffing |
| **Integration** | Dagster ([dagster-rocky](../integrations/dagster/)), dbt import, CI pipeline |

## CLI at a glance

```bash
rocky init           # Scaffold a new project
rocky validate       # Check config without API calls
rocky compile        # Type-check all models
rocky test           # Run assertions locally (DuckDB)
rocky plan           # Preview generated SQL (dry-run)
rocky run            # Execute the pipeline
rocky state          # Inspect stored watermarks
rocky ai "<intent>"  # Generate a model from natural language
rocky lineage        # Trace column-level lineage
rocky lineage-diff   # Per-changed-column downstream blast-radius for PR review
rocky doctor         # Aggregate health checks
rocky serve          # HTTP API + live watch
rocky lsp            # Language Server Protocol for IDEs
```

Full reference: [CLI commands](https://rocky-data.dev/reference/cli/).

## Adapters

| Role | Adapter | Status | Notes |
|------|---------|--------|-------|
| Source | Fivetran | Production | REST API discovery of connectors and tables |
| Source | Airbyte | Beta | Airbyte API discovery of connections and streams |
| Source | Iceberg | Beta | REST catalog discovery of namespaces and tables |
| Source | Manual | Production | Schema/table lists inline in `rocky.toml` |
| Warehouse | Databricks | Production | SQL Statement API + Unity Catalog governance |
| Warehouse | Snowflake | Beta | SQL execution via Snowflake connector |
| Warehouse | BigQuery | Beta | SQL execution via BigQuery connector |
| Warehouse | DuckDB | Local / Testing | Embedded execution for development and CI |
| Warehouse | Trino | Beta | REST `/v1/statement` polling client; Basic + JWT auth; Docker conformance harness behind the `trino-conformance` feature |

Build a custom adapter in Rust or any language: [Adapter SDK guide](https://rocky-data.dev/guides/adapter-sdk/) — walks through a ClickHouse-shaped skeleton, the trait surface, auth, testing, and distribution. Concepts overview: [Adapter SDK](https://rocky-data.dev/concepts/adapters/).

## Installation

**macOS / Linux:**

```bash
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
```

**Windows:**

```powershell
irm https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.ps1 | iex
```

**Build from source** (requires Rust 1.88+):

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky/engine
cargo build --release
```

## Documentation

**[rocky-data.dev](https://rocky-data.dev)** — concepts, guides, CLI reference, Dagster integration, adapter SDK.

## License

[Apache 2.0](../LICENSE)
