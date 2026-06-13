# Rocky

**Rocky is the typed graph between your code and whichever warehouse, table format, or query engine you've chosen.** It is a typed compiler that sits above Databricks, Snowflake, BigQuery, or DuckDB and owns the graph between your code and your data: named branches, content-addressed run records, column-level lineage, compile-time contracts, and per-model cost attribution. It ships as a single static Rust binary, and storage and compute stay where they are.

**Rocky is a real compiler** with type inference, diagnostic codes, and an IDE, not a warehouse, table format, query engine, or templating layer. The failures that quietly cost data teams the most (silent schema drift, column-rename blast radius, dialect divergence, cost spikes nobody can attribute) become compile errors and blocked PRs.

There is no Jinja, no manifest, and no separate parse step.

## Why Rocky exists

The expensive failures in modern data platforms aren't slow queries. They're trust failures:

- A source column type changes upstream and a revenue dashboard quietly diverges for three days.
- An engineer renames a column on `stg_orders` and 47 downstream models break in production.
- A `SELECT *` pulls a new column nobody designed for; a downstream join silently double-counts.
- A Snowflake-only function lands in a Databricks-targeted project and only fails in prod.
- Warehouse spend doubles in a month and nobody can attribute which model caused it.
- An auditor asks who changed `fct_revenue.amount`, when, and why, and the answer involves `git blame` and screenshots.

Rocky turns each of these into a compile error or a blocked PR before it ships: a column-type change is `E013` at compile, a rename's blast radius is a `rocky lineage-diff` comment, an unbudgeted cost spike is a `[budget]` block that fails the run, and unmasked classified data fails `rocky compliance`. These failures are invisible to the warehouse and out of scope for the templating layer above it. Rocky is the typed graph in between: real type inference and diagnostic codes, not text macros or runtime checks. For how Rocky compares to dbt Core, dbt Fusion, and SQLMesh, see the [comparison page](https://rocky-data.dev/getting-started/comparison/).

## Scope on the ELT spectrum

| Stage | Rocky | Notes |
|---|---|---|
| Extract (SaaS sources) | — | Use Fivetran, Airbyte, Stitch, or warehouse-native CDC |
| Extract (files) | ✅ | `rocky load`: CSV / Parquet / JSONL from a directory |
| Load (bronze replication) | ✅ | Config-driven replication pipelines |
| Transform | ✅ | Compiled SQL models |
| Quality | ✅ | Inline assertions during `rocky run` |
| Orchestration | Partial | First-class Dagster integration; `rocky serve` standalone |

## The seven trust dimensions

1. **SQL as a typed, compiled language.** Real type inference, diagnostic codes (`E###` errors, `W###` warnings, `P###` portability lints), and a real LSP, not text macros or runtime checks.
2. **Compile-time column-level lineage.** Rocky knows every column's lineage before a row is written, so `rocky lineage-diff main` can block a PR when a downstream contract breaks.
3. **Branches and an inspectable run ledger.** `rocky branch create`, `rocky run --branch`, and `rocky replay <run_id>`: branches are isolated schemas, and every run is recorded in an auditable ledger (who ran it, the commit, the per-model SQL hash, and row counts). On the content-addressed materialization path, output files are named by the hash of their bytes, so an auditor can confirm the recorded output is exactly what shipped.
4. **Per-model cost attribution.** Cost is a column on every run record. `[budget]` blocks fail the run, `budget_breach` fires a hook, and `rocky preview cost` projects spend at PR time.
5. **AI gated through the compiler.** Every AI suggestion is type-checked before it lands. The `Attempts: 2` retry on `rocky ai` is the loop: generate, type-check, auto-fix, then land.
6. **Dialect-divergence lint.** Cross-warehouse teams write SQL once, and `P001` catches Snowflake-only constructs in a Databricks project at compile time.
7. **Declarative governance.** RBAC as code with GRANT/REVOKE diffing, Unity Catalog tags, workspace isolation, and masking strategies bound to classification tags, so compliance becomes a CI check.

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
| **DSL** | Pipeline-oriented `.rocky` syntax; optional, models stay plain SQL by default |
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

Build a custom adapter in Rust or any language with the [Adapter SDK guide](https://rocky-data.dev/guides/adapter-sdk/), which walks through a ClickHouse-shaped skeleton, the trait surface, auth, testing, and distribution. Concepts overview: [Adapter SDK](https://rocky-data.dev/concepts/adapters/).

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

**[rocky-data.dev](https://rocky-data.dev)**: concepts, guides, CLI reference, Dagster integration, and the adapter SDK.

## License

[Apache 2.0](../LICENSE)
