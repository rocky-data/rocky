# Rocky

The **trust system for your data**. A Rust-based control plane for warehouse-side data pipelines: branches, replay, provable reproducibility, column-level lineage, compile-time safety, per-model cost attribution.

Keep Databricks or Snowflake. Bring Rocky for the DAG.

**Rocky is not a warehouse.** Storage and compute stay with your warehouse; Rocky owns the graph — dependencies, compile-time types, drift handling, incremental logic, lineage, cost.

No Jinja. No manifest. No parse step.

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

1. **Branches + replay + column-level lineage** — `rocky branch create`, `rocky run --branch`, `rocky replay <run_id>`. Branch and replay workflow on top of your warehouse.
2. **Cost attribution + budgets** — per-model cost on every run; `[budget]` block in `rocky.toml`; `budget_breach` hook event.
3. **Resume + circuit breakers** — three-state `CircuitBreaker`, checkpointed run state, deploy safety.
4. **Observability** — `rocky trace` Gantt output, OpenTelemetry OTLP export (feature-gated).
5. **Schema-grounded AI** — every AI feature gated through the compiler; generated SQL type-checks before it lands.
6. **Polyglot correctness** — dialect-divergence lint across Databricks / Snowflake / BigQuery / DuckDB.
7. **SQL as first-class with types** — type inference over raw `.sql`, `SELECT *` blast-radius lint, DAG-aware refactoring.

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

**Build from source** (requires Rust 1.85+):

```bash
git clone https://github.com/rocky-data/rocky.git
cd rocky/engine
cargo build --release
```

## Documentation

**[rocky-data.dev](https://rocky-data.dev)** — concepts, guides, CLI reference, Dagster integration, adapter SDK.

## License

[Apache 2.0](../LICENSE)
