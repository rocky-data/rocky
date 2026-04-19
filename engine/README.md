# Rocky

A SQL transformation engine built in Rust. Type-safe compilation, column-level lineage, AI-powered intent, and a language server — for data pipelines that don't break.

No Jinja. No manifest. No parse step.

## Why Rocky

Rocky replaces dbt's core responsibilities — DAG resolution, incremental logic, SQL generation, schema management — with a compiled, type-safe approach. Models are type-checked before execution, schema changes are detected automatically, and an AI intent layer keeps models synchronized when upstream schemas evolve. Rocky ships as a single binary with no runtime dependencies.

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
rocky compile        # Type-check all models
rocky test           # Run assertions locally (DuckDB)
rocky plan           # Preview generated SQL (dry-run)
rocky run            # Execute the pipeline
rocky ai "<intent>"  # Generate a model from natural language
rocky lineage        # Trace column-level lineage
rocky doctor         # Aggregate health checks
rocky serve          # HTTP API + live watch
rocky lsp            # Language Server Protocol for IDEs
```

Full reference: [CLI commands](https://rocky-data.github.io/rocky/reference/cli/).

## Adapters

| Role | Adapter | Status | Notes |
|------|---------|--------|-------|
| Source | Fivetran | Production | REST API discovery of connectors and tables |
| Source | Manual | Production | Schema/table lists inline in `rocky.toml` |
| Warehouse | Databricks | Production | SQL Statement API + Unity Catalog governance |
| Warehouse | Snowflake | Beta | SQL execution via Snowflake connector |
| Warehouse | BigQuery | Beta | SQL execution via BigQuery connector |
| Warehouse | DuckDB | Local / Testing | Embedded execution for development and CI |

Build a custom adapter in Rust or any language: [Adapter SDK](https://rocky-data.github.io/rocky/concepts/adapters/).

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

**[rocky-data.github.io/rocky](https://rocky-data.github.io/rocky/)** — concepts, guides, CLI reference, Dagster integration, adapter SDK.

## License

[Apache 2.0](../LICENSE)
