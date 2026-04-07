# Rocky

A SQL transformation engine built in Rust. Type-safe compilation, column-level lineage, AI-powered intent, and a language server — for data pipelines that don't break.

No Jinja. No manifest. No parse step.

## Why Rocky

Rocky replaces dbt's core responsibilities — DAG resolution, incremental logic, SQL generation, schema management — with a compiled, type-safe approach. Models are type-checked before execution, schema changes are detected automatically, and an AI intent layer keeps models synchronized when upstream schemas evolve. Rocky ships as a single binary with no runtime dependencies.

## Quick Start

```bash
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
rocky playground my-first-project
cd my-first-project
rocky compile       # Type-check all models
rocky test          # Run assertions locally with DuckDB
rocky run           # Execute the pipeline
```

The playground creates a self-contained project with sample models, contracts, and a DuckDB backend — no credentials needed.

## Features

| Category | Capabilities |
|----------|-------------|
| **Compiler** | Type checking, column-level lineage, data contracts, DAG resolution, diagnostics with suggestions |
| **DSL** | Pipeline-oriented syntax (`.rocky` files), NULL-safe operators, window functions, CTEs, pattern matching |
| **AI** | Intent metadata, `ai-sync` (schema change propagation), `ai-explain`, `ai-test` (assertion generation) |
| **IDE** | VS Code extension, LSP with completion, hover, go-to-definition, find references, rename, code actions, inlay hints, semantic tokens, signature help |
| **Execution** | DuckDB (local/testing), Databricks (production), Snowflake (beta) adapters |
| **Quality** | Row count, column match, freshness, null rate, anomaly detection, custom SQL checks |
| **Optimization** | Cost-based materialization, storage profiling, compaction, partition archival |
| **Integration** | Dagster ([dagster-rocky](https://github.com/rocky-data/rocky/tree/main/integrations/dagster)), dbt import, CI pipeline |
| **Governance** | Unity Catalog tags, workspace isolation, declarative RBAC with GRANT/REVOKE diffing |

## CLI Commands

### Core Pipeline

| Command | Description |
|---------|-------------|
| `rocky init [path]` | Scaffold a new project |
| `rocky validate` | Validate config and models without API calls |
| `rocky discover` | List connectors and tables from the source |
| `rocky plan --filter key=value` | Preview SQL (dry-run) |
| `rocky run --filter key=value` | Execute the full pipeline |
| `rocky state` | Show stored watermarks |

### Modeling

| Command | Description |
|---------|-------------|
| `rocky compile` | Type-check models, validate contracts |
| `rocky lineage <model>` | Trace column-level lineage |
| `rocky test` | Run local tests via DuckDB |
| `rocky ci` | Combined compile + test for CI pipelines |

### AI

| Command | Description |
|---------|-------------|
| `rocky ai "<intent>"` | Generate a model from natural language |
| `rocky ai-explain <model>` | Generate intent description from code |
| `rocky ai-sync` | Propose intent-guided updates for schema changes |
| `rocky ai-test <model>` | Generate test assertions from intent |

### Development

| Command | Description |
|---------|-------------|
| `rocky playground [path]` | Create a sample DuckDB project |
| `rocky import-dbt --dbt-project <path>` | Convert a dbt project to Rocky |
| `rocky serve` | HTTP API server with watch mode |
| `rocky lsp` | Language Server Protocol for IDE integration |
| `rocky init-adapter <name>` | Scaffold a new warehouse adapter crate |
| `rocky hooks list` | List configured lifecycle hooks |
| `rocky hooks test <event>` | Fire a test hook event |
| `rocky test-adapter` | Run adapter conformance suite |
| `rocky validate-migration` | Validate dbt → Rocky migration output |

### Administration

| Command | Description |
|---------|-------------|
| `rocky doctor` | Aggregate health checks (config, state, adapters, pipelines) |
| `rocky history` | Run history with trend analysis |
| `rocky metrics <model>` | Quality metrics with alerts |
| `rocky optimize` | Materialization cost recommendations |
| `rocky drift` | Schema drift detection |
| `rocky compare` | Shadow vs production comparison |
| `rocky compact` | Generate OPTIMIZE/VACUUM SQL |
| `rocky profile-storage` | Column encoding recommendations |
| `rocky archive` | Partition archival |

## Adapters

| Role | Adapter | Status | Description |
|------|---------|--------|-------------|
| Source | Fivetran | Production | REST API discovery of connectors and tables |
| Source | Manual | Production | Schema/table lists defined in `rocky.toml` |
| Warehouse | Databricks | Production | SQL Statement API, Unity Catalog governance |
| Warehouse | Snowflake | Beta | SQL execution via Snowflake connector |
| Warehouse | BigQuery | Beta | SQL execution via BigQuery connector |
| Warehouse | DuckDB | Local/Testing | Embedded execution for development and CI |

## VS Code Extension

The [Rocky VS Code extension](https://github.com/rocky-data/rocky/tree/main/editors/vscode) provides full language support:

- Syntax highlighting for `.rocky` and SQL model files
- Diagnostics, hover, completion, go-to-definition
- Find references, rename, code actions, inlay hints
- Model lineage visualization
- AI model generation from the editor

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

Full documentation: **[rocky-data.github.io/rocky](https://rocky-data.github.io/rocky/)**

## License

[Apache 2.0](../LICENSE)
