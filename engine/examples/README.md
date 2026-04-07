# Rocky Examples

Self-contained example projects demonstrating Rocky's features. Each example uses DuckDB as the backend for local execution -- no external credentials needed.

## Examples

| Directory | Description |
|-----------|-------------|
| [quickstart/](quickstart/) | Minimal 3-model pipeline (source, staging, fact table) |
| [seed-demo/](seed-demo/) | Loading CSV seed files into DuckDB with `rocky seed` |
| [window-functions/](window-functions/) | Rocky DSL window functions vs SQL side by side |
| [test-declarative/](test-declarative/) | Declarative `[[tests]]` in TOML sidecars |
| [dbt-migration/](dbt-migration/) | Side-by-side dbt vs Rocky, showing how to migrate |
| [dagster-integration/](dagster-integration/) | Orchestrating Rocky with Dagster using `dagster-rocky` |
| [ai-intent/](ai-intent/) | AI-powered test generation using model intent fields |
| [multi-layer/](multi-layer/) | Bronze, Silver, Gold medallion architecture with contracts |
| [snapshot/](snapshot/) | SCD Type 2 snapshots with `rocky snapshot` |
| [watch-demo/](watch-demo/) | Auto-recompile on file changes with `rocky watch` |
| [fmt-demo/](fmt-demo/) | Format `.rocky` files with `rocky fmt` |
| [docs-demo/](docs-demo/) | Generate HTML documentation catalog with `rocky docs` |
| [compare-demo/](compare-demo/) | Shadow table validation with `rocky compare` |
| [shell-demo/](shell-demo/) | Interactive SQL REPL with `rocky shell` |

## Running

Each example includes a `rocky.toml` configured for DuckDB local execution:

```bash
cd examples/quickstart
rocky plan --config rocky.toml
rocky run --config rocky.toml
```

Or use the playground mode which sets up sample data automatically:

```bash
rocky playground
```

## Prerequisites

- Rocky CLI installed (`cargo install rocky` or build from source)
- No external services required -- all examples use DuckDB locally
