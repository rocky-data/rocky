# Rocky Examples

Self-contained example projects that show Rocky's features. The examples that run a pipeline use DuckDB for local execution, so they need no warehouse credentials. `ai-intent` is the exception for credentials: it calls the Anthropic API and needs `ANTHROPIC_API_KEY`.

## Examples

| Directory | Description |
|-----------|-------------|
| [quickstart/](quickstart/) | Minimal 3-model pipeline (source, staging, fact table) |
| [seed-demo/](seed-demo/) | Loading CSV seed files into DuckDB with `rocky seed` |
| [window-functions/](window-functions/) | Rocky DSL window functions vs SQL side by side |
| [test-declarative/](test-declarative/) | Declarative `[[tests]]` in TOML sidecars |
| [dbt-migration/](dbt-migration/) | Side-by-side dbt vs Rocky, showing how to migrate |
| [dagster-integration/](dagster-integration/) | Orchestrating Rocky with Dagster using `dagster-rocky` |
| [ai-intent/](ai-intent/) | AI test generation from model intent fields (needs `ANTHROPIC_API_KEY`) |
| [multi-layer/](multi-layer/) | Bronze, Silver, Gold medallion architecture with contracts |
| [snapshot/](snapshot/) | SCD Type 2 snapshots with `rocky snapshot` |
| [watch-demo/](watch-demo/) | Auto-recompile on file changes with `rocky watch` |
| [fmt-demo/](fmt-demo/) | Format `.rocky` files with `rocky fmt` |
| [docs-demo/](docs-demo/) | Generate HTML documentation catalog with `rocky docs` |
| [compare-demo/](compare-demo/) | Shadow table validation with `rocky compare` |
| [shell-demo/](shell-demo/) | Interactive SQL REPL with `rocky shell` |
| [process-adapter-echo/](process-adapter-echo/) | A process adapter in one Python script, tested with `rocky test-adapter` |

## Running

Most examples include a `rocky.toml` configured for DuckDB. Three do not: `fmt-demo` needs none, `dbt-migration` keeps its config in `rocky-project/`, and `process-adapter-echo` is an adapter, not a project. Each example's README gives its own commands. The paths below are relative to `engine/`, so run them from there:

```bash
cd examples/quickstart
rocky --config rocky.toml plan
rocky --config rocky.toml run
```

Or use the playground mode which sets up sample data automatically:

```bash
rocky playground
```

## Prerequisites

- The Rocky CLI. Install it with `engine/install.sh` (see the [engine README](../README.md#installation)) or build it from source. Rocky is not published on crates.io, so `cargo install rocky` does not install it.
- No warehouse. The pipeline examples use DuckDB locally. `ai-intent` needs `ANTHROPIC_API_KEY`.
