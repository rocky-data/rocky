---
title: Development Commands
description: Commands for local development — playground, dbt import, HTTP server, LSP, and adapter scaffolding
sidebar:
  order: 4
---

The development commands support local workflows: creating sample projects, importing existing dbt projects, running the semantic graph HTTP server, IDE integration via LSP, and scaffolding new warehouse adapter crates.

---

## `rocky playground`

Create a self-contained sample project using DuckDB as the local execution engine. No warehouse credentials or external services are required. Useful for learning Rocky, testing model logic, and rapid prototyping.

```bash
rocky playground [path]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `path` | `string` | `rocky-playground` | Directory name for the playground project. |

### Examples

Create a playground with the default name:

```bash
rocky playground
```

```
Created rocky-playground/rocky.toml (DuckDB config)
Created rocky-playground/models/stg_orders.sql
Created rocky-playground/models/stg_orders.toml
Created rocky-playground/models/stg_customers.sql
Created rocky-playground/models/stg_customers.toml
Created rocky-playground/models/fct_revenue.sql
Created rocky-playground/models/fct_revenue.toml
Created rocky-playground/seeds/orders.csv
Created rocky-playground/seeds/customers.csv

Playground ready! Run:
  cd rocky-playground
  rocky compile
  rocky test
```

Create a playground with a custom name:

```bash
rocky playground my-experiment
```

```
Created my-experiment/rocky.toml (DuckDB config)
Created my-experiment/models/...
Playground ready! Run:
  cd my-experiment
  rocky compile
  rocky test
```

### Related Commands

- [`rocky init`](/reference/commands/core-pipeline/#rocky-init) -- create a production project
- [`rocky test`](/reference/commands/modeling/#rocky-test) -- run tests in the playground
- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- compile playground models

---

## `rocky import-dbt`

Import an existing dbt project and convert it to Rocky models. Translates dbt SQL (Jinja + ref/source macros) and YAML config into Rocky's `.sql` + `.toml` sidecar format.

```bash
rocky import-dbt [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--dbt-project <PATH>` | `PathBuf` | **(required)** | Path to the dbt project directory (containing `dbt_project.yml`). |
| `--output <PATH>` | `PathBuf` | `models` | Output directory for the generated Rocky model files. |

### Examples

Import a dbt project:

```bash
rocky import-dbt --dbt-project ~/projects/acme-dbt
```

```json
{
  "version": "0.1.0",
  "command": "import-dbt",
  "models_imported": 24,
  "sources_imported": 6,
  "warnings": [
    { "model": "stg_payments", "message": "custom Jinja macro 'cents_to_dollars' not translated, left as comment" },
    { "model": "fct_orders", "message": "incremental_strategy 'merge' converted to Rocky incremental with note" }
  ],
  "output_dir": "models/"
}
```

Import to a custom output directory:

```bash
rocky import-dbt --dbt-project ~/projects/acme-dbt --output src/models
```

```json
{
  "version": "0.1.0",
  "command": "import-dbt",
  "models_imported": 24,
  "sources_imported": 6,
  "warnings": [],
  "output_dir": "src/models/"
}
```

Import and then compile to verify:

```bash
rocky import-dbt --dbt-project ~/projects/acme-dbt && rocky compile
```

### Related Commands

- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- compile the imported models
- [`rocky init`](/reference/commands/core-pipeline/#rocky-init) -- create a new project first, then import

---

## `rocky serve`

Start an HTTP API server that exposes the compiler's semantic graph. Provides REST endpoints for model metadata, lineage, and compilation results. Useful for editor integrations, dashboards, and custom tooling.

```bash
rocky serve [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--models <PATH>` | `PathBuf` | `models` | Directory containing model files. |
| `--contracts <PATH>` | `PathBuf` | | Directory containing data contract definitions. |
| `--port <PORT>` | `u16` | `8080` | Port to listen on. |
| `--watch` | `bool` | `false` | Watch for file changes and auto-recompile. |

### Examples

Start the server with defaults:

```bash
rocky serve
```

```
Compiled 14 models in 42ms
Listening on http://127.0.0.1:8080
Endpoints:
  GET /api/models          - List all compiled models
  GET /api/models/:name    - Get model details
  GET /api/lineage/:name   - Column-level lineage
  GET /api/dag             - Full dependency graph
```

Start with file watching on a custom port:

```bash
rocky serve --port 3000 --watch
```

```
Compiled 14 models in 42ms
Watching models/ for changes...
Listening on http://127.0.0.1:3000
```

Start with contracts:

```bash
rocky serve --models src/models --contracts src/contracts --port 9090 --watch
```

### Related Commands

- [`rocky lsp`](#rocky-lsp) -- IDE integration via Language Server Protocol
- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- one-shot compilation without the server
- [`rocky lineage`](/reference/commands/modeling/#rocky-lineage) -- CLI lineage (the server exposes the same data via HTTP)

---

## `rocky lsp`

Start a Language Server Protocol server for IDE integration. Provides diagnostics, completions, hover information, and go-to-definition for Rocky SQL models.

```bash
rocky lsp
```

### Flags

No command-specific flags. The LSP server communicates over stdin/stdout per the LSP specification.

### Examples

Start the LSP server (typically called by an editor, not directly):

```bash
rocky lsp
```

Configure in VS Code (`settings.json`):

```json
{
  "rocky.lsp.path": "rocky",
  "rocky.lsp.args": ["lsp"]
}
```

Configure in Neovim (with lspconfig):

```lua
require('lspconfig').rocky.setup({
  cmd = { "rocky", "lsp" },
  filetypes = { "sql" },
  root_dir = function(fname)
    return require('lspconfig.util').root_pattern('rocky.toml')(fname)
  end,
})
```

### Related Commands

- [`rocky serve`](#rocky-serve) -- HTTP API server (alternative integration method)
- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- the LSP uses the same compilation engine

---

## `rocky init-adapter`

Scaffold a new warehouse adapter crate. Creates the directory structure, Cargo.toml, and trait implementation stubs for building a custom adapter (e.g., BigQuery, Redshift, Snowflake).

```bash
rocky init-adapter <name>
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `name` | `string` | **(required)** | Adapter name (e.g., `bigquery`, `redshift`, `snowflake`). |

### Examples

Scaffold a BigQuery adapter:

```bash
rocky init-adapter bigquery
```

```
Created crates/rocky-bigquery/Cargo.toml
Created crates/rocky-bigquery/src/lib.rs
Created crates/rocky-bigquery/src/connector.rs
Created crates/rocky-bigquery/src/auth.rs

Adapter scaffold ready at crates/rocky-bigquery/
Implement the WarehouseAdapter trait in src/connector.rs to get started.
```

Scaffold a Snowflake adapter:

```bash
rocky init-adapter snowflake
```

```
Created crates/rocky-snowflake/Cargo.toml
Created crates/rocky-snowflake/src/lib.rs
Created crates/rocky-snowflake/src/connector.rs
Created crates/rocky-snowflake/src/auth.rs

Adapter scaffold ready at crates/rocky-snowflake/
Implement the WarehouseAdapter trait in src/connector.rs to get started.
```

### Related Commands

- [`rocky compile`](/reference/commands/modeling/#rocky-compile) -- compile models using the new adapter
- [`rocky validate`](/reference/commands/core-pipeline/#rocky-validate) -- validate config after registering the adapter

---

## `rocky hooks`

Manage lifecycle hooks configured in rocky.toml.

### `rocky hooks list`

List all configured hooks.

```bash
rocky hooks list
```

### `rocky hooks test`

Fire a synthetic test event to validate hook scripts.

```bash
rocky hooks test <EVENT>
```

| Argument | Type | Description |
|----------|------|-------------|
| `EVENT` | string | Event name (e.g., `pipeline_start`, `materialize_error`) |

### Examples

```bash
$ rocky hooks test pipeline_start
Firing test event: pipeline_start
Hook 'bash scripts/notify.sh': OK (exit 0, 120ms)
```

---

## `rocky validate-migration`

Compare a dbt project against its Rocky import to verify correctness.

```bash
rocky validate-migration [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--dbt-project` | path | required | Path to the dbt project |
| `--rocky-project` | path | — | Path to the Rocky project (defaults to current directory) |
| `--sample-size` | number | — | Number of sample rows for data comparison |

### Examples

```bash
$ rocky validate-migration --dbt-project ~/dbt-project
Validating 12 models...
  stg_customers: PASS (schema match, row count match)
  fct_orders:    PASS (schema match, row count match)
  dim_products:  WARN (column order differs)
```

---

## `rocky test-adapter`

Run conformance tests against a warehouse adapter.

```bash
rocky test-adapter [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--adapter` | string | — | Built-in adapter name (databricks, snowflake, duckdb) |
| `--command` | string | — | Path to a process adapter binary |
| `--adapter-config` | string | — | JSON config to pass to the adapter |

### Examples

```bash
$ rocky test-adapter --adapter duckdb
Running conformance tests for duckdb...
  19/19 core tests passed
  3/7 optional tests passed (4 skipped: not supported)
```

### Related Commands

- [`rocky init-adapter`](#rocky-init-adapter) -- scaffold a new adapter crate
- [`rocky validate`](/reference/commands/core-pipeline/#rocky-validate) -- validate config after registering the adapter
