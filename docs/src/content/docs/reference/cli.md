---
title: CLI Reference
description: Every Rocky command, where it is documented, and the global flags
sidebar:
  order: 1
---

Rocky ships one binary, `rocky`. This page lists every command and the global flags. Each command row links to the page that documents it.

## Command index

The groups and their order match `rocky --help`. The core commands come first.

### Core commands

| Command | What it does |
|---|---|
| [`compile`](/reference/commands/modeling/#rocky-compile) | Resolve dependencies, type-check the SQL, and check contracts. |
| <span id="rocky-run"></span><span id="run"></span>[`run`](/reference/commands/core-pipeline/#rocky-run) | Plan and apply in one step. |
| [`test`](/reference/commands/modeling/#rocky-test) | Run model tests on an in-memory DuckDB, or sidecar `[[tests]]` on the warehouse. |
| [`plan`](/reference/commands/core-pipeline/#rocky-plan) | Write a reviewable plan of what a run would do, without running it. |
| [`review`](/reference/commands/governance-reclamation/#rocky-review) | Sign off on a gated plan, or list the review queue. |
| [`apply`](/reference/commands/core-pipeline/#rocky-apply) | Execute a stored plan by its `plan_id`. |
| [`policy`](/reference/commands/governance-reclamation/#rocky-policy) | Check, test, show, freeze and unfreeze the agent-policy rules. |

### Getting started

| Command | What it does |
|---|---|
| [`init`](/reference/commands/core-pipeline/#rocky-init) | Create a new project from a template. |
| [`playground`](/reference/commands/development/#rocky-playground) | Create a sample DuckDB project that needs no credentials. |

### Model development

| Command | What it does |
|---|---|
| [`validate`](/reference/commands/core-pipeline/#rocky-validate) | Check `rocky.toml` with no network calls. |
| [`discover`](/reference/commands/core-pipeline/#rocky-discover) | List the connectors and tables the source exposes. |
| [`dag`](/reference/commands/modeling/#rocky-dag) | Print the whole project as one graph of pipeline stages. |
| [`catalog`](/reference/commands/modeling/#rocky-catalog) | Write a column-level lineage snapshot to disk. |
| [`lineage`](/reference/commands/modeling/#rocky-lineage) | Trace a model or a column through its upstream or downstream models. |
| [`lineage-diff`](/reference/commands/modeling/#rocky-lineage-diff) | Report the downstream reach of each changed column, for PR review. |
| [`ci`](/reference/commands/modeling/#rocky-ci) | Compile and test with no warehouse credentials. |
| [`ci-diff`](/reference/commands/modeling/#rocky-ci-diff) | Report the column changes between a git ref and `HEAD`. |
| [`preview`](/reference/commands/modeling/#rocky-preview) | Build the changed models of a PR into a branch, then diff rows and cost. |
| [`compare`](#rocky-compare) | Compare shadow tables against production tables. |
| [`branch`](/reference/commands/core-pipeline/#rocky-branch) | Create, list, compare, approve, promote and delete named branches. |
| <span id="rocky-list"></span>[`list`](/reference/commands/development/#rocky-list) | List pipelines, adapters, models, sources and dependencies. |
| [`emit-sql`](/reference/commands/modeling/#rocky-emit-sql) | Print the warehouse SQL each model compiles to. |
| [`lint`](/reference/commands/modeling/#rocky-lint) | Check model SQL for style problems (`S001`–`S007`). |
| [`imports`](/reference/commands/modeling/#rocky-imports) | Advance the vendored producer baselines your project checks against. |
| [`publish-ir`](/reference/commands/modeling/#rocky-publish-ir) | Publish this project's compiled schema for other teams to check against. |

### Data loading

| Command | What it does |
|---|---|
| [`load`](#rocky-load) | Load CSV, Parquet or JSONL files from a directory into the warehouse. |
| [`seed`](#rocky-seed) | Load static CSV reference files into the warehouse. |
| [`snapshot`](#rocky-snapshot) | Run an SCD Type 2 snapshot pipeline. |
| [`backfill`](/reference/commands/governance-reclamation/#rocky-backfill) | Write a review-gated recovery plan for a set of models and a window. |

### Governance

| Command | What it does |
|---|---|
| [`audit`](/reference/commands/governance-reclamation/#rocky-audit) | Read the policy-decision ledger, a custody chain, or a scorecard. |
| [`brief`](/reference/commands/governance-reclamation/#rocky-brief) | Print the governor's digest: what happened and what needs a person. |
| <span id="rocky-compliance"></span>[`compliance`](/reference/commands/administration/#rocky-compliance) | Report whether every classified column is masked as policy requires. |
| [`product`](/reference/commands/products/) | Verify, compile, approve and read data-product specs. |
| [`fulfill`](/reference/commands/fulfill/) | Drive a product spec through the agent loop. Experimental. |
| [`gc`](/reference/commands/governance-reclamation/#rocky-gc) | Inventory reclaimable artifacts and write a review-gated eviction plan. |
| [`restore`](/reference/commands/governance-reclamation/#rocky-restore) | Write a review-gated plan to rebuild an artifact that `gc` evicted. |

### Operations

| Command | What it does |
|---|---|
| <span id="rocky-doctor"></span><span id="doctor"></span>[`doctor`](/reference/commands/development/#rocky-doctor) | Run health checks on the config, state, adapters and auth. |
| [`state`](/reference/commands/administration/#rocky-state) | Show watermarks, and maintain the state store and schedules. |
| [`history`](/reference/commands/administration/#rocky-history) | Show past runs and per-model executions. |
| [`replay`](/reference/commands/administration/#rocky-replay) | Inspect, audit or re-execute a recorded run. |
| [`trace`](/reference/commands/administration/#rocky-trace) | Show a recorded run as a timeline with concurrency lanes. |
| [`cost`](/reference/commands/administration/#rocky-cost) | Allocate cost per model for a recorded run. |
| [`metrics`](/reference/commands/administration/#rocky-metrics) | Show quality metrics for a model. |
| [`optimize`](/reference/commands/administration/#rocky-optimize) | Recommend `table` or `view` from run history and cost. |
| [`profile`](#rocky-profile) | Report row, null and distinct counts per column. DuckDB only. |
| [`profile-storage`](/reference/commands/administration/#rocky-profile-storage) | Recommend column encodings for a table. |
| [`compact`](/reference/commands/administration/#rocky-compact) | Write a plan of `OPTIMIZE` and `VACUUM` SQL. |
| [`archive`](/reference/commands/administration/#rocky-archive) | Write a plan that deletes old rows. |
| <span id="rocky-retention-status"></span>[`retention-status`](/reference/commands/administration/#rocky-retention-status) | Report each model's declared retention policy. |
| [`hooks`](/reference/commands/development/#rocky-hooks) | List the lifecycle hooks, or fire a test event. |
| [`tick`](/reference/commands/administration/#rocky-tick) | Evaluate schedule demand once and run what is due. Experimental. |

### dbt migration

| Command | What it does |
|---|---|
| [`import-dbt`](/reference/commands/development/#rocky-import-dbt) | Convert a dbt project into a Rocky project. |
| [`validate-migration`](/reference/commands/development/#rocky-validate-migration) | Compare an imported project's models with the dbt original. |

### AI

| Command | What it does |
|---|---|
| [`ai`](/reference/commands/ai/#rocky-ai) | Generate a model from a plain-English description. |
| [`ai-sync`](/reference/commands/ai/#rocky-ai-sync) | Propose model updates after a source schema changes. |
| [`ai-explain`](/reference/commands/ai/#rocky-ai-explain) | Describe what a model does, in plain English. |
| [`ai-test`](/reference/commands/ai/#rocky-ai-test) | Draft test assertions from a model's intent. |
| [`ai-contract`](/reference/commands/ai/#rocky-ai-contract) | Draft a data contract from a model's observed data. DuckDB only. |
| [`mcp`](/reference/commands/ai/#rocky-mcp) | Serve Rocky's tools to an AI agent over MCP. |

### Integrations and tooling

| Command | What it does |
|---|---|
| [`serve`](/reference/commands/development/#rocky-serve) | Start the HTTP API (`/api/v1`), with the optional browser UI (`--ui`) and scheduler (`--scheduler`). |
| [`lsp`](/reference/commands/development/#rocky-lsp) | Start the language server for editors. |
| [`export-schemas`](#rocky-export-schemas) | Write a JSON Schema file for every `--output json` payload. |
| [`export-openapi`](#rocky-export-openapi) | Write the OpenAPI 3.1 document for `rocky serve`. |
| [`completions`](#rocky-completions) | Print a shell completion script. |
| [`test-adapter`](/reference/commands/development/#rocky-test-adapter) | Run the conformance suite against an adapter. |
| [`init-adapter`](/reference/commands/development/#rocky-init-adapter) | Scaffold a new warehouse adapter crate. |
| [`adapter`](/reference/commands/development/#rocky-adapter) | List and inspect process adapters on `$PATH`. |

### Other tools

| Command | What it does |
|---|---|
| [`docs`](#rocky-docs) | Generate a static documentation site, or Parquet metadata tables. |
| [`shell`](#rocky-shell) | Open an interactive SQL shell against the warehouse. |
| [`estimate`](#rocky-estimate) | Estimate each model's cost with warehouse `EXPLAIN`, without running it. |
| [`bench`](#rocky-bench) | Run the built-in performance benchmarks. |
| [`watch`](#rocky-watch) | Recompile when a file in the models directory changes. |
| [`fmt`](#rocky-fmt) | Format `.rocky` files. |

### Other commands

`rocky --help` lists these two under "Other commands".

| Command | What it does |
|---|---|
| [`freshness`](/reference/commands/core-pipeline/#rocky-freshness) | Check source and model freshness against the warehouse. |
| [`package`](/reference/commands/development/#rocky-package) | Vendor a dbt Hub package as Rocky models. |

## Global flags

These flags apply to every command. Put `--config`, `--state-path` and `--state-namespace` **before** the subcommand. The other four work before or after it.

```
rocky --config prod.toml run      # works
rocky run --config prod.toml      # error: unexpected argument '--config' found
rocky run --output json           # works: --output is accepted anywhere
```

| Flag | Short | Placement | Default | Description |
|------|-------|-----------|---------|-------------|
| `--config <PATH>` | `-c` | before the subcommand | `rocky.toml` | Path to the pipeline config. `rocky mcp` also takes its own `--config` after the subcommand. |
| `--output <FORMAT>` | `-o` | anywhere | terminal-aware | `json`, `table` or `md`. Only `rocky brief` renders `md` differently; other commands treat it as `table`. Unset, Rocky uses `table` when stdout is a terminal and `json` otherwise, so piped consumers (Dagster, the LSP, CI) get JSON. |
| `--state-path <PATH>` | | before the subcommand | resolved | Path to the state store. Unset, Rocky uses `<models>/.rocky-state.redb`, or a legacy `.rocky-state.redb` in the current directory with a warning. An explicit path always wins. See [`rocky state`](/reference/commands/administration/#state-path-resolution). |
| `--state-namespace <KEY>` | | before the subcommand | (none) | Use a separate state file, `<models>/.rocky-state/<KEY>.redb`. See [`--state-namespace`](/reference/commands/core-pipeline/#--state-namespace). |
| `--principal <PRINCIPAL>` | | anywhere | `human` | Who is acting: `human` or `agent`. The `[policy]` gates use the more restrictive of this and the plan's own kind, so `--principal human` does not ungate an agent-authored plan. `ROCKY_PRINCIPAL=agent` raises it to `agent`. Only an explicit `--principal` can lower it. Without a `[policy]` block it has no effect. |
| `--principal-id <ID>` | | anywhere | `unnamed` | A name for who is acting. Rocky records it on every policy decision, and `rocky audit --actor <ID>` filters by it. See the rules below. |
| `--cache-ttl <SECONDS>` | | anywhere | `[cache.schemas] ttl_seconds`, else `86400` | Override the schema-cache TTL (how long a cached `DESCRIBE TABLE` result stays valid). `0` treats every entry as stale. To turn the cache off, set `[cache.schemas] enabled = false`. Applies to the CLI read path only: `rocky lsp` and `rocky serve` keep the config TTL. |

Rules for `--principal-id`:

- Use lowercase letters, digits, `.`, `_` and `-`, at most 63 bytes.
- An `@` is refused, so an email address cannot be stored.
- `unnamed`, `unrecorded` and every id that starts with `mcp-` are reserved.
- Precedence: the flag, then `ROCKY_PRINCIPAL_ID`, then `mcp-<profile>` for `rocky mcp`, then `unnamed`. An empty `ROCKY_PRINCIPAL_ID` counts as unset. Any other invalid value is an error.
- Rocky never reads `$USER` or a CI variable for it.
- The id is self-asserted. Nothing verifies it, and it does not change what a gate decides.

```bash
# A custom config and table output
rocky -c pipelines/prod.toml -o table discover

# A fresh type-check against warehouse metadata
rocky --cache-ttl 0 compile
```

---

## Commands documented on this page

These commands have no category page. Each section gives the purpose, the usage, the flags and an example.

### `rocky compare`

Compare kept shadow tables against production tables. A plain `rocky run --shadow` already compares before it drops the shadow objects. Pass `--keep-shadow` to the run to keep them for this command.

```bash
rocky compare [flags]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--filter <key=value>` | (all) | Filter tables by component, for example `--filter tenant=acme`. |
| `--pipeline <NAME>` | | Pipeline name. Required when `rocky.toml` defines more than one pipeline. |
| `--shadow-suffix <SUFFIX>` | `_rocky_shadow` | Suffix of the shadow tables. |
| `--shadow-schema <NAME>` | | Schema of the shadow tables. |
| `--thresholds <JSON>` | | Verdict thresholds as JSON. Keys: `row_count_diff_pct_warn` (default `0.01`), `row_count_diff_pct_fail` (default `0.05`), `allow_column_order_diff` (default `true`). |

```bash
rocky run --filter tenant=acme --shadow --keep-shadow
rocky compare --filter tenant=acme --thresholds '{"row_count_diff_pct_fail": 0.02}'
```

```json
{
  "version": "1.80.0",
  "command": "compare",
  "filter": "tenant=acme",
  "tables_compared": 1,
  "tables_passed": 1,
  "tables_warned": 0,
  "tables_no_baseline": 0,
  "tables_failed": 0,
  "results": [
    {
      "production_table": "warehouse.staging.orders",
      "shadow_table": "warehouse.staging.orders_rocky_shadow",
      "row_count_match": true,
      "production_count": 15000,
      "shadow_count": 15000,
      "row_count_diff_pct": 0.0,
      "schema_match": true,
      "schema_diffs": [],
      "verdict": "pass",
      "reasons": []
    }
  ],
  "overall_verdict": "pass"
}
```

Each `verdict` is one of these. `reasons` explains each one.

| `verdict` | Meaning | Fails the command |
|---|---|---|
| `pass` | Within the thresholds. | No |
| `warn` | Past the warn threshold. | No |
| `fail` | Past the fail threshold. | Yes |
| `no_baseline` | Rocky confirmed that production has no target yet. Counted in `tables_no_baseline`. | No |
| `error` | Rocky could not confirm the target or read a count or schema. Counted in `tables_failed`. | Yes |

An unreadable count is `null`, never `0`. `row_count_diff_pct` is `null` unless Rocky read both counts.

---

### `rocky load`

Load data files from a directory into the warehouse. Rocky reads CSV, Parquet and JSONL. It finds the format from the file extension unless you set it.

```bash
rocky load [flags]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--source-dir <PATH>` | (from the pipeline config) | Directory that holds the data files. |
| `--format <FORMAT>` | from the extension | `csv`, `parquet` or `jsonl`. |
| `--target <NAME>` | from the file name | Target table name. Every file in the directory goes to this one table. |
| `--pipeline <NAME>` | | Pipeline name. Required when more than one pipeline is defined. |
| `--truncate` | off | Empty the target table before each file loads. Read the warning below first. |

```bash
rocky load --source-dir data/dropbox/ --format parquet
```

:::caution[`--truncate` empties the target once per file, not once per command]
Rocky loads the files one at a time, in sorted filename order. `--truncate` deletes every row of the target before each file. When several files share one target table, **only the last file's rows survive**.

Files share one target when you pass `--target <NAME>`, or when the pipeline config sets `target.table`. With neither, each file goes to a table named after the file, and the truncates do not erase each other.

To combine several files into one table, leave `--truncate` off. Empty that table yourself first if you need a clean replacement.
:::

A `load` pipeline reads every file it finds on each run. It does not track what it already read. So a `load` pipeline cannot join the [`[pipeline.NAME.schedule]`](/reference/configuration/#pipelinenameschedule) graph, because each scheduled run would duplicate data. `rocky validate` refuses that config with `V044`.

---

### `rocky seed`

Load static reference data from CSV files into the warehouse. Rocky infers each column type (`STRING`, `BIGINT`, `DOUBLE`, `BOOLEAN`, `TIMESTAMP`) from the data, then creates or replaces the target table.

```bash
rocky seed [flags]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--seeds <PATH>` | `seeds` | Directory of `.csv` seed files. |
| `--pipeline <NAME>` | | Pipeline name. Required when more than one pipeline is defined. |
| `--filter <NAME>` | (all) | Load only the seed with this name. |

```bash
rocky seed --filter dim_date
```

An optional `.toml` sidecar beside a CSV sets the target, overrides inferred types, and adds SQL hooks:

```toml
# seeds/dim_date.toml
pre_hook  = ["CREATE SCHEMA IF NOT EXISTS warehouse.reference"]
post_hook = ["ANALYZE warehouse.reference.dim_date"]

[target]
catalog = "warehouse"
schema = "reference"
table = "dim_date"

[column_types]          # column name -> SQL type
date_key = "DATE"
```

`pre_hook` statements run in order before the seed writes anything. `post_hook` statements run after the table loads. A failing `pre_hook` stops the seed before any data is written. So a guard such as `SELECT 1 / COUNT(*) FROM warehouse.reference.dim_date` stops the load when the source is empty. These SQL hooks are not the pipeline [lifecycle hooks](/concepts/hooks/), which run shell commands and webhooks on run events.

```json
{
  "version": "1.80.0",
  "command": "seed",
  "seeds_dir": "seeds",
  "tables_loaded": 1,
  "tables_failed": 0,
  "tables": [
    { "name": "dim_date", "target": "warehouse.reference.dim_date", "rows": 365, "columns": 4, "duration_ms": 42 }
  ],
  "duration_ms": 55
}
```

---

### `rocky snapshot`

Run an SCD Type 2 snapshot pipeline. Rocky generates and runs `MERGE` SQL that keeps the history of a source table. The target table gets `valid_from`, `valid_to`, `is_current` and `snapshot_id` columns.

```bash
rocky snapshot [flags]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--pipeline <NAME>` | | Pipeline name. Required when more than one pipeline is defined. |
| `--dry-run` | off | Print the generated SQL and execute nothing. |

```toml
[pipeline.customers_history]
type = "snapshot"
unique_key = ["customer_id"]
updated_at = "updated_at"
invalidate_hard_deletes = true

[pipeline.customers_history.source]
adapter = "prod"
catalog = "main"
schema = "raw"
table = "customers"

[pipeline.customers_history.target]
adapter = "prod"
catalog = "warehouse"
schema = "history"
table = "customers_history"
```

Rocky finds changed rows in one of two ways. The **timestamp** strategy compares the `updated_at` column. The **check** strategy compares the columns you list, for a source with no reliable timestamp.

```
  initial load ──▶ close changed rows ──▶ insert new versions ──▶ invalidate hard deletes
  CREATE TABLE      MERGE: valid_to,        INSERT the rows          UPDATE rows missing
  IF NOT EXISTS     is_current = FALSE      just closed              from the source (optional)
```

```json
{
  "version": "1.80.0",
  "command": "snapshot",
  "pipeline": "customers_history",
  "source": "main.raw.customers",
  "target": "warehouse.history.customers_history",
  "dry_run": false,
  "steps_total": 4,
  "steps_ok": 4,
  "steps": [
    { "step": "initial_load", "sql": "...", "status": "ok", "duration_ms": 12 },
    { "step": "merge_1", "sql": "...", "status": "ok", "duration_ms": 45 }
  ],
  "duration_ms": 120
}
```

---

### `rocky profile`

Report what is in a model's data, column by column: row count, null count and distinct count. Run it before you write a contract or a test, so the assertion matches the data. DuckDB only.

```bash
rocky profile <model> [flags]
```

| Argument or flag | Default | Description |
|------|---------|-------------|
| `model` | required | Model to profile. |
| `--column <NAME>` | (every column) | Profile only this column. |
| `--sample <N>` | `0` (off) | Also return up to N distinct non-null values per column as `sample_values`. N is at most 100. |
| `--models <PATH>` | `models` | Models directory. Rocky compiles it to get the model's inferred schema. |

```bash
rocky profile fct_orders --column amount --sample 5
```

- **The table Rocky reads.** Rocky profiles the model's target table when it exists. Otherwise it profiles the first source table it can resolve, and skips any column that table does not have. The JSON names the table read under `profiled_table` and the missing target under `fell_back_from`. The text output prints neither.
- **Minimum and maximum.** `--output json` carries `min` and `max` for every column. The text output prints only the counts.
- **Cell values.** Without `--sample`, the only cell values are `min`, `max` and `observed_values` (the value list of a column with 25 or fewer distinct values). `--sample` reads every distinct value of each column, so it costs more on a large table. Rocky picks the sample by a hash of each value, so a re-run on unchanged data returns the same values. The [tag-suggestion aid](/python-sdk/classification-aid/) uses it.

---

### `rocky export-schemas`

Write a JSON Schema file for every `--output json` payload. The Python SDK and the VS Code extension generate their types from these files.

```bash
rocky export-schemas [output_dir]
```

| Argument | Default | Description |
|------|---------|-------------|
| `output_dir` | `schemas` | Directory for the `.schema.json` files. |

```bash
rocky export-schemas schemas/
```

---

### `rocky export-openapi`

Write an OpenAPI 3.1 document for the `rocky serve` HTTP API. Rocky builds `components/schemas` from the same registry as `export-schemas`, and `paths` from the `/api/v1` route table. It checks the result against the OpenAPI 3.1 meta-schema before it writes the file.

```bash
rocky export-openapi [output_path]
```

| Argument | Default | Description |
|------|---------|-------------|
| `output_path` | `docs/public/openapi.json` | Where to write the document. |

See [Embedding Rocky](/guides/embedding/) for the API itself.

---

### `rocky completions`

Print a shell completion script.

```bash
rocky completions <shell>
```

| Argument | Description |
|------|-------------|
| `shell` | `bash`, `elvish`, `fish`, `powershell` or `zsh`. |

```bash
rocky completions zsh  > ~/.zsh/completions/_rocky
rocky completions bash > /etc/bash_completion.d/rocky
rocky completions fish > ~/.config/fish/completions/rocky.fish
```

---

### `rocky docs`

Generate project documentation. By default Rocky writes a static site with a page for each model and source, search, and an interactive lineage graph. `--format parquet` writes the same facts as Parquet tables that DuckDB can query.

```bash
rocky docs [flags]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--models <PATH>` | `models` | Models directory. |
| `--output-path <PATH>` | `docs/site` | Output directory. A path that ends in `.html` writes one single-page catalog instead. |
| `--format <FORMAT>` | `site` | `site` or `parquet`. |
| `--contracts <PATH>` | | Directory of `<model>.contract.toml` files to show, in addition to contracts beside a model. |
| `--var <NAME=VALUE>` | | Run variable for the offline compile, as in `rocky compile --var`. Repeatable. |
| `--select`, `--exclude`, `--state-ref`, `--state-working-tree` | (all) | Narrow the documented models. See [node selection](/reference/node-selection/). |

```bash
rocky docs                                       # site in docs/site/
rocky docs --output-path catalog.html            # one self-contained page
rocky docs --format parquet --output-path meta   # Parquet tables in meta/
```

The site shows:

- **Overview.** Counts, a filterable model table, and the sources.
- **Model pages.** Description, target, source file, strategy, tags, freshness, access and owner. A column table with type, nullability, classification, description and the columns each column feeds or reads. Tests, the contract, upstream and downstream models, and the SQL.
- **Source pages.** External tables that models read, with the columns used.
- **Lineage.** One interactive graph. Click a node to highlight its upstream and downstream. Click a column to list its column-level lineage.
- **Search.** Press `/` on any page. It matches model names, column names and descriptions.

How it behaves:

- Column types, nullability and lineage come from the same offline compile `rocky compile` runs.
- The site needs no server and makes no network request. Open `index.html` from disk, or host the directory anywhere.
- A rerun replaces the `.html` files under `models/` and `sources/`, so a deleted model leaves no page. Other files stay.
- When the project does not compile, `rocky docs` warns and renders without column types and lineage. It does not fail.

`--format parquet` writes eight files. Every file exists even when it has no rows.

| File | One row per | Main columns |
|------|-------------|--------------|
| `models.parquet` | model | `name`, `target`, `strategy`, `description`, `file`, `sql`, `access`, `freshness_max_lag_seconds`, `tags`, `has_contract` |
| `columns.parquet` | model output column | `model`, `ordinal`, `name`, `data_type`, `nullable`, `description`, `classification` |
| `edges.parquet` | model-level dependency | `upstream`, `downstream`, `upstream_kind` (`model` or `source`) |
| `column_lineage.parquet` | column-level lineage edge | `source_model`, `source_column`, `target_model`, `target_column`, `transform` |
| `tests.parquet` | declared test | `model`, `kind`, `column_name`, `severity`, `params`, `filter` |
| `contracts.parquet` | contract constraint | `model`, `kind` (`column`, `required`, `protected`, `no_new_nullable`), `column_name`, `type_name`, `nullable` |
| `sources.parquet` | external table | `name`, `columns_read`, `used_by_count` |
| `consumers.parquet` | downstream consumer and model it reads | `consumer`, `kind`, `owner`, `url`, `description`, `model` |

```sql
-- Which models read raw.orders.amount, directly or through other models?
WITH RECURSIVE reach(model, col) AS (
  SELECT target_model, target_column
  FROM 'meta/column_lineage.parquet'
  WHERE source_model = 'raw.orders' AND source_column = 'amount'
  UNION
  SELECT l.target_model, l.target_column
  FROM 'meta/column_lineage.parquet' l JOIN reach r
    ON l.source_model = r.model AND l.source_column = r.col
)
SELECT DISTINCT model FROM reach;

-- Columns tagged pii that no test covers.
SELECT c.model, c.name
FROM 'meta/columns.parquet' c
LEFT JOIN 'meta/tests.parquet' t
  ON t.model = c.model AND t.column_name = c.name
WHERE c.classification = 'pii' AND t.model IS NULL;
```

The JSON output reports what was written. `format` is `site`, `html` or `parquet`. `files` lists the files, relative to `output_path`.

```json
{
  "version": "1.80.0",
  "command": "docs",
  "output_path": "docs/site",
  "models_count": 12,
  "pipelines_count": 2,
  "duration_ms": 15,
  "format": "site",
  "sources_count": 3,
  "files": ["index.html", "lineage.html", "assets/site.css", "models/orders.html"]
}
```

---

### `rocky shell`

Open an interactive SQL shell against the configured warehouse. It keeps a command history and accepts multi-line queries. End a statement with `;` to run it.

```bash
rocky shell [--pipeline <NAME>]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--pipeline <NAME>` | | Pipeline whose warehouse adapter to use. |

| Meta-command | Description |
|---------|-------------|
| `.tables` | List the tables in the current catalog and schema. |
| `.schema <table>` | Describe the columns of a table. |
| `.quit` / `.exit` | Exit the shell. |

---

### `rocky estimate`

Estimate what each transformation model would cost before you run it. Rocky generates each model's SQL and asks the warehouse to `EXPLAIN` it. Nothing materializes. It does not price replication tables or the rest of a run.

```bash
rocky estimate [flags]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--models <PATH>` | `models` | Models directory. |
| `--model <NAME>` | (all) | Estimate one model. |
| `--pipeline <NAME>` | | Pipeline name. Required when more than one pipeline is defined. |
| `--verbose` | off | Also print the full `EXPLAIN` plan, the pricing rates, and any models skipped before `EXPLAIN`. |

```bash
rocky estimate --model fct_orders --verbose
```

**Where the prices come from.** Rocky has one built-in rate table each for Databricks, Snowflake, BigQuery and DuckDB. It uses the one for the pipeline's target adapter. Any other adapter type uses the Databricks rates, and `--verbose` labels that as a fallback. `rocky estimate` does not read the [`[cost]`](/reference/configuration/#cost) block. For a recommendation, use [`rocky optimize`](/reference/commands/administration/#rocky-optimize).

**An unknown `--model` fails.** The command exits `1` when no model has that name. Stderr reads `model '<NAME>' not found (no transformation model with that name)`. Stdout stays empty, even under `--output json`.

**An empty result says why.** A run that produces no estimate exits `0`. Its JSON gains a `message` field, which is absent when `estimates` is not empty.

| Situation | `message` |
|---|---|
| The project has no models to estimate | `no models found to estimate` |
| SQL generation or `EXPLAIN` failed for every selected model | `no model produced an estimate` |

:::caution[This is a behavior change]
Earlier engine versions exited `0` for an unknown `--model` and returned an empty `estimates` array with no `message`. A CI job that treated the empty array as a pass now fails on a bad selector.
:::

---

### `rocky bench`

Run Rocky's built-in performance benchmarks, and compare a run against a saved baseline. Requires the DuckDB feature, which the shipped binary has.

```bash
rocky bench [group] [flags]
```

| Argument or flag | Default | Description |
|------|---------|-------------|
| `group` | `all` | `compile`, `dag`, `sql_gen`, `startup` or `all`. `all` runs `compile`, `dag` and `sql_gen`, not `startup`. |
| `--models <N>` | | Number of models to generate for the compile benchmarks. |
| `--format <FORMAT>` | `table` | `json` for machine-readable output. |
| `--save <PATH>` | | Write the results to a JSON baseline file. |
| `--compare <PATH>` | | Compare the results against a saved baseline file. |

```bash
rocky bench --save baseline.json
rocky bench --compare baseline.json
```

---

### `rocky watch`

Watch the models directory and recompile on every change. Rocky uses the platform's file notifications and waits for writes to settle before it compiles. It prints the diagnostics to the terminal.

```bash
rocky watch [flags]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--models <PATH>` | `models` | Models directory to watch. |
| `--contracts <PATH>` | | Contracts directory. |

```bash
rocky watch --models src/models/ --contracts contracts/
```

---

### `rocky fmt`

Format `.rocky` DSL files. Rocky normalizes indentation and trims trailing whitespace. For `.sql` files, use [`rocky lint`](/reference/commands/modeling/#rocky-lint).

```bash
rocky fmt [paths]... [--check]
```

| Argument or flag | Default | Description |
|----------|---------|-------------|
| `paths` | `.` | Files or directories to format. |
| `--check` | off | Change nothing. Exit non-zero if any file needs formatting. For CI. |

```bash
rocky fmt --check models/
```
