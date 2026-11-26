# Rocky user surface — reference

Read this when you work on behavior a Rocky user sees: CLI verbs, `rocky.toml`, model files, hooks.
It moved out of `engine/AGENTS.md` so that engine sessions do not load it every time.
For `rocky.toml` authoring, the root `rocky-config` skill is the full reference.
Paths below are relative to `engine/`.

## User Project Structure

```
my-project/
├── rocky.toml          # Pipeline configuration
└── models/                # Silver layer transformations (optional)
    ├── dim_customers.sql   # Pure SQL
    ├── dim_customers.toml  # Config (name, depends_on, strategy, target)
    ├── fct_orders.sql
    └── fct_orders.toml
```

## Model File Format (Silver Layer)

Each model is a pair: `name.sql` (pure SQL) + `name.toml` (config).

**TOML config:**
```toml
name = "fct_orders"
depends_on = ["stg_orders", "dim_customers"]

[strategy]
type = "merge"           # or "incremental" or "full_refresh"
unique_key = ["order_id"]

[target]
catalog = "analytics"
schema = "marts"
table = "fct_orders"
```

**SQL file:** Plain SQL, no templating. Use `{target}` in custom check SQL only.

## CLI Commands

### Core Pipeline
```bash
rocky init [path]                    # Scaffold a new project
rocky validate                       # Check config, schema patterns, models without APIs
rocky discover                       # Discover available schemas/tables (metadata only, no extraction)
rocky plan --filter key=value        # Preview SQL without executing (e.g., --filter client=acme)
rocky run --filter key=value         # Execute full pipeline: discover → drift → create → copy → check
rocky state                          # Show stored watermarks
```

### Everything else

Rocky has about 75 top-level verbs: modeling (`compile`, `lineage`, `test`, `ci`), AI (`ai`, `ai-explain`, `ai-sync`, `ai-test`), development (`playground`, `import-dbt`, `serve`, `lsp`, `mcp`, `init-adapter`), administration (`history`, `metrics`, `optimize`, `doctor`, …) and more. This file does not list them. Use `rocky --help` and `rocky <verb> --help`, or the grouped reference pages in `../docs/src/content/docs/reference/commands/`.

### Products (spec-driven fulfillment, gate side)
```bash
rocky product verify <name>      # Frozen propose_only posture + tag resolution + collisions (exit 0/1/2)
rocky product compile <name>     # Verify, then lower: Phase A contract / Phase B sidecar merge (staged commit)
rocky product approve <name>     # Human authority transition: immutable snapshot + one state-store txn
rocky product status <name>      # Read-only: spec, lowering, artifact verification, approval, loop state
rocky product list               # Read-only: one status row per product (spec dir ∪ state-store records), sorted
rocky product journal <name>     # Read-only: the fulfillment journal, every persisted transition in append order
rocky product clear-breach <name> --model <m>  # Delete an output's open volume-breach marker by hand (idempotent)
```

Product specs live at `products/<name>.toml`; `rocky validate` checks them offline (V050–V053). Reference: `../docs/src/content/docs/reference/commands/products.md`.

### Fulfillment (spec-driven fulfillment, loop side — EXPERIMENTAL)
```bash
rocky fulfill <name> [--retry]     # Drive the reconciler: elicit → approve → lower → draft (agent) → verify → propose → review → apply → observe
rocky fulfill approve-spec <name>  # Same authority transition as `rocky product approve` (one implementation, two spellings)
```

The loop lives in `crates/rocky-fulfill` (state machine + subprocess agent driver) and consumes `rocky-cli` ONLY through `commands/fulfill_api.rs`; configure it via `[fulfill.driver]` in `rocky.toml`. Reference: `../docs/src/content/docs/reference/commands/fulfill.md`.

Schema drift is not a standalone verb — there is no `rocky drift` command. Drift
detection runs inside `rocky run` / `rocky plan` (surfaced on `RunOutput.drift`);
see `drift.rs`.

Global flags: `--config` (default: `rocky.toml`), `--output` (`json`|`table`), `--state-path` (default: `models/.rocky-state.redb` beside the config file; an existing `.rocky-state.redb` in the current directory still works and emits a one-time deprecation warning)

## Schema Pattern

Rocky uses a configurable schema pattern to map source schemas to target catalogs/schemas:

- **Source:** `<source_catalog>.src__{tenant}__{regions...}__{source}.table`
- **Target:** `{catalog_template}.raw__{regions}__{source}.table`
- Example: `src__acme__us_west__shopify` -> `acme_warehouse.raw__us_west__shopify`

## Configuration

Rocky reads `rocky.toml`. Env vars substituted at parse time: `${VAR_NAME}` (with defaults: `${VAR:-default}`). Two top-level sections: `[adapter]` (warehouse connection) and `[pipeline.<name>]` (pipeline definition). For the full config reference, schema, and examples, see the `rocky-config` skill at the monorepo root (`.agents/skills/rocky-config/SKILL.md`).

Section reference:

- `[adapter]` — warehouse connection (type: duckdb/databricks/snowflake/bigquery/trino, auth, host, http_path). Unnamed `[adapter]` auto-wraps as `adapter.default`.
- `[pipeline.<name>]` — pipeline definition (strategy, timestamp_column, metadata_columns)
  - `[pipeline.<name>.source]` — type (fivetran/manual), API credentials, schema_pattern
  - `[pipeline.<name>.target]` — catalog_template, schema_template (with `{variable}` placeholders)
  - `[pipeline.<name>.checks]` — row_count, column_match, freshness, null_rate, anomaly_threshold_pct; plus `fail_on_error`, `[[assertions]]` blocks (DQX parity: `not_null`, `unique`, `accepted_values`, `relationships`, `expression`, `row_count_range`, `in_range`, `regex_match`, `aggregate`, `composite`, `not_in_future`, `older_than_n_days` — each supports `severity` and `filter`), and `[quarantine]` (`mode = "split" | "tag" | "drop"`)
  - `[pipeline.<name>.execution]` — concurrency, fail_fast, error_rate_abort_pct, table_retries
- `[pipeline.<name>.target.governance]` — auto_create_catalogs, auto_create_schemas, tags, isolation, grants (per target; there is no top-level `[governance]` table)
- `[cost]` — storage_cost_per_gb_month, compute_cost_per_dbu, warehouse_size
- `[state]` — backend (local/s3/gcs/valkey/tiered)
- `[cache.schemas]` — the schema cache: enabled, ttl_seconds, replicate, trusted_max_age_seconds, strict_sources (the last two drive the E041/W041 missing-source-column check, and `strict_sources` also escalates the W045 missing-source-table warning to E045; there is no other `[cache]` key)

**Key defaults** (omit when redundant):
- `pipeline.type` → `"replication"`, unnamed `[adapter]` → `adapter.default`
- `[state] backend = "local"`, `auto_create_catalogs = false`
- Model sidecar `name` → filename stem, `target.table` → `name`
- `models/_defaults.toml` provides directory-level `[target]` defaults

## Hooks System

Rocky supports lifecycle hooks configured in `rocky.toml`. Hooks fire shell commands or webhooks on pipeline events.

```toml
[hook.on_pipeline_start]
command = "scripts/notify.sh"
timeout_ms = 5000
on_failure = "warn"  # or "abort" or "ignore" (silent); "warn" is the default
```

`rocky run` ignores every hook outcome: each call site discards the result of `fire`, so a failing hook is logged and the run continues whatever `on_failure` says. Only `rocky hooks test` acts on `abort`. A hook cannot gate a run today.

Implementation in `crates/rocky-core/src/hooks/` (mod.rs, webhook.rs, template.rs, presets.rs).

## When helping users

- **Config issues** → check `rocky.toml` syntax, env vars, schema pattern
- **SQL generation** → `rocky plan` shows exactly what SQL will run
- **New tables not appearing** → check source type, schema prefix, connector status
- **Check failures** → look at JSON output `check_results` for details
- **DAG errors** → check `depends_on` in model TOML files for cycles or unknown refs
- **Drift** → safe type widenings become `ALTER COLUMN TYPE`; unsafe changes trigger full refresh
