# rocky.toml reference: pipeline examples and model files

This file covers a full example for each pipeline type, model sidecar files, time-interval models and the canonical example configs.

## Replication pipeline

```toml
[pipeline.raw]
# type = "replication"               # optional, this is the default
strategy         = "incremental"     # or "full_refresh" or "merge"
timestamp_column = "_fivetran_synced"
# merge_keys     = ["id"]            # required when strategy = "merge"
# merge_keys_fallback = ["id"]       # used when merge_keys is unset
metadata_columns = [
    { name = "_loaded_by", type = "STRING", value = "'rocky'" },
]

[pipeline.raw.source]
# adapter = "default"                # optional — first adapter by default
[pipeline.raw.source.schema_pattern]
prefix     = "src__"
separator  = "__"
components = ["client", "regions...", "connector"]
# "name"    = single segment
# "name..." = variable-length (1+)
# Reserved: `table` and `id` are not allowed as component names
# (they're consumed by `--filter` and `[[table_overrides]]`).

[pipeline.raw.target]
catalog_template = "{client}_warehouse"
schema_template  = "staging__{regions}__{connector}"

# Per-`(connector, table)` overrides on top of the pipeline defaults.
# Per-field most-specific-match-wins: each field is supplied by the
# most-specific matching rule that sets it; unrelated rules don't
# clobber it. See `engine/crates/rocky-core/src/config.rs` ::
# `TableOverride` for the full surface.

# Skip diagnostic tables on every connector.
[[pipeline.raw.table_overrides]]
match.table = "_diagnostics_*"        # `*`/`?` glob in TOML only — CLI literals
enabled     = false

# `pii_users` on one specific connector needs a composite merge key.
[[pipeline.raw.table_overrides]]
match.connector = "stripe_main"        # matches conn.id OR conn.schema
match.table     = "pii_users"
merge_keys      = ["user_id", "tenant_id"]

# `audit_log` is append-only — keep it on incremental even when
# the pipeline default is `merge`.
[[pipeline.raw.table_overrides]]
match.connector  = "stripe_main"
match.table      = "audit_log"
strategy         = "incremental"
timestamp_column = "occurred_at"
```

**CLI symmetry:** `--filter table=<literal>` filters to one table at a
time (no globs at the CLI; shell quoting is too fragile). The
connector-level `--filter id=<conn_id>` keys remain unchanged.

## Transformation pipeline

```toml
[pipeline.silver]
type       = "transformation"
models     = "models/silver/**"   # glob, relative to rocky.toml; default "models/**"
depends_on = ["raw"]

# There is no contracts key: a model's contract is auto-discovered as the
# sibling `<model>.contract.toml`, or passed at the CLI via `--contracts <dir>`.

# A transformation target takes an adapter ref (plus optional governance)
# and nothing else — `catalog`/`schema` here are parse errors
# (`deny_unknown_fields`). Each model names its own destination in its
# sidecar `[target]`:
#
#   # models/silver/dim_customers.toml
#   [target]
#   catalog = "analytics"
#   schema  = "marts"
#   table   = "dim_customers"
[pipeline.silver.target]
adapter = "warehouse"
```

Declare the external tables the models read, with optional dbt-style freshness
checked by `rocky freshness` (exit 1 on `error` / `runtime_error`):

```toml
[[pipeline.silver.sources]]
schema = "raw"                     # catalog optional (two-part names)
table  = "orders"

[pipeline.silver.sources.freshness]
loaded_at_field = "_loaded_at"     # DATE / TIMESTAMP column
warn_after      = "12h"            # <N>s | <N>h | <N>d; at least one of the two
error_after     = "24h"            # must be >= warn_after (else E050)
filter          = "status <> 'test'"   # optional WHERE predicate; no `;`
```

## Quality pipeline

```toml
[pipeline.qa]
type       = "quality"
depends_on = ["silver"]

# A quality target is an adapter ref only. The tables to check are listed
# in `[[pipeline.qa.tables]]` (omit `table` to check the whole schema).
[pipeline.qa.target]
adapter = "warehouse"

[[pipeline.qa.tables]]
catalog = "analytics"
schema  = "marts"

# Quality runs execute `row_count`, `custom`, and `assertions` only. Other
# check kinds are inert here — `rocky validate` warns (V034).
[pipeline.qa.checks]
row_count = true
```

## Snapshot pipeline

```toml
[pipeline.dim_history]
type       = "snapshot"
unique_key = ["customer_id"]     # required: row identity in the source table
updated_at = "updated_at"        # required: change-detection column
depends_on = ["silver"]

[pipeline.dim_history.source]
catalog = "analytics"
schema  = "marts"
table   = "dim_customers"

[pipeline.dim_history.target]
catalog = "analytics_history"
schema  = "snapshots"
table   = "dim_customers_history"   # required: explicit history table
```

## Model sidecar files (`.sql` + `.toml`)

Model files live under `models/` and use a sidecar pattern:

```toml
# models/marts/dim_customers.toml
name       = "dim_customers"
depends_on = ["stg_customers"]

[strategy]
type       = "merge"                 # e.g. full_refresh, merge, delete_insert, time_interval, view, snapshot; not incremental (E037)
unique_key = ["customer_id"]
# update_columns = ["name", "email"] # omit for UPDATE SET *

[target]
catalog = "analytics"
schema  = "marts"
table   = "dim_customers"

[[sources]]                          # optional: declare source tables
catalog = "analytics"
schema  = "staging"
table   = "customers"
```

```sql
-- models/marts/dim_customers.sql
-- Pure SQL. No Jinja. No templating.
SELECT customer_id, name, email, updated_at
FROM {{ analytics.staging.customers }}        -- Rocky expands refs at compile time, not via Jinja
```

Directory-level defaults via `models/<dir>/_defaults.toml`:

```toml
[target]
catalog = "analytics"
schema  = "marts"
```

## Time-interval (partition-keyed) materialization

For transformation models that need partition-by-partition execution:

```toml
# models/fact_events.toml
[strategy]
type              = "time_interval"
time_column       = "event_date"
granularity       = "day"                   # hour | day | month | year
lookback          = 7                       # re-run last 7 partitions
batch_size        = 4                       # partitions per concurrent batch
first_partition   = "2024-01-01"
```

Model SQL uses `@start_date` and `@end_date` placeholders — Rocky substitutes per partition:

```sql
SELECT * FROM raw.events
WHERE event_date >= @start_date AND event_date < @end_date
```

CLI: `rocky run --partition KEY` / `--from KEY --to KEY` / `--latest` / `--missing` / `--lookback N` / `--parallel N`.

## Full canonical example

See `examples/playground/pocs/00-foundations/01-replication-basics/rocky.toml` for the minimal DuckDB **replication** case (schema-pattern routing), `examples/playground/pocs/00-foundations/00-playground-default/rocky.toml` for the minimal **transformation** case (model DAG), or `engine/examples/multi-layer/rocky.toml` for a full Bronze/Silver/Gold setup.
