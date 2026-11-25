---
title: Data Governance
description: Configure schema patterns, data contracts, permissions, tagging, quality checks, and audit trails.
sidebar:
  order: 17
---

Governance in Rocky is declarative. Write the rules in `rocky.toml` or a model sidecar, and `rocky apply` carries them out. Two features run as standalone commands, so you can gate a PR on them: `rocky compliance` and `rocky retention-status`.

| You get | You write | Rocky does |
|---|---|---|
| Schema routing ([1](#1-schema-patterns)) | `schema_pattern` and target templates | Routes each source schema to a catalog and schema. |
| Contracts ([2](#2-data-contracts)) | `contracts/*.contract.toml` | Fails the compile on a broken schema agreement. |
| Grants ([3](#3-grants-pillar-1-of-5)) | `governance.grants` | Issues a `GRANT` on every apply. It never revokes. |
| Masking ([4](#4-column-classification-and-masking-pillar-2-of-5)) | `[classification]` and `[mask]` | Obscures tagged columns (Databricks only today). |
| Compliance ([5](#5-compliance-rollup-pillar-3-of-5)) | Nothing | `rocky compliance` reports unmasked classified columns. |
| Roles ([6](#6-role-graph-reconciliation-pillar-4-of-5)) | `[role.<name>]` | Flattens the hierarchy and reconciles it. |
| Retention ([7](#7-data-retention-pillar-5-of-5)) | `retention = "90d"` | Writes the warehouse-native property. |
| Isolation, tags, groups ([8](#8-workspace-isolation)-[11](#11-model-tags)) | `governance.isolation`, `tags`, `group` | Binds workspaces, tags objects, enforces shared config. |
| Checks and audit ([12](#12-quality-checks)-[13](#13-audit-trail)) | `[pipeline.*.checks]` | Checks each copied table and records every run. |

Each one runs at a different moment. This is where they fire:

```
  config load        compile           rocky apply
  ───────────        ───────           ───────────────────────────────
  role graph         data contracts    during the run:
  validated          checked             catalogs + schemas created
  (cycles, unknown                       grants applied
   parents)                              tags applied
  mask strategy                          quality checks, per copied
  names checked                          table (replication)
                                       after the DAG completes:
                                         classification + masking
                                         retention
                                         role graph reconciled
                                       every run:
                                         audit record in the state store

  Any time:  rocky compliance     rocky retention-status
```

## 1. Schema Patterns

A schema pattern maps a source schema name onto a target catalog and schema. Patterns let one pipeline route many tenants. [Schema patterns](/concepts/schema-patterns/) has the parsing rules, multi-region examples, and error cases.

The pattern lives on the pipeline source. The templates live on the pipeline target. Both use the same component names:

```toml
[pipeline.bronze.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["client", "regions...", "connector"]

[pipeline.bronze.target]
adapter = "prod"
catalog_template = "{client}_warehouse"
schema_template = "staging__{regions}__{connector}"
```

Source schema `src__acme__us_west__shopify` lands in `acme_warehouse.staging__us_west__shopify`. The `...` suffix captures one or more segments. Rocky joins them with the separator in the target schema.

## 2. Data Contracts

A data contract is a [compile-time contract](/reference/glossary/#compile-time-contract): a schema agreement Rocky checks before any row is written. It declares which columns must exist, what type each one has, and which columns nobody may remove.

### Create a contract

Create a `.contract.toml` file in the `contracts/` directory. Name the file after the model:

```toml
# contracts/fct_daily_revenue.contract.toml

[[columns]]
name = "order_date"
type = "Date"
nullable = false

[[columns]]
name = "revenue"
type = "Decimal"
nullable = false

[rules]
required = ["order_date", "revenue"]
protected = ["order_date", "revenue"]
```

### Contract rules

| Rule | Description |
|---|---|
| **required** | Column must exist in the model's output with the specified type. Compilation fails if missing or wrong type. |
| **protected** | Column cannot be removed from the model in future changes. If a protected column disappears, compilation fails with error `E013`. |
| **nullable** | When `false`, the compiler verifies the column is non-nullable in the type system. |

### Compile with contracts

```bash
rocky compile --models models --contracts contracts
```

A violation is a compiler error:

```
  error[E011]: column 'revenue' type mismatch: contract expects Decimal, got String
    = help: CAST `revenue` to Decimal in the SELECT, or update the contract's expected type

  error[E013]: protected column 'revenue' has been removed
    = help: restore `revenue` in the SELECT, or remove it from `[rules] protected`
```

Run the same check in CI with `rocky ci --models models --contracts contracts`.

## 3. Grants (Pillar 1 of 5)

You declare the permissions you want in `rocky.toml`. Rocky issues a `GRANT` for each declared permission on every `rocky apply`. Rocky does not read the current grants and never revokes one.

### Catalog-level grants

Rocky applies these to every catalog the pipeline manages:

```toml
[[pipeline.bronze.target.governance.grants]]
principal = "data_engineers"
permissions = ["USE CATALOG", "MANAGE"]

[[pipeline.bronze.target.governance.grants]]
principal = "analysts"
permissions = ["BROWSE", "USE CATALOG"]

[[pipeline.bronze.target.governance.grants]]
principal = "ml_team"
permissions = ["BROWSE", "USE CATALOG", "SELECT"]
```

### Schema-level grants

Rocky applies these to every schema the pipeline manages:

```toml
[[pipeline.bronze.target.governance.schema_grants]]
principal = "data_engineers"
permissions = ["USE SCHEMA", "SELECT", "MODIFY"]

[[pipeline.bronze.target.governance.schema_grants]]
principal = "analysts"
permissions = ["USE SCHEMA", "SELECT"]
```

### How Rocky applies grants

For each catalog and schema it manages, `rocky apply` issues one `GRANT` per declared permission:

```sql
GRANT SELECT ON CATALOG `acme_warehouse` TO `analysts`;
GRANT USE SCHEMA ON SCHEMA `acme_warehouse`.`staging__us_west__shopify` TO `analysts`;
```

:::caution[Rocky never revokes]
Removing a grant from `rocky.toml` leaves it on the warehouse. A person must revoke an unwanted grant by hand. `grants_revoked` in the run output is always `0`.
:::

Catalog grants run only with `auto_create_catalogs = true`. Schema grants run only with `auto_create_schemas = true`. See [Permissions](/reference/permissions/).

### Managed vs skipped permissions

| Managed (Rocky controls) | Skipped (Rocky ignores) |
|---|---|
| `BROWSE` | `OWNERSHIP` |
| `USE CATALOG` | `ALL PRIVILEGES` |
| `USE SCHEMA` | `CREATE SCHEMA` |
| `SELECT` | |
| `MODIFY` | |
| `MANAGE` | |

Rocky never grants a skipped permission. It therefore cannot disturb ownership or admin-level grants.

### Principal validation

A principal name must match the pattern `^[a-zA-Z0-9_ \-\.@]+$`. Rocky wraps every principal in backticks in the SQL it generates, so ``GRANT USE CATALOG ON CATALOG acme_warehouse TO `data engineers` `` works.

## 4. Column Classification and Masking (Pillar 2 of 5)

A classification tag marks a column as sensitive. A masking strategy decides how Rocky obscures it. The two are separate, so you can tag a column before you pick a policy. One project-level block maps every tag to a strategy, with per-environment overrides:

```
  models/customers.toml            rocky.toml
  ┌─────────────────────┐          ┌────────────────────┐
  │ [classification]    │  tag     │ [mask]             │  strategy
  │ pii_email = "pii" ──┼────────► │ pii = "hash" ──────┼─────────►
  │                     │  name    │ [mask.prod]        │  SQL Rocky
  └─────────────────────┘          │ pii = "none"       │  emits
                                   └────────────────────┘
```

Implemented on Databricks today. Other adapters do nothing.

### Tag columns in the model sidecar

A `[classification]` block in the model's `.toml` sidecar maps each column name to a free-form tag string. Rocky enforces no fixed vocabulary:

```toml
# models/customers.toml
name = "customers"

[classification]
pii_email = "pii"
phone = "pii"
ssn = "confidential"
home_address = "pii"
```

Rocky matches each tag string against the project-level `[mask]` block to pick a strategy.

### Map tags to masking strategies

The `[mask]` block in `rocky.toml` binds each tag to a strategy. A scalar sets the workspace default. A `[mask.<env>]` table overrides it for one environment:

```toml
[mask]
pii = "hash"             # default: SHA-256 hash of the value
confidential = "redact"  # default: replace with '***'

[mask.prod]
pii = "none"             # prod override: do not mask pii
confidential = "partial" # keep first/last 2 chars, mask the middle
```

`RockyConfig::resolve_mask_for_env` takes the top-level scalars as defaults, then overlays the matching `[mask.<env>]` table. With no environment, only the defaults apply.

### Supported strategies

| Strategy | Emitted SQL behaviour |
|---|---|
| `"hash"` | SHA-256 hash of the column value. |
| `"redact"` | Replace with the literal `'***'`. |
| `"partial"` | Keep the first and last 2 characters; mask the middle. |
| `"none"` | Explicit identity -- no masking applied. Counts as masked for compliance. |

`rocky preview rows` applies `hash` and `partial` on Databricks, Snowflake and DuckDB only. On other adapters it refuses with `unmaskable_column` and returns no rows. `redact` works everywhere. See [`rocky preview rows`](/reference/commands/modeling/#rocky-preview-rows).

An unknown strategy name, such as `"mask"` or `"obfuscate"`, fails at config-load time.

### Allowed unmasked tags

List tags that are only for discovery and lineage, and need no `[mask]` strategy, in the `[classifications]` block:

```toml
[classifications]
allow_unmasked = ["internal", "public"]
```

A tag listed there silences the `W004` "tag has no masking strategy" compiler warning. The list is advisory: it enforces nothing.

### How apply works

After the DAG completes, `rocky apply` walks each model's `[classification]` block and calls the governance adapter's `apply_column_tags` and `apply_masking_policy` hooks. Both are best-effort: a failure emits `warn!` and the pipeline continues.

On Databricks, Rocky writes Unity Catalog column tags and issues `CREATE MASK` / `SET MASKING POLICY`, **one statement per column**, because Unity Catalog rejects multi-column masking DDL. BigQuery, Snowflake, and DuckDB do nothing here.

The [configuration reference](/reference/configuration/) has the full schema of the `[mask]` and `[classifications]` blocks.

## 5. Compliance Rollup (Pillar 3 of 5)

`rocky compliance` answers one question: **is every classified column masked wherever policy says it should be?**

It resolves the classifications against the masks in your configuration. It makes no warehouse call.

### Basic usage

```bash
rocky compliance
```

```
Compliance report (env: <all>)
  models scanned:       42
  classified columns:   87
  with strategy:        84
  exceptions:           3

EXCEPTIONS:
  customers.pii_email    (prod)  no strategy for classification 'pii'
  orders.card_last_four  (prod)  no strategy for classification 'financial'
  users.ssn              (dev)   no strategy for classification 'confidential'
```

### Flags

| Flag | Purpose |
|---|---|
| `--env <name>` | Scope the report to a single environment. Without it, Rocky expands across the defaults plus every `[mask.<env>]` override. |
| `--exceptions-only` | Filter the `per_column` table to rows that produced at least one exception. The `exceptions` list itself is always shown. |
| `--fail-on exception` | Exit with code `1` when any exception is emitted. Without it the exit code stays `0`, whether or not exceptions exist. Wire this into CI to block merges that leave classified columns unmasked. |
| `--models <dir>` | Models directory to scan (defaults to `models/`). |

### How `none` counts

`MaskStrategy::None`, the explicit identity strategy, counts as **masked**. "Do not mask" is a policy decision, not a gap. An exception means a tag has no entry in `[mask]` at all. `[classifications] allow_unmasked = [...]` suppresses exceptions for tags you left out on purpose. It does not claim those columns are protected.

### JSON output

```bash
rocky compliance --env prod --output json
```

The payload follows the `ComplianceOutput` schema: a `summary` block of counters, a `per_column` array, and an `exceptions` array. Point tooling at the JSON, not the text table.

## 6. Role-Graph Reconciliation (Pillar 4 of 5)

You declare roles that inherit from other roles. Rocky flattens the hierarchy into one resolved permission set per role. It rejects cycles and unknown parents at config-load time, on every adapter.

### Declare roles in `rocky.toml`

```toml
[role.reader]
permissions = ["SELECT", "USE CATALOG", "USE SCHEMA"]

[role.analytics_engineer]
inherits = ["reader"]
permissions = ["MODIFY"]

[role.admin]
inherits = ["analytics_engineer"]
permissions = ["MANAGE"]
```

Each `[role.<name>]` block takes two keys:

- `inherits` -- the immediate parent roles. Rocky walks them transitively.
- `permissions` -- canonical Rocky permission strings (`"SELECT"`, `"USE CATALOG"`, `"MODIFY"`, `"MANAGE"`, and so on).

An empty `permissions` list makes a role a grouping node that other roles inherit from.

### Resolution semantics

`RockyConfig::role_graph()` flattens the `[role.*]` map into a deterministic `name → ResolvedRole` map:

1. Walk the `inherits` DAG depth-first, detecting cycles.
2. Union this role's `permissions` with the `permissions` of every ancestor.
3. Reject an unknown parent, such as `inherits = ["nonexistent_role"]`.
4. Reject an unknown permission string.

### Databricks reconcile

`reconcile_role_graph` on Databricks validates each flattened role's `rocky_role_<name>` principal syntax. With a SCIM client configured, it then runs two passes:

- **Pass 1** — create one `rocky_role_<name>` SCIM group per role, best-effort per role.
- **Pass 2** — emit `GRANT <permission> ON CATALOG ...` for every `(role, catalog, permission)` triple.

Both passes only add. Rocky revokes no grant and deletes no group, so removing a role or a permission from `rocky.toml` needs manual cleanup. Without a SCIM client, the adapter runs **log-only**: it validates the graph, emits `debug!` events and touches nothing in the warehouse. Every other adapter does nothing here.

## 7. Data Retention (Pillar 5 of 5)

A retention policy sets how long the warehouse keeps a table's historical data. You write one sidecar key. Each adapter translates it into the warehouse's own property.

### Declare retention on a model

A model sidecar takes a top-level `retention` key:

```toml
# models/events_daily.toml
name = "events_daily"
retention = "90d"   # grammar: \d+[dy] -- days or years
```

`<N>d` is N days. `<N>y` is N years, flat-multiplied to 365 days each. Rocky rejects a malformed value such as `"abc"`, `"90"`, or `"-3d"` with `ModelError::InvalidRetention`. With no `retention` key, Rocky manages no retention for that model.

### Adapter translation

| Adapter | Translation |
|---|---|
| **Databricks** | Paired Delta TBLPROPERTIES: `delta.logRetentionDuration = '<N> days'` and `delta.deletedFileRetentionDuration = '<N> days'`. Applied via `ALTER TABLE ... SET TBLPROPERTIES`. |
| **Snowflake** | `DATA_RETENTION_TIME_IN_DAYS = <N>` via `ALTER TABLE ... SET`. |
| **BigQuery** | Default-unsupported. No built-in retention setting; sidecar ignored with a `warn!`. |
| **DuckDB** | Default-unsupported. Sidecar ignored with a `warn!`. |

Rocky applies retention after the DAG completes, in the same best-effort loop as masking. A failure emits `warn!` and never aborts the run.

### Inspecting configured retention: `rocky retention-status`

```bash
rocky retention-status
```

```
MODEL              CONFIGURED   WAREHOUSE   IN SYNC
──────────────────────────────────────────────────────
events_daily       90 days      -           no
orders             365 days     -           yes
customers          -            -           yes
```

Without `--drift`, Rocky does not probe the warehouse, and the `WAREHOUSE` column stays `-`.

Flags:

| Flag | Purpose |
|---|---|
| `--models <dir>` | Models directory (defaults to `models/`). |
| `--model <name>` | Scope the report to a single model. |
| `--drift` | Probe the warehouse for the applied retention, fill `warehouse_days`, and filter the report to models with a declared policy. |

### `--drift` probes the warehouse

With `--drift`, Rocky reads the retention the warehouse currently applies, fills `warehouse_days` and recomputes `in_sync`. The probe works on Databricks and Snowflake only. On DuckDB and BigQuery `warehouse_days` stays empty. A probe error prints per model on stderr and does not fail the command.

## 8. Workspace Isolation

Rocky can restrict a catalog to named Databricks workspaces through the Unity Catalog workspace bindings API. Each binding names a workspace ID and an access level, either `READ_WRITE` or `READ_ONLY`.

```toml
[pipeline.bronze.target.governance.isolation]
enabled = true

[[pipeline.bronze.target.governance.isolation.workspace_ids]]
id = 123456789
binding_type = "READ_WRITE"

[[pipeline.bronze.target.governance.isolation.workspace_ids]]
id = 987654321
binding_type = "READ_ONLY"
```

`binding_type` defaults to `"READ_WRITE"` when you omit it. Rocky maps the two values to the Databricks API values `BINDING_TYPE_READ_WRITE` and `BINDING_TYPE_READ_ONLY`.

With isolation enabled, Rocky does two things:

1. Sets each managed catalog's isolation mode to `ISOLATED`, with `PATCH /api/2.1/unity-catalog/catalogs/{name}`
2. Binds each catalog to the listed workspaces at their declared access level, with `PATCH /api/2.1/unity-catalog/bindings/catalog/{name}`

No other workspace can then reach the catalog. Only a listed workspace can read it, and only a `READ_WRITE` one can write to it.

Use isolation when separate workspaces must not reach each other's catalogs, for example dev against production. Isolation is best-effort. If the API call fails, Rocky logs a warning and continues the run.

## 9. Tagging Strategy

A tag is a key-value pair. Rocky writes tags onto catalogs, schemas, and tables with Databricks `ALTER ... SET TAGS` SQL.

### Configuration

```toml
[pipeline.bronze.target.governance.tags]
managed_by = "rocky"
data_owner = "analytics-team"
environment = "production"
cost_center = "CC-1234"
```

### What gets tagged

`rocky apply` writes tags at three levels:

| Level | SQL | Applied Tags |
|---|---|---|
| Catalogs | `ALTER CATALOG ... SET TAGS (...)` | Governance tags + parsed schema components |
| Schemas | `ALTER SCHEMA ... SET TAGS (...)` | Governance tags + parsed schema components |
| Tables | `ALTER TABLE ... SET TAGS (...)` | Governance tags only |

### Example generated SQL

```sql
ALTER CATALOG acme_warehouse SET TAGS (
    'managed_by' = 'rocky',
    'data_owner' = 'analytics-team',
    'environment' = 'production',
    'client' = 'acme'
);

ALTER SCHEMA acme_warehouse.staging__us_west__shopify SET TAGS (
    'managed_by' = 'rocky',
    'data_owner' = 'analytics-team',
    'connector' = 'shopify',
    'regions' = 'us_west'
);
```

### Using tags for discovery

Rocky finds its own catalogs by the tag `managed_by = "rocky"`, which it reads from `system.information_schema.catalog_tags`. Always set it.

## 10. Config Groups and Enforcement

A [config group](/reference/glossary/#config-group) is one definition that models opt into with `group = "<name>"` in each sidecar. It supplies shared routing (`schema_template`) and a shared `strategy`. [The model format guide](/reference/model-format/#config-groups) is the full reference.

### Enforced config groups

A group is an overridable default: a member can pin its own `target.schema` or `strategy`, and the local value wins. Set `enforce = true` and the group becomes an [enforced group](/reference/glossary/#enforced-group). A member that pins a field the group controls then fails to load:

```toml
# models/groups/regulated.toml
enforce = true
schema_template = "mart_{region}"

[strategy]
type = "merge"
unique_key = ["id"]
```

Enforcement covers two fields: the target `schema` (when the group sets `schema_template`) and the `strategy`. A member that sets either fails the load with a `GroupOverride` error. A member may still supply its own `[args]` to fill the template, and set any other field, such as `target.catalog`.

The check runs when the model graph loads, before any SQL reaches the warehouse. It covers every member, in SQL or in the `.rocky` DSL. The group governs routing and materialization, never the model body. Enforcement is opt-in.

## 11. Model Tags

A model tag is a free-form attribute for a whole model: `domain`, `tier`, `owner`. Model tags are not the [tagging strategy](#9-tagging-strategy). They live in the model sidecar or its config group, and flow into the model graph, the orchestrator's asset tags and the `rocky compile` JSON.

### Sidecar `[tags]`

Declare model tags in a `[tags]` block in the model's `.toml` sidecar. Keys and values are free-form strings:

```toml
# models/fct_orders.toml
name = "fct_orders"

[tags]
domain = "finance"
tier = "gold"
owner = "data-eng"
```

### Config-group `[tags]` baseline

A config group can declare a `[tags]` block. Every member inherits it:

```toml
# models/groups/finance.toml
schema_template = "mart_{region}"

[tags]
domain = "finance"
tier = "gold"
```

### Sidecar over group, per key

Rocky merges the model's own `[tags]` over the group's `[tags]`, key by key. A model in the `finance` group above can set `tier = "silver"` and still inherit `domain = "finance"`.

### Projection to Dagster

`rocky compile --output json` emits the resolved tags as `models_detail[].tags`. The `dagster-rocky` integration projects them onto each derived asset as Dagster tags, so you can select assets with, for example, `tag:domain=finance`. The translator adds its own `rocky/`-namespaced tags for the model name, target catalog, target schema and strategy, so they never collide with your keys.

### Per-model warehouse tags: `[governance.tags]`

Model `[tags]` never reach the warehouse. To write a tag onto a model's own target table or view in Unity Catalog, declare a `[governance.tags]` block in the sidecar:

```toml
# models/fct_orders.toml
name = "fct_orders"

[governance.tags]
domain = "finance"
tier = "gold"
```

After the model materializes, `rocky apply` emits `ALTER VIEW ... SET TAGS (...)` for a view-format model and `ALTER TABLE ... SET TAGS (...)` otherwise. It writes your keys and values verbatim, with no prefix. The three tag surfaces are independent:

| Block | Where it lives | What it does |
|---|---|---|
| `[tags]` | Model sidecar / config group | Dagster asset tags + `rocky compile` JSON. Never written to the warehouse. |
| `[governance.tags]` | Model sidecar | `ALTER VIEW/TABLE ... SET TAGS` on the model's own securable, post-materialize. |
| `[pipeline.*.target.governance.tags]` | Pipeline target | `ALTER CATALOG/SCHEMA/TABLE ... SET TAGS` during replication, used for catalog discovery. |

`[governance.tags]` is best-effort, like classification and retention: a failure warns and never aborts the run. Rocky skips an empty block, because Unity Catalog rejects `SET TAGS ()`.

## 12. Quality Checks

Rocky runs quality checks inside a replication run, as soon as each table is copied. [Data quality checks](/concepts/data-quality-checks/) has the JSON result shapes, the anomaly rules, and the freshness edge cases.

```toml
[pipeline.bronze.checks]
enabled = true
row_count = true
column_match = true
freshness = { error_after = "24h" }
anomaly_threshold_pct = 50.0
null_rate = { columns = ["email", "phone"], threshold = 0.05, sample_percent = 10 }

[[pipeline.bronze.checks.custom]]
name = "no_future_dates"
sql = "SELECT COUNT(*) FROM {target} WHERE order_date > CURRENT_DATE()"
threshold = 0
```

| Check | What it compares |
|---|---|
| Row count | `COUNT(*)` on the source against the target. Rocky batches tables into `UNION ALL` queries, 200 per batch. |
| Column match | Source and target column sets, ignoring case. Reports missing and extra columns. Costs two extra metadata queries per table. |
| Freshness | `MAX(timestamp_column)` against now. Fails when the newest row is older than `error_after` (error severity) or `warn_after` (warning severity). |
| Null rate | Null percentage per column, from a `TABLESAMPLE`. `sample_percent` keeps it fast on a large table. |
| Anomaly | Row count against a moving average of past runs. Flags a change above `anomaly_threshold_pct`. |
| Custom | Your SQL, with a `{target}` placeholder. Passes when the result is at most `threshold`. |

## 13. Audit Trail

Rocky keeps run history and quality metrics in its embedded [state store](/reference/glossary/#state-store). Every `rocky apply` stamps eight governance fields onto its `RunRecord`. `rocky history --audit` shows them.

### `rocky history --audit` and the 8 audit fields

Plain `rocky history` stays compact. Pass `--audit` to expand every governance field, in text or JSON:

```bash
rocky history --audit
rocky history --audit --output json
```

Each `RunRecord` carries:

| Field | Source |
|---|---|
| `triggering_identity` | Auth principal that kicked off the run. |
| `session_source` | Auto-detected: `Cli` / `Dagster` / `Lsp` / `HttpApi`. |
| `git_commit` | Resolved at run start from the current repo. |
| `git_branch` | Resolved at run start from the current repo. |
| `idempotency_key` | Echoed from `rocky plan --idempotency-key <KEY>` (or the single-step `rocky run --idempotency-key` alias) when passed. |
| `target_catalog` | The catalog(s) the run wrote to. |
| `hostname` | The host that executed the run. |
| `rocky_version` | The CLI version that produced the record. |

### Rows from older stores

A run recorded before the audit fields shows `hostname = "unknown"`, `rocky_version = "<pre-audit>"` and `session_source = Cli` under `rocky history --audit`. Rocky never rewrites a stored row.

### Read history and metrics

| Command | Shows |
|---|---|
| `rocky history` | Recent runs: id, start, status, model count, trigger. |
| `rocky history --since 2026-03-29` | Runs on or after a date. |
| `rocky history --model fct_daily_revenue` | One model's executions: duration, rows, status, SQL hash. |
| `rocky metrics fct_daily_revenue` | The latest quality snapshot: row count, freshness lag, null rates. |
| `rocky metrics fct_daily_revenue --trend` | Row count and freshness for each run. |
| `rocky metrics fct_daily_revenue --column email --alerts` | Metrics for one column, with alerts. |
| `rocky metrics fct_daily_revenue --alerts` | Quality alerts for the model. |

`--alerts` prints lines such as `[WARNING] null rate 25.0% exceeds 20% threshold (column: phone)`.

Alert severity levels:
- **critical**: the null rate exceeds 50%
- **warning**: the null rate exceeds 20%, or the freshness lag exceeds 24 hours

### JSON output

Every history and metrics command also takes `-o json`.

## 14. Where Each Block Lives

Pipeline blocks sit under each pipeline, so two pipelines can carry different policies. Project blocks sit at the top of `rocky.toml`. Model blocks sit in the sidecar.

| Level | Block | Section |
|---|---|---|
| Pipeline target | `[pipeline.<name>.target.governance]`: `auto_create_*`, `tags`, `grants`, `schema_grants`, `isolation` | 3, 8, 9 |
| Pipeline | `[pipeline.<name>.checks]` | 12 |
| Project | `[mask]`, `[mask.<env>]`, `[classifications]`, `[role.<name>]` | 4, 6 |
| Model sidecar | `[classification]`, `retention`, `[tags]`, `[governance.tags]`, `group` | 4, 7, 10, 11 |
| Contracts | `contracts/<model>.contract.toml` | 2 |

The smallest working target block turns on both `auto_create_*` switches. Grants need them (see [section 3](#3-grants-pillar-1-of-5)):

```toml
[pipeline.bronze.target.governance]
auto_create_catalogs = true
auto_create_schemas = true
```

## 15. CI Gate Example

Wire `rocky compliance --fail-on exception` into CI to block a merge that leaves a classified column unmasked. For a quieter local run, drop `--fail-on` and add `--exceptions-only`.

### GitHub Actions

```yaml
# .github/workflows/rocky-compliance.yml
name: Rocky Compliance

on:
  pull_request:
    paths:
      - 'models/**'
      - 'rocky.toml'

jobs:
  compliance:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install Rocky
        run: |
          curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
          echo "$HOME/.local/bin" >> $GITHUB_PATH
      - name: Run compliance gate
        run: rocky compliance --env prod --fail-on exception
```

The gate exits `0` when every classified column has a strategy or appears in `allow_unmasked`. It exits `1` on the first exception.
