---
title: Data Governance
description: Configure schema patterns, data contracts, permissions, tagging, quality checks, and audit trails.
sidebar:
  order: 7
---

Rocky enforces data quality, schema stability, access control, masking, retention, and auditability. Most of it is declarative: write the rules in `rocky.toml` or in a model sidecar, and `rocky apply` carries them out. Two features run as standalone commands instead, so you can gate a PR on them. `rocky compliance` rolls up classification against masking, and `rocky retention-status` reports retention per model.

Five governance features do the enforcement. Each one names what Rocky does:

1. **Grants** -- Rocky reconciles catalog and schema ACLs against Unity Catalog on every apply.
2. **Column classification and masking** -- you tag columns in the model sidecar; project-level `[mask]` and `[mask.<env>]` blocks decide how Rocky obscures them.
3. **Compliance rollup** -- `rocky compliance` resolves tags against strategies and reports the gaps, without touching the warehouse.
4. **Role-graph reconciliation** -- Rocky flattens your `[role.<name>]` hierarchy and reconciles the result.
5. **Data retention** -- a model sidecar sets `retention = "<N>[dy]"`, and each adapter writes it as the warehouse-native property.

Each one runs at a different moment. This is where they fire:

```
  config load        compile           rocky apply
  ───────────        ───────           ───────────────────────────────
  role graph         data contracts    during the run:
  validated          checked             catalogs + schemas created
  (cycles, unknown                       grants reconciled
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

A schema pattern maps a source schema name onto a target catalog and schema. Patterns are what let one pipeline route many tenants.

### Configuration

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

### How parsing works

Given a source schema `src__acme__us_west__shopify`:

1. Rocky strips the prefix `src__`
2. Splits on the separator `__` to get segments: `["acme", "us_west", "shopify"]`
3. Maps segments to components:
   - `client` = `"acme"` (single segment)
   - `regions` = `["us_west"]` (variable-length, marked with `...`)
   - `connector` = `"shopify"` (terminal segment)
4. Resolves target templates:
   - `{client}_warehouse` becomes `acme_warehouse`
   - `staging__{regions}__{connector}` becomes `staging__us_west__shopify`

### Multi-region examples

The `...` suffix on `regions` captures one or more segments between the fixed components:

| Source Schema | client | regions | connector |
|---|---|---|---|
| `src__acme__us_west__shopify` | `acme` | `["us_west"]` | `shopify` |
| `src__acme__us_west__us_east__shopify` | `acme` | `["us_west", "us_east"]` | `shopify` |
| `src__globex__emea__france__paris__zendesk` | `globex` | `["emea", "france", "paris"]` | `zendesk` |

Rocky joins multi-valued regions with the separator when it fills the target schema:

```
staging__us_west__us_east__shopify
staging__emea__france__paris__zendesk
```

### Custom patterns

You choose the component names. Use the ones your naming convention already has:

```toml
[pipeline.bronze.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["environment", "department", "system"]

[pipeline.bronze.target]
adapter = "prod"
catalog_template = "{environment}_analytics"
schema_template = "{department}__{system}"
```

This maps `raw__prod__finance__sap` to `prod_analytics.finance__sap`.

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
name = "category"
type = "String"
nullable = false

[[columns]]
name = "revenue"
type = "Decimal"
nullable = false

[[columns]]
name = "order_count"
type = "Int64"
nullable = false

[rules]
required = ["order_date", "category", "revenue", "order_count"]
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

  error[E013]: protected column 'order_count' has been removed
    = help: restore `order_count` in the SELECT, or remove it from `[rules] protected`
```

### Contract validation in CI

Run the same check in your CI pipeline:

```bash
rocky ci --models models --contracts contracts
```

The PR then fails on a contract violation, before the code reaches production.

## 3. Grants (Pillar 1 of 5)

You declare the permissions you want in `rocky.toml`. Rocky reconciles Databricks Unity Catalog against that declaration on every `rocky apply`.

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

### Reconciliation flow

`rocky apply` runs this loop for each catalog and schema it manages:

```
  1. read desired    [pipeline.<name>.target.governance.grants]
                     [pipeline.<name>.target.governance.schema_grants]
                                   │
                                   ▼
  2. query actual    SHOW GRANTS ON CATALOG / ON SCHEMA
                                   │
                                   ▼
  3. diff            which grants to add, which to revoke
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                             ▼
  4. apply     Databricks:                  no REST permissions API:
               one batched request          GRANT / REVOKE SQL
               to the Unity Catalog
               permissions API
```

The privilege effect is identical on both branches. Only the transport differs, so a Databricks audit log shows PATCH requests rather than `GRANT` statements. Rocky batches one request per securable, grouped by principal.

```sql
-- Equivalent SQL (the form emitted on SQL-only warehouses)
GRANT SELECT ON CATALOG `acme_warehouse` TO `analysts`;
GRANT USE SCHEMA ON SCHEMA `acme_warehouse`.`staging__us_west__shopify` TO `analysts`;
REVOKE MODIFY ON CATALOG `acme_warehouse` FROM `temp_access`;
```

### Managed vs skipped permissions

| Managed (Rocky controls) | Skipped (Rocky ignores) |
|---|---|
| `BROWSE` | `OWNERSHIP` |
| `USE CATALOG` | `ALL PRIVILEGES` |
| `USE SCHEMA` | `CREATE SCHEMA` |
| `SELECT` | |
| `MODIFY` | |
| `MANAGE` | |

Rocky never grants or revokes a skipped permission. It therefore cannot disturb ownership or admin-level grants.

### Principal validation

A principal name must match the pattern `^[a-zA-Z0-9_ \-\.@]+$`. Rocky wraps every principal in backticks in the SQL it generates, so spaces and other special characters survive:

```sql
GRANT USE CATALOG ON CATALOG acme_warehouse TO `data engineers`
```

## 4. Column Classification and Masking (Pillar 2 of 5)

A classification tag marks a column as sensitive. A masking strategy decides how Rocky obscures that column in the warehouse. Rocky keeps the two apart, so you can tag a column for discovery and lineage without deciding its masking policy yet. One project-level block then maps every tag to a strategy, with per-environment overrides:

```
  models/customers.toml            rocky.toml
  ┌─────────────────────┐          ┌────────────────────┐
  │ [classification]    │  tag     │ [mask]             │  strategy
  │ pii_email = "pii" ──┼────────► │ pii = "hash" ──────┼─────────►
  │                     │  name    │ [mask.prod]        │  SQL Rocky
  └─────────────────────┘          │ pii = "none"       │  emits
                                   └────────────────────┘
```

Shipped in engine-v1.16.0. Implemented on Databricks today. Other adapters do nothing.

### Tag columns in the model sidecar

Classification tags live in a `[classification]` block in the model's `.toml` sidecar. Each key is a column name. Each value is a free-form tag string, because Rocky enforces no fixed vocabulary:

```toml
# models/customers.toml
name = "customers"

[classification]
pii_email = "pii"
phone = "pii"
ssn = "confidential"
home_address = "pii"
```

Rocky matches each tag string (`pii`, `confidential`, and so on) against the project-level `[mask]` block to pick a strategy. Coin new tags such as `financial`, `health`, or `internal` whenever you need them. The engine does not need to know about them.

### Map tags to masking strategies

The `[mask]` block in `rocky.toml` binds each classification tag to a masking strategy. A scalar value sets the workspace default. A nested `[mask.<env>]` table overrides the strategy for one environment:

```toml
[mask]
pii = "hash"             # default: SHA-256 hash of the value
confidential = "redact"  # default: replace with '***'

[mask.prod]
pii = "none"             # prod override: do not mask pii
confidential = "partial" # keep first/last 2 chars, mask the middle
```

`RockyConfig::resolve_mask_for_env` resolves the masks for one environment. It takes the top-level scalars as defaults, then overlays the same keys from the matching `[mask.<env>]` table. Pass no environment and only the defaults apply.

### Supported strategies

| Strategy | Emitted SQL behaviour |
|---|---|
| `"hash"` | SHA-256 hash of the column value. |
| `"redact"` | Replace with the literal `'***'`. |
| `"partial"` | Keep the first and last 2 characters; mask the middle. |
| `"none"` | Explicit identity -- no masking applied. Counts as masked for compliance. |

An unknown strategy name, such as `"mask"` or `"obfuscate"`, fails at config-load time. Rocky never accepts a strategy it cannot emit SQL for.

### Allowed unmasked tags

Some tags exist only for discovery and lineage, and are never meant to have a `[mask]` strategy. List those in the `[classifications]` block:

```toml
[classifications]
allow_unmasked = ["internal", "public"]
```

A tag listed there silences the `W004` "tag has no masking strategy" compiler warning. The list is advisory. It does not enforce anything on those columns; it only stops the warning.

### How apply works

Once the DAG completes successfully, `rocky apply` walks each model's `[classification]` block. For each one it calls the governance adapter's `apply_column_tags` and `apply_masking_policy` hooks. Both are best-effort: a failure emits `warn!` and the pipeline continues, exactly as `apply_grants` does.

On Databricks, Rocky writes Unity Catalog column tags and issues `CREATE MASK` / `SET MASKING POLICY`, **one statement per column**. Unity Catalog rejects masking DDL that covers several columns in one statement. BigQuery, Snowflake, and DuckDB do nothing here until their adapters grow the support.

See the [configuration reference](/reference/configuration/) for the full schema of the `[mask]` and `[classifications]` blocks.

## 5. Compliance Rollup (Pillar 3 of 5)

`rocky compliance` answers one question: **is every classified column masked wherever policy says it should be?**

It resolves the classifications against the masks in your configuration and reports the result. It makes no warehouse call and no network round-trip. Shipped in engine-v1.16.0.

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
| `--fail-on exception` | Exit with code `1` when any exception is emitted. Wire this into CI to block merges that leave classified columns unmasked. |
| `--models <dir>` | Models directory to scan (defaults to `models/`). |

### Exit codes

| Exit code | Meaning |
|---|---|
| `0` | Report produced. Exceptions may or may not be present -- exit stays 0 unless `--fail-on exception` is passed. |
| `1` | `--fail-on exception` was set and at least one exception was emitted. |

### How `none` counts

`MaskStrategy::None`, the explicit identity strategy, counts as **masked**. Choosing "do not mask" is a policy decision, not a gap. The gap that produces an exception is a tag with no entry in `[mask]` at all.

The `[classifications] allow_unmasked = [...]` list suppresses exceptions for the tags you deliberately left out of the mask policy. It does not claim those columns are protected.

### JSON output

```bash
rocky compliance --env prod --output json
```

The payload follows the `ComplianceOutput` schema: a `summary` block of counters, a `per_column` array, and an `exceptions` array. Feed it to a dashboard or a CI step summary.

## 6. Role-Graph Reconciliation (Pillar 4 of 5)

You declare roles that inherit from other roles. Rocky flattens that hierarchy into one resolved permission set per role. It rejects cycles and unknown parents at config-load time.

Shipped in engine-v1.16.0. What happens next depends on your Databricks setup. With a SCIM client configured, the adapter creates a `rocky_role_*` SCIM group per role and emits per-catalog `GRANT` statements from the flattened graph. It only adds: it deletes no group and revokes no grant, so removal needs manual cleanup. Without a SCIM client, the adapter runs **log-only**: it validates the flattened graph and emits `debug!` events, and touches nothing in the warehouse.

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

A role may declare an empty `permissions` list. It then acts as a grouping node that exists only so other roles can inherit from it.

### Resolution semantics

At reconcile time Rocky calls `RockyConfig::role_graph()`. That flattens the `[role.*]` map into a deterministic `name → ResolvedRole` map:

1. Walk the `inherits` DAG depth-first, detecting cycles.
2. Union this role's `permissions` with the `permissions` of every ancestor.
3. Reject an unknown parent, such as `inherits = ["nonexistent_role"]`.
4. Reject an unknown permission string.

Rocky catches cycles and unknown parents at config-load time, whether or not the target adapter reconciles role graphs at all. So the resolver still catches your mistake on a warehouse whose adapter does nothing.

### Databricks reconcile

`reconcile_role_graph` on Databricks first validates each flattened role's `rocky_role_<name>` principal syntax. With a SCIM client configured, it then runs two passes:

- **Pass 1** — create one `rocky_role_<name>` SCIM group per role, best-effort per role.
- **Pass 2** — emit `GRANT <permission> ON CATALOG ...` for every `(role, catalog, permission)` triple.

Both passes only add. Rocky revokes no grant and deletes no group. Removing a role or a permission from `rocky.toml` leaves the warehouse untouched until you clean it up by hand. Without a SCIM client, the adapter validates and logs the resolved permission set and emits no `GRANT`. Every other adapter does nothing here.

## 7. Data Retention (Pillar 5 of 5)

A retention policy tells the warehouse how long to keep a table's historical data. You write one sidecar key. Each adapter translates it into the warehouse's own TBLPROPERTIES or session parameter.

Shipped in engine-v1.16.0.

### Declare retention on a model

A model sidecar takes a top-level `retention` key:

```toml
# models/events_daily.toml
name = "events_daily"
retention = "90d"   # grammar: \d+[dy] -- days or years
```

Grammar:

- `<N>d` -- N days
- `<N>y` -- N years; flat-multiplied to 365 days each (no leap-year math)

Rocky rejects a malformed value such as `"abc"`, `"90"`, or `"-3d"` when it parses the sidecar, with `ModelError::InvalidRetention`.

Omit the `retention` key, or set it to null, and Rocky manages no retention for that model. The warehouse keeps its own default behaviour.

### Adapter translation

| Adapter | Translation |
|---|---|
| **Databricks** | Paired Delta TBLPROPERTIES: `delta.logRetentionDuration = '<N> days'` and `delta.deletedFileRetentionDuration = '<N> days'`. Applied via `ALTER TABLE ... SET TBLPROPERTIES`. |
| **Snowflake** | `DATA_RETENTION_TIME_IN_DAYS = <N>` via `ALTER TABLE ... SET`. |
| **BigQuery** | Default-unsupported. No built-in retention setting; sidecar ignored with a `warn!`. |
| **DuckDB** | Default-unsupported. Sidecar ignored with a `warn!`. |

Rocky applies retention after the DAG completes, in the same post-run reconcile loop as classification and masking. A failure emits `warn!` and never aborts the run.

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

Without `--drift`, Rocky does not probe the warehouse. The `WAREHOUSE` column stays `-`, and `IN SYNC` compares the configured value against nothing.

Flags:

| Flag | Purpose |
|---|---|
| `--models <dir>` | Models directory (defaults to `models/`). |
| `--model <name>` | Scope the report to a single model. |
| `--drift` | Probe the warehouse for the applied retention, fill `warehouse_days`, and filter the report to models with a declared policy. |

### `--drift` probes the warehouse

With `--drift`, Rocky resolves a governance adapter per model and reads the retention the warehouse currently applies. It fills `warehouse_days` and recomputes `in_sync`, so you can see where `rocky.toml` and the live table disagree. The probe works on Databricks and Snowflake only. DuckDB and BigQuery inherit the default no-observation implementation, so `--drift` leaves `warehouse_days` empty there. A probe error prints per model on stderr and does not fail the command.

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

### When to use isolation

- **Multi-workspace environments**: teams or environments have separate workspaces
- **Compliance requirements**: the data must not be reachable from an unapproved workspace
- **Development and production separation**: a dev workspace must not touch a production catalog

Isolation is best-effort. If the API call fails, because the workspace ID does not exist for example, Rocky logs a warning and continues the run.

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

Rocky finds its own catalogs by tag. It queries the `managed_by = "rocky"` tag like this:

```sql
SELECT catalog_name
FROM system.information_schema.catalog_tags
WHERE tag_name = 'managed_by' AND tag_value = 'rocky'
```

So you can deploy Rocky across many catalogs and still find every managed catalog by its tag.

### Tagging best practices

- Always set `managed_by = "rocky"`, so Rocky can find its own catalogs
- Use `environment` to tell dev, staging, and prod apart
- Use `data_owner` to record who is responsible
- Use `cost_center` for chargeback
- Add your own tags for compliance, such as `pii = "true"` or `data_classification = "internal"`

## 10. Config Groups and Enforcement

A [config group](/reference/glossary/#config-group) is one definition that a set of models opts into by name, with `group = "<name>"` in each sidecar. It supplies shared routing (`schema_template`) and a shared `strategy`, so those models route and materialize the same way without repeating the config. [The model format guide](/reference/model-format/#config-groups) is the full reference. This section covers the governance angle.

### Enforced config groups

A group is an overridable default. A member model can pin its own `target.schema` or `strategy`, and the local value wins. Set `enforce = true` and the group becomes an [enforced group](/reference/glossary/#enforced-group): its fields are binding. A member that pins a field the group controls then fails to load. It does not quietly route or materialize itself differently from the rest of the group:

```toml
# models/groups/regulated.toml
enforce = true
schema_template = "mart_{region}"

[strategy]
type = "merge"
unique_key = ["id"]
```

Enforcement covers exactly the two fields the group owns: the target `schema`, when the group sets `schema_template`, and the `strategy`. A member that sets either one fails the load with a `GroupOverride` error. The model may still supply its own `[args]` to fill the template. It may still set any field the group does not own, such as `target.catalog`. It only loses the schema routing and the materialization the group governs.

This is a load-time guarantee, in the same family as a data contract. The check runs when the model graph loads. Rocky rejects an off-policy override before any SQL reaches the warehouse, instead of surfacing it as drift later. Enforcement is opt-in. Without `enforce`, a group stays an overridable default.

Enforcement covers every model in the group, whether you wrote it in SQL or in the `.rocky` DSL. The group governs routing and materialization, never the model body.

## 11. Model Tags

A model tag is a free-form attribute that describes a model as a whole: `domain`, `tier`, `owner`, or whatever your governance model needs. Model tags are not the [tagging strategy](#9-tagging-strategy) of the previous section. Those tags live under `[pipeline.*.target.governance.tags]`, land on Unity Catalog catalogs, schemas, and tables through `ALTER ... SET TAGS`, and drive catalog discovery. Model tags live in the model sidecar or its config group, and flow into Rocky's model graph, the orchestrator's asset tags, and the `rocky compile` JSON.

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

A config group can declare its own `[tags]` block. Every member model inherits those tags, so one attribute set on the group reaches every model in it:

```toml
# models/groups/finance.toml
schema_template = "mart_{region}"

[tags]
domain = "finance"
tier = "gold"
```

### Sidecar over group, per key

For a model in a group, Rocky merges the model's own `[tags]` on top of the group's `[tags]`, key by key. A member can override one inherited key and keep the rest: a model in the `finance` group above can set `tier = "silver"` in its sidecar and still inherit `domain = "finance"`. Precedence matches the rest of group resolution, sidecar over group.

### Projection to Dagster

`rocky compile --output json` emits the resolved tags as `models_detail[].tags`. The `dagster-rocky` integration projects them onto each derived asset as Dagster tags, so you can select assets by them, for example `tag:domain=finance`. The translator adds its own `rocky/`-namespaced tags for the model name, the target catalog, the target schema, and the strategy. The `rocky/` prefix keeps those from ever colliding with one of your keys. A tag you set once in a sidecar or a group is therefore visible end to end: in the typed model graph, in `rocky compile`, and in the orchestrator.

### Per-model warehouse tags: `[governance.tags]`

Model `[tags]` face the orchestrator and never reach the warehouse. To write a tag onto a model's **own** target table or view in Unity Catalog, declare a `[governance.tags]` block in the model sidecar:

```toml
# models/fct_orders.toml
name = "fct_orders"

[governance.tags]
domain = "finance"
tier = "gold"
```

Once the model materializes, `rocky apply` emits tag DDL against its target securable. It uses `ALTER VIEW ... SET TAGS (...)` for a view-format model, and `ALTER TABLE ... SET TAGS (...)` otherwise. It writes your keys and values verbatim, with no prefix. This is the per-model counterpart to the catalog- and schema-level [tagging strategy](#9-tagging-strategy) above (`[pipeline.*.target.governance.tags]`).

The three tag surfaces are independent, and each reaches a different consumer. Keep them apart:

| Block | Where it lives | What it does |
|---|---|---|
| `[tags]` | Model sidecar / config group | Dagster asset tags + `rocky compile` JSON. Never written to the warehouse. |
| `[governance.tags]` | Model sidecar | `ALTER VIEW/TABLE ... SET TAGS` on the model's own securable, post-materialize. |
| `[pipeline.*.target.governance.tags]` | Pipeline target | `ALTER CATALOG/SCHEMA/TABLE ... SET TAGS` during replication, used for catalog discovery. |

`[governance.tags]` is best-effort, like classification and retention: a failure warns and never aborts the run. Rocky skips an empty block, because Unity Catalog rejects `SET TAGS ()`.

## 12. Quality Checks

Rocky runs data quality checks inside a replication run. Each check executes as soon as its table is copied, and the run output carries the results.

### Configuration

```toml
[pipeline.bronze.checks]
enabled = true
row_count = true
column_match = true
freshness = { threshold_seconds = 86400 }
anomaly_threshold_pct = 50.0
```

### Check types

#### Row count

Compares `COUNT(*)` on the source against `COUNT(*)` on the target. Rocky batches these into `UNION ALL` queries, 200 tables per batch:

```json
{
  "name": "row_count",
  "passed": true,
  "source_count": 15000,
  "target_count": 15000
}
```

#### Column match

Compares the source and target column sets, ignoring case, and reports a missing or extra column. It reuses the columns drift detection already cached, so it costs no extra query:

```json
{
  "name": "column_match",
  "passed": false,
  "missing": ["new_column"],
  "extra": []
}
```

#### Freshness

Compares `MAX(timestamp_column)` against the current time, which gives the age of the newest row:

```toml
freshness = { threshold_seconds = 86400 }  # 24 hours
```

Rocky flags a table that has received no new data within the threshold:

```json
{
  "name": "freshness",
  "passed": false,
  "lag_seconds": 172800,
  "threshold_seconds": 86400
}
```

#### Null rate

Samples the table with `TABLESAMPLE` and computes the percentage of nulls per column:

```toml
[pipeline.bronze.checks]
null_rate = { columns = ["email", "phone"], threshold = 0.05, sample_percent = 10 }
```

`sample_percent` keeps the query fast on a large table.

#### Anomaly detection

Compares the current row count against a moving average of past runs. Rocky flags the table when the difference exceeds the threshold:

```toml
anomaly_threshold_pct = 50.0  # Flag if count changes by more than 50%
```

This catches three failures:

- A source table was truncated, so the count drops to near zero
- A bad sync duplicated data, so the count spikes
- A connector stopped, so the count stays flat

#### Custom checks

Your own SQL query, with a `{target}` placeholder:

```toml
[[pipeline.bronze.checks.custom]]
name = "no_future_dates"
sql = "SELECT COUNT(*) FROM {target} WHERE order_date > CURRENT_DATE()"
threshold = 0

[[pipeline.bronze.checks.custom]]
name = "revenue_positive"
sql = "SELECT COUNT(*) FROM {target} WHERE revenue < 0"
threshold = 0
```

The check passes when the query result is less than or equal to the threshold.

## 13. Audit Trail

Rocky keeps run history and quality metrics in its embedded [state store](/reference/glossary/#state-store) (a redb database on disk). You query that store for the audit trail. Every `rocky apply` stamps eight extra governance fields onto its `RunRecord`, shipped in engine-v1.16.0. `rocky history --audit` shows them.

### `rocky history --audit` and the 8 audit fields

Plain `rocky history` stays compact, so its bytes remain stable for schema v5 consumers. Pass `--audit` to expand every governance field, in text or in JSON:

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

### Schema version v5 → v6 (forward-deserialize)

The audit trail took the redb schema from v5 to v6. Rocky migrates by deserializing forward, and never rewrites a stored blob, so an existing store opens cleanly. It fills three defaults on a v5 row:

- `hostname = "unknown"`
- `rocky_version = "<pre-audit>"`
- `session_source = Cli`

An old run therefore still renders under `rocky history --audit`. It shows those placeholder strings for the three fields that did not exist when it ran.

### View run history

```bash
rocky history
```

```
RUN ID       STARTED                  STATUS     MODELS   TRIGGER
────────────────────────────────────────────────────────────────────
abc12345678  2026-03-30 10:00:00      Completed  42       Scheduled
def98765432  2026-03-29 10:00:00      Completed  42       Scheduled
ghi11111111  2026-03-28 14:30:00      Failed     38       Manual

Total runs: 3
```

### Filter by date

```bash
rocky history --since 2026-03-29
```

### View model execution history

```bash
rocky history --model fct_daily_revenue
```

```
STARTED                  DURATION   ROWS         STATUS         SQL HASH
────────────────────────────────────────────────────────────────────────────
2026-03-30 10:00:00      2300ms     15432        succeeded      a1b2c3d4
2026-03-29 10:00:00      2100ms     15200        succeeded      a1b2c3d4
2026-03-28 14:30:00      0ms        -            failed         a1b2c3d4

Total executions: 3
```

### View quality metrics

```bash
rocky metrics fct_daily_revenue
```

```
Latest snapshot (run: abc12345678):
  Row count: 15432
  Freshness lag: 300s
  Null rates:
    email: 2.10%
    phone: 15.30%
```

### View quality trends

```bash
rocky metrics fct_daily_revenue --trend
```

```
TIMESTAMP                ROW COUNT    RUN ID     FRESHNESS
──────────────────────────────────────────────────────────────
2026-03-30 10:00:00      15432        abc123456  300s
2026-03-29 10:00:00      15200        def987654  280s
2026-03-28 10:00:00      14980        ghi111111  310s
```

### View column-specific metrics

```bash
rocky metrics fct_daily_revenue --column email --alerts
```

### Quality alerts

Pass `--alerts` to see quality issues:

```bash
rocky metrics fct_daily_revenue --alerts
```

```
Latest snapshot (run: abc12345678):
  Row count: 15432

ALERTS:
  [WARNING] null rate 25.0% exceeds 20% threshold (column: phone)
```

Alert severity levels:
- **critical**: the null rate exceeds 50%
- **warning**: the null rate exceeds 20%, or the freshness lag exceeds 24 hours

### JSON output

Every history and metrics command also emits JSON:

```bash
rocky history -o json
rocky metrics fct_daily_revenue --trend -o json
```

## 14. Complete Governance Configuration

This is one pipeline target with every governance feature turned on. Governance lives under each pipeline's target, so two pipelines can carry different policies:

```toml
[pipeline.bronze.target.governance]
auto_create_catalogs = true
auto_create_schemas = true

# Tags applied to all managed catalogs, schemas, and tables
[pipeline.bronze.target.governance.tags]
managed_by = "rocky"
environment = "production"
data_owner = "analytics-team"

# Catalog-level grants
[[pipeline.bronze.target.governance.grants]]
principal = "data_engineers"
permissions = ["USE CATALOG", "MANAGE"]

[[pipeline.bronze.target.governance.grants]]
principal = "analysts"
permissions = ["BROWSE", "USE CATALOG"]

[[pipeline.bronze.target.governance.grants]]
principal = "ml_team"
permissions = ["BROWSE", "USE CATALOG", "SELECT"]

# Schema-level grants
[[pipeline.bronze.target.governance.schema_grants]]
principal = "data_engineers"
permissions = ["USE SCHEMA", "SELECT", "MODIFY"]

[[pipeline.bronze.target.governance.schema_grants]]
principal = "analysts"
permissions = ["USE SCHEMA", "SELECT"]

# Workspace isolation
[pipeline.bronze.target.governance.isolation]
enabled = true

[[pipeline.bronze.target.governance.isolation.workspace_ids]]
id = 123456789
binding_type = "READ_WRITE"

[[pipeline.bronze.target.governance.isolation.workspace_ids]]
id = 987654321
binding_type = "READ_ONLY"
```

Combined with quality checks (also under the pipeline):

```toml
[pipeline.bronze.checks]
enabled = true
row_count = true
column_match = true
freshness = { threshold_seconds = 86400 }
anomaly_threshold_pct = 50.0
null_rate = { columns = ["email"], threshold = 0.05, sample_percent = 10 }

[[pipeline.bronze.checks.custom]]
name = "no_future_dates"
sql = "SELECT COUNT(*) FROM {target} WHERE order_date > CURRENT_DATE()"
threshold = 0
```

Classification, masking, roles, and retention sit outside the pipeline target, because they are project-level. Together they complete the picture:

```toml
# Project-level masking policy
[mask]
pii = "hash"
confidential = "redact"

[mask.prod]
pii = "none"
confidential = "partial"

[classifications]
allow_unmasked = ["internal"]

# Project-level role graph
[role.reader]
permissions = ["SELECT", "USE CATALOG", "USE SCHEMA"]

[role.analytics_engineer]
inherits = ["reader"]
permissions = ["MODIFY"]

[role.admin]
inherits = ["analytics_engineer"]
permissions = ["MANAGE"]
```

Paired with a model sidecar:

```toml
# models/customers.toml
name = "customers"
retention = "365d"

[classification]
pii_email = "pii"
phone = "pii"
ssn = "confidential"
```

That one configuration exercises every feature in this guide: schema routing, declarative grants with reconciliation, workspace isolation, per-environment classification and masking, role-graph validation at config load, retention, inline quality checks, and the audit trail.

## 15. CI Gate Example

Wire `rocky compliance --fail-on exception` into a CI step and it blocks a merge that leaves a classified column unmasked. For a quieter local run, drop `--fail-on` and add `--exceptions-only`. The output then skips the per-column table when nothing is wrong.

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

The gate exits `0` when every classified column has a resolved strategy, or appears in `allow_unmasked`. It exits `1`, and fails the job, as soon as one exception appears.

### Local quiet-mode run

```bash
rocky compliance --env prod --exceptions-only
```

When everything is compliant, this prints the summary counters alone. When exceptions exist, it filters the `per_column` table to the offending rows.

### Machine-readable gate

For a dashboard or your own policy engine, emit JSON and pipe it into `jq`:

```bash
rocky compliance --env prod --output json \
  | jq '.exceptions[] | {model, column, env, reason}'
```

The `ComplianceOutput` schema stays stable across minor versions. Point your tooling at the JSON payload, not at the text table.
