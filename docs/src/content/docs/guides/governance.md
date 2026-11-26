---
title: Data Governance
description: Configure schema patterns, data contracts, permissions, masking, retention, tagging, quality checks, data residency and audit trails.
sidebar:
  order: 17
---

Governance in Rocky is declarative. Write the rules in `rocky.toml` or a model sidecar, and `rocky apply` carries them out. Two features run as standalone commands you can gate a PR on: `rocky compliance` and `rocky retention-status`. Every key is in the [configuration reference](/reference/configuration/).

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
| [Data residency](#data-residency) | `[residency]` and `region` | Refuses a plan or AI request that moves data between regions. |

```
  config load        compile           rocky apply
  ───────────        ───────           ───────────────────────────────
  role graph         data contracts    during the run:
  validated          checked             catalogs + schemas created
  (cycles, unknown                       grants applied
   parents)                              tags applied
  mask strategy                          quality checks, per copied
  names checked                          table (replication)
  residency labels                     after the DAG completes:
  checked                                classification + masking
                                         retention
                                         role graph reconciled
                                       every run:
                                         audit record in the state store

  Any time:  rocky compliance     rocky retention-status
```

## 1. Schema Patterns

A schema pattern maps a source schema name onto a target catalog and schema, so one pipeline can route many tenants. The pattern lives on the pipeline source, the templates on the target, and both use the same component names. [Schema patterns](/concepts/schema-patterns/) has the parsing rules and error cases.

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

Source schema `src__acme__us_west__shopify` lands in `acme_warehouse.staging__us_west__shopify`. The `...` suffix captures one or more segments, which Rocky joins with the separator.

## 2. Data Contracts

A data contract is a [compile-time contract](/reference/glossary/#compile-time-contract): a schema agreement Rocky checks before any row is written. Create `contracts/<model>.contract.toml`:

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

| Rule | Description |
|---|---|
| **required** | Column must exist in the model's output with the specified type. Compilation fails if missing or wrong type. |
| **protected** | Column cannot be removed. If a protected column disappears, compilation fails with `E013`. |
| **nullable** | When `false`, the compiler verifies the column is non-nullable in the type system. |

Check with `rocky compile --models models --contracts contracts`, or in CI with `rocky ci --models models --contracts contracts`. A violation is a compiler error, for example `error[E011]: column 'revenue' type mismatch: contract expects Decimal, got String` or `error[E013]: protected column 'revenue' has been removed`.

## 3. Grants (Pillar 1 of 5)

Declare the permissions you want under `[pipeline.<name>.target.governance]`. On every `rocky apply`, Rocky issues one `GRANT` per declared permission on every catalog (`grants`) and schema (`schema_grants`) the pipeline manages. It does not read the current grants and never revokes one.

```toml
[pipeline.bronze.target.governance]
auto_create_catalogs = true     # catalog grants need this
auto_create_schemas = true      # schema grants need this

[[pipeline.bronze.target.governance.grants]]
principal = "data_engineers"
permissions = ["USE CATALOG", "MANAGE"]

[[pipeline.bronze.target.governance.schema_grants]]
principal = "analysts"
permissions = ["USE SCHEMA", "SELECT"]
```

:::caution[Rocky never revokes]
Removing a grant from `rocky.toml` leaves it on the warehouse. A person must revoke it by hand. `grants_revoked` in the run output is always `0`.
:::

Rocky manages `BROWSE`, `USE CATALOG`, `USE SCHEMA`, `SELECT`, `MODIFY` and `MANAGE`, and never grants `OWNERSHIP`, `ALL PRIVILEGES` or `CREATE SCHEMA`, so it cannot disturb ownership or admin-level grants. A principal name must match `^[a-zA-Z0-9_ \-\.@]+$`. Rocky wraps it in backticks in the SQL. Accepted permissions per block, the generated SQL, and the run output: [Permissions](/reference/permissions/).

## 4. Column Classification and Masking (Pillar 2 of 5)

A classification tag marks a column as sensitive. A masking strategy decides how Rocky obscures it. The two are separate, so you can tag a column before you pick a policy. Implemented on Databricks today. Other adapters do nothing.

```
  models/customers.toml            rocky.toml
  ┌─────────────────────┐          ┌────────────────────┐
  │ [classification]    │  tag     │ [mask]             │  strategy
  │ pii_email = "pii" ──┼────────► │ pii = "hash" ──────┼─────────►
  │                     │  name    │ [mask.prod]        │  SQL Rocky
  └─────────────────────┘          │ pii = "none"       │  emits
                                   └────────────────────┘
```

### Tag columns in the model sidecar

A `[classification]` block maps each column name to a free-form tag string. Rocky enforces no fixed vocabulary:

```toml
# models/customers.toml
name = "customers"

[classification]
pii_email = "pii"
ssn = "confidential"
```

### Map tags to masking strategies

The `[mask]` block in `rocky.toml` binds each tag to a strategy: `hash`, `redact`, `partial` or `none`. A `[mask.<env>]` table overrides it for one environment (`rocky plan --env <env>`). Strategy definitions and precedence: [`[mask]`](/reference/configuration/#mask).

```toml
[mask]
pii = "hash"
confidential = "redact"

[mask.prod]
pii = "none"             # prod override: do not mask pii
```

`rocky preview rows` applies `hash` and `partial` on Databricks, Snowflake and DuckDB only. On other adapters it refuses with `unmaskable_column` and returns no rows. `redact` works everywhere. See [`rocky preview rows`](/reference/commands/modeling/#rocky-preview-rows).

### Allowed unmasked tags

List tags that exist only for discovery and lineage in `[classifications] allow_unmasked = ["internal", "public"]`. It silences the `W004` "tag has no masking strategy" warning and enforces nothing. See [`[classifications]`](/reference/configuration/#classifications).

### How apply works

After the DAG completes, `rocky apply` walks each model's `[classification]` block and calls the governance adapter's `apply_column_tags` and `apply_masking_policy` hooks. Both are best-effort: a failure emits `warn!` and the pipeline continues. On Databricks, Rocky writes Unity Catalog column tags and issues `CREATE MASK` / `SET MASKING POLICY`, one statement per column, because Unity Catalog rejects multi-column masking DDL.

## 5. Compliance Rollup (Pillar 3 of 5)

`rocky compliance` answers one question: **is every classified column masked wherever policy says it should be?** It resolves classifications against the masks in your configuration and makes no warehouse call.

```
$ rocky compliance
Compliance report (env: <all>)
  models scanned:       42
  classified columns:   87
  with strategy:        84
  exceptions:           3

EXCEPTIONS:
  customers.pii_email    (prod)  no strategy for classification 'pii'
  users.ssn              (dev)   no strategy for classification 'confidential'
```

| Flag | Purpose |
|---|---|
| `--env <name>` | Scope the report to one environment. Without it, Rocky expands across the defaults plus every `[mask.<env>]` override. |
| `--exceptions-only` | Filter the `per_column` table to rows with at least one exception. The `exceptions` list is always shown. |
| `--fail-on exception` | Exit `1` when any exception is emitted. Without it the exit code stays `0`. Wire it into CI to block merges that leave classified columns unmasked. |
| `--models <dir>` | Models directory to scan (defaults to `models/`). |

`none` counts as **masked**: "do not mask" is a policy decision, not a gap. An exception means a tag has no entry in `[mask]` at all. `allow_unmasked` suppresses exceptions for tags you left out on purpose without claiming those columns are protected. `--output json` follows the `ComplianceOutput` schema (`summary`, `per_column`, `exceptions`).

## 6. Role-Graph Reconciliation (Pillar 4 of 5)

Declare roles that inherit from other roles. Rocky flattens the hierarchy into one permission set per role and rejects cycles, unknown parents and unknown permission strings at config load, on every adapter. Keys and an example: [`[role.<name>]`](/reference/configuration/#rolename).

On Databricks, `reconcile_role_graph` validates each role's `rocky_role_<name>` principal syntax. With a SCIM client configured, it runs two passes:

- **Pass 1:** create one `rocky_role_<name>` SCIM group per role, best-effort per role.
- **Pass 2:** emit `GRANT <permission> ON CATALOG ...` for every `(role, catalog, permission)` triple.

Both passes only add. Rocky revokes no grant and deletes no group, so removing a role or permission from `rocky.toml` needs manual cleanup. Without a SCIM client the adapter runs **log-only**: it validates the graph and touches nothing in the warehouse. Every other adapter does nothing.

## 7. Data Retention (Pillar 5 of 5)

A retention policy sets how long the warehouse keeps a table's historical data. Write one sidecar key, `retention = "90d"` (grammar `\d+[dy]`: days or years, flat 365 days per year). Rocky rejects a malformed value with `ModelError::InvalidRetention`, and manages no retention for a model without the key. See [Retention](/reference/model-format/#retention).

| Adapter | Translation |
|---|---|
| **Databricks** | Paired Delta TBLPROPERTIES `delta.logRetentionDuration` and `delta.deletedFileRetentionDuration`, via `ALTER TABLE ... SET TBLPROPERTIES`. |
| **Snowflake** | `DATA_RETENTION_TIME_IN_DAYS = <N>` via `ALTER TABLE ... SET`. |
| **BigQuery**, **DuckDB** | Unsupported. The sidecar is ignored with a `warn!`. |

Rocky applies retention after the DAG completes, in the same best-effort loop as masking.

### Inspecting retention: `rocky retention-status`

`rocky retention-status` prints each model's configured retention beside its `WAREHOUSE` value and an `IN SYNC` flag. Flags: `--models <dir>` (defaults to `models/`), `--model <name>`, and `--drift`, which probes the warehouse for the applied retention, fills `warehouse_days`, recomputes `in_sync` and filters to models with a declared policy. Without `--drift`, `WAREHOUSE` stays `-`. The probe works on Databricks and Snowflake only. A probe error prints per model on stderr and does not fail the command.

## 8. Workspace Isolation

Rocky can restrict a catalog to named Databricks workspaces through the Unity Catalog workspace bindings API, at `READ_WRITE` or `READ_ONLY`. Set `[pipeline.<name>.target.governance.isolation]` with `enabled = true` and a `workspace_ids` list (`id`, `binding_type`). Rocky sets each managed catalog to `ISOLATED` and binds it to the listed workspaces, so no other workspace can reach it. It is best-effort and Databricks only. Keys and the reconcile rules (Rocky also removes bindings that `rocky.toml` does not declare): [`isolation`](/reference/configuration/#pipelinenametargetgovernanceisolation) and [Permissions](/reference/permissions/#workspace-isolation).

## 9. Tagging Strategy

A tag is a key-value pair. Rocky writes `[pipeline.<name>.target.governance.tags]` onto catalogs, schemas and tables with Databricks `ALTER ... SET TAGS`. Catalogs and schemas also get the parsed schema components; tables get the governance tags only. The statements per level: [Permissions](/reference/permissions/#tagging).

Rocky finds its own catalogs by the tag `managed_by = "rocky"`, read from `system.information_schema.catalog_tags`. Always set it.

## 10. Config Groups and Enforcement

A [config group](/reference/glossary/#config-group) is one definition that models opt into with `group = "<name>"`. It supplies shared routing (`schema_template`) and a shared `strategy`. A group is an overridable default until you set `enforce = true`: then a member that pins the target `schema` or the `strategy` fails to load with `GroupOverride`, before any SQL reaches the warehouse. A member may still supply its own `[args]` and set other fields, such as `target.catalog`. The group governs routing and materialization, never the model body. File layout, precedence and examples: [Config groups](/reference/model-format/#config-groups).

## 11. Model Tags

A model tag is a free-form attribute for a whole model (`domain`, `tier`, `owner`). Three tag surfaces are independent:

| Block | Where it lives | What it does |
|---|---|---|
| `[tags]` | Model sidecar / config group | Dagster asset tags and `rocky compile` JSON (`models_detail[].tags`). Never written to the warehouse. |
| `[governance.tags]` | Model sidecar | `ALTER VIEW/TABLE ... SET TAGS` on the model's own securable, post-materialize. Verbatim keys, no prefix. Best-effort. An empty block is skipped (Unity Catalog rejects `SET TAGS ()`). |
| `[pipeline.*.target.governance.tags]` | Pipeline target | `ALTER CATALOG/SCHEMA/TABLE ... SET TAGS` during replication, used for catalog discovery. |

The model's own `[tags]` merge over the group's, key by key. `dagster-rocky` projects them onto each asset, next to its own `rocky/`-namespaced tags for the model name, target catalog, target schema and strategy. See [`[tags]`](/reference/model-format/#tags) and [`[governance.tags]`](/reference/model-format/#governancetags).

## 12. Quality Checks

Rocky runs quality checks inside a replication run, as soon as each table is copied. Keys are in [`[pipeline.NAME.checks]`](/reference/configuration/#pipelinenamechecks). [Data quality checks](/concepts/data-quality-checks/) has the result shapes, anomaly rules and freshness edge cases.

Example: `[pipeline.bronze.checks]` with `enabled = true`, `row_count = true`, `column_match = true`, `freshness = { error_after = "24h" }` and `null_rate = { columns = ["email", "phone"], threshold = 0.05, sample_percent = 10 }`, plus a `[[pipeline.bronze.checks.custom]]` entry with `name`, `sql` (using the `{target}` placeholder) and `threshold`.

## 13. Audit Trail

Rocky keeps run history and quality metrics in its embedded [state store](/reference/glossary/#state-store). Every `rocky apply` stamps eight governance fields onto its `RunRecord`. Plain `rocky history` stays compact. `rocky history --audit` (text or `--output json`) expands them:

| Field | Source |
|---|---|
| `triggering_identity` | Auth principal that kicked off the run. |
| `session_source` | Auto-detected: `Cli` / `Dagster` / `Lsp` / `HttpApi`. |
| `git_commit`, `git_branch` | Resolved at run start from the current repo. |
| `idempotency_key` | Echoed from `rocky plan --idempotency-key <KEY>` (or the `rocky run --idempotency-key` alias). |
| `target_catalog` | The catalog(s) the run wrote to. |
| `hostname` | The host that executed the run. |
| `rocky_version` | The CLI version that produced the record. |

A run recorded before the audit fields shows `hostname = "unknown"`, `rocky_version = "<pre-audit>"` and `session_source = Cli`. Rocky never rewrites a stored row.

Read history and metrics with `rocky history` (`--since <date>`, `--model <name>`) and `rocky metrics <model>` (`--trend`, `--column <col> --alerts`, `--alerts`). `--alerts` prints lines such as `[WARNING] null rate 25.0% exceeds 20% threshold (column: phone)`. **critical**: the null rate exceeds 50%. **warning**: the null rate exceeds 20%, or the freshness lag exceeds 24 hours. Every history and metrics command takes `-o json`.

## Data residency

Say which region each pipeline and the AI endpoint is in, and stop a plan or an AI request that moves data between regions. Keys: [`[residency]`](/reference/configuration/#residency). A region is a label of lowercase letters, digits and hyphens. Rocky compares labels as exact text, so `"eu"` and `"eu-west-1"` are different regions, and it never corrects a label.

```
  [pipeline.bronze.source] region = "eu"  ──▶  [pipeline.bronze.target] region = "us"
                                          V061 unless allow has { from = "eu", to = "us" }

  data in region "eu"  ──▶  [ai] region = "us"
                            V062 unless allow has { from = "eu", to = "us" }
```

**Pipelines (`V061`).** A replication or snapshot pipeline reads its `source` and writes its `target`. When both set `region` and the labels differ, Rocky refuses the pipeline, whether or not `enforce` is on. `rocky validate` reports it offline, and every command that loads the config (`plan`, `run`, `apply`, `discover`) refuses the project. The reverse direction needs its own `allow` entry. Rocky checks only a pipeline's own source and target. It does not check the SQL inputs of a transformation's models, the `tables` of a quality pipeline, the `source_dir` of a load pipeline, a `source.discovery` block, or a pipeline-level `depends_on` (it orders pipelines but does not say one reads the other's tables). A transformation in `"us"` that reads a table another pipeline wrote in `"eu"` is not refused.

**Required labels (`V060`).** With `enforce = true`, `region` is required on `source` and `target` of replication and snapshot pipelines, on `target` of transformation, quality and load pipelines, and on `[ai]` (even when the project never runs `rocky ai`). An empty string counts as missing.

**AI requests (`V062`).** Rocky compares `[ai] region` with the region of the data an AI request carries. Same region: the request goes. Different regions: refused unless `allow` has an entry from the data's region to the `[ai]` region, whether or not `enforce` is on. No label on the data or on `[ai]`, with `enforce` off: the request goes. Rocky checks when it builds the AI client, before it profiles the warehouse or sends anything, and `rocky ai-sync` checks before it writes its upstream schema baseline. One command builds one client for all its data, so one refused item stops the whole command: `rocky ai-explain --all` over an `"eu"` and a `"us"` model with `[ai] region = "us"` sends nothing. Name one model (`rocky ai-explain <model>`, `rocky ai-test <model>`, `rocky ai-sync --model <model>`) to run the models that may go.

Data means the schema (column names and types), statistics or values of a table. The SQL, `.rocky` source and `rocky.toml` text you wrote are not data.

| Feature | What the request carries | Region of that data |
|---------|--------------------------|---------------------|
| `rocky ai-contract`, MCP `ai_contract` | Statistics of the model's table (row, null and distinct counts; with `--with-data`, also min, max and sample values) | The `target` of the project's pipeline, and its `source` for a replication or snapshot pipeline |
| `rocky ai-explain`, `rocky ai-sync`, MCP `explain_model`, the editor's contract fix | The model's schema and the schemas of its direct upstreams | The model's region |
| `rocky ai-test`, MCP `ai_test` | The model's schema | The model's region |
| `rocky ai "<intent>"` | The schema of every compiled model and source table | The model's region |
| MCP `suggest_freshness_block`, the editor's freshness and classification fixes | The model's column names | The model's region |
| The editor's `.rocky` syntax fix | The `.rocky` source and the parse error only | No data, so no check |

**Region of a model.** The `target` region of each transformation pipeline whose `models` glob loads it. A model that no transformation pipeline loads is in the `target` region of every pipeline.

**Region of a table that is not a model** (for example, one from the schema cache) comes from the pipelines that write or read it. Rocky matches `<schema>.<table>` (optional leading catalog) without regard to case and ignores the catalog.

| Pipeline | Tables it writes (target side) | Tables it reads (source side) |
|---|---|---|
| `replication` | Every table in a schema that `target.schema_template` makes (a `{placeholder}` stands for text without a dot). | Every table in a schema that `source.schema_pattern` accepts: `prefix`, then at least one part per component. |
| `snapshot` | The `target.schema` / `target.table` table. | The `source.schema` / `source.table` table. |
| `load` | The `target.schema` / `target.table` table; with no `table`, every table in `target.schema`. | None: it reads files. |

A written table takes the pipeline's `target` region, plus its `source` region for replication and snapshot. A read table takes only the `source` region. A table that two pipelines match takes both regions. A `source` or `target` block with no `region` adds "no region": without `enforce` the labels that exist decide, and with `enforce = true` it refuses the request with `V062`. A `schema_pattern` Rocky cannot parse matches every table. A wrong dot-split can only add regions.

A table that no pipeline writes or reads has no region. Without `enforce` a request that carries it goes. With `enforce = true` Rocky refuses it with `V062`: `rocky ai "<intent>"` when the source schema cache holds one, and `rocky ai-explain` or `rocky ai-sync` for a model whose `depends_on` lists one. A table the SQL reads but `depends_on` omits is not an upstream, and its schema is not sent.

`rocky validate` also warns with `V062` for each pipeline `source` or `target` label that differs from `[ai] region` with no `allow` entry. It is a hint: `rocky run` never makes an AI request. `ROCKY_AI_BASE_URL` changes where requests go, not `[ai] region`, so keep the label in step with the endpoint.

## 14. Where Each Block Lives

Pipeline blocks sit under each pipeline, so two pipelines can carry different policies. Project blocks sit at the top of `rocky.toml`. Model blocks sit in the sidecar.

| Level | Block | Section |
|---|---|---|
| Pipeline target | `[pipeline.<name>.target.governance]`: `auto_create_*`, `tags`, `grants`, `schema_grants`, `isolation` | 3, 8, 9 |
| Pipeline | `[pipeline.<name>.checks]` | 12 |
| Project | `[mask]`, `[mask.<env>]`, `[classifications]`, `[role.<name>]`, `[residency]`, `[policy]` | 4, 6, residency |
| Model sidecar | `[classification]`, `retention`, `[tags]`, `[governance.tags]`, `group` | 4, 7, 10, 11 |
| Contracts | `contracts/<model>.contract.toml` | 2 |

## 15. CI Gate Example

Wire `rocky compliance --fail-on exception` into CI to block a merge that leaves a classified column unmasked. It exits `0` when every classified column has a strategy or appears in `allow_unmasked`, and `1` on the first exception.

```yaml
# .github/workflows/rocky-compliance.yml (on pull_request, paths: models/**, rocky.toml)
- name: Install Rocky
  run: |
    curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
    echo "$HOME/.local/bin" >> $GITHUB_PATH
- name: Run compliance gate
  run: rocky compliance --env prod --fail-on exception
```

For policy rules that gate agents, see [Testing Policies](/guides/testing-policies/).
