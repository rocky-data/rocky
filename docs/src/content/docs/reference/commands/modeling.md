---
title: Modeling Commands
description: Compile, test, lint and diff Rocky models, trace their lineage, and preview a change before it merges
sidebar:
  order: 2
---

These commands compile and test models, trace lineage, render SQL, compare a change against a base git ref, and publish and check schemas across teams.

---

## `rocky compile`

Compile models: resolve dependencies, type-check SQL, validate data contracts, and build the semantic graph.

```bash
rocky compile [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--models <PATH>` | `PathBuf` | every pipeline | Compile only the `.sql`, `.rocky` and `.toml` model files in this directory. Without it, Rocky compiles the models of every transformation pipeline together. See [The whole project by default](#the-whole-project-by-default). |
| `--contracts <PATH>` | `PathBuf` | | Directory containing data contract definitions. Default: the project `contracts/` directory beside the models directory, which is `models/` beside the config file when you pass no `--models`. |
| `--model <NAME>` | `string` | | Restrict the reported result and exit status to one exact model name. It reports whether that model's own source is valid, not whether its upstreams can be rebuilt. See [Selecting one model](#selecting-one-model). |
| `--select <SELECTOR>...`, `-s` / `--exclude <SELECTOR>...` / `--state-ref <REF>` / `--state-working-tree` | `string` | | Report and fail on the [selected models](/reference/node-selection/) only. Cannot be combined with `--model`. |
| `--var <NAME=VALUE>` | `string` (repeatable) | | Bind a run variable, so `rocky compile` checks the same SQL `rocky run --var` executes. A required `@var(name)` with no value and no inline default is a compile error. See [`@var()` run variables](/reference/model-format/#var-run-variables). |
| `--expand-macros` | `bool` | `false` | Expand macros from `macros/` and include the expanded SQL in the output. |
| `--target-dialect <DIALECT>` | `dbx` \| `sf` \| `bq` \| `duckdb` | | Run the **P001 dialect-portability lint** against the chosen target. Non-portable constructs emit `error`-severity diagnostics. Precedence: flag > `[portability] target_dialect` in `rocky.toml` > unset. See [Portability linting](/concepts/linters/). The flag also selects the warehouse for the `E042`/`E043` operand checks, ahead of the adapter type. See [Aggregate and comparison operands](/concepts/compiler/#aggregate-and-comparison-operands). |
| `--deny-warnings <CODES>` | `string` (comma-separated, repeatable) | | Report the listed warning codes as errors and exit non-zero, such as `--deny-warnings W042,W043`. Other warnings stay warnings. A code that is not a warning code (`W999`, `W42`, `E042`) is refused before the compile, with the list of valid codes. |
| `--with-seed` | `bool` | `false` | Require `data/seed.sql`, and use only its tables as the source schemas. Without the flag, Rocky still uses the seed when the project has one. The flag makes a missing or broken seed an error. Requires the `duckdb` feature (enabled by default in the shipped binary). |
| `--strict-contracts` | `bool` | `false` | Refuse a contract column whose declared type Rocky cannot check. The `I003` info note becomes the `E059` error, which names the model and the column, says why the type is unknown, and says how to fix it. Same as `[contracts] strict = true`. |
| `--strict-sources` | `bool` | `false` | Treat every known source schema as current. A reference to a column the source lacks is the `E041` error, even when the schema came from a seed or an old cache entry. Without the flag, those schemas give the `W041` warning. A read of a table missing from a known schema is likewise the `E045` error instead of the `W045` warning. Same as `[cache.schemas] strict_sources = true`. See [Missing columns in external sources](/concepts/compiler/#missing-columns-in-external-sources-e041--w041). |
| `--dbt-project <DIR>` | `PathBuf` | | **Experimental.** Compile a dbt project in place (attach mode). See [Attach to a dbt project](#attach-to-a-dbt-project-experimental). Conflicts with `--models` and `--with-seed`. |

### Examples

Compile all models:

```bash
rocky compile
```

#### The whole project by default

With no `--models`, `rocky compile` reads the models of every transformation pipeline in `rocky.toml` and compiles them as one project graph. A model in one pipeline that reads the output of another pipeline gets that output's column types. So a type error across pipelines is found in one command.

```
[pipeline.transform]  models = "models/**"     ─┐
                                                 ├─► one compile, one graph
[pipeline.reporting]  models = "reporting/**"  ─┘
```

Each model is still checked against the warehouse of the pipeline that runs it. Two model files with the same name in different pipelines are an error. A project with no transformation pipeline compiles the `models` directory. `--models <PATH>` compiles that one directory only.

#### Source schemas

Rocky types the models that read source tables from these schemas, in this order:

1. With `--with-seed`: the tables of `data/seed.sql` only.
2. Otherwise: the schema cache, when `[cache.schemas]` is enabled.
3. Plus, when the project has `data/seed.sql`: every seed table the cache does not hold.

The seed runs in an in-memory DuckDB and contacts no warehouse. A seed that fails to run is skipped, unless you pass `--with-seed`. The seed is SQL from your repository, so only the `rocky compile` command runs it, not the compile behind `rocky serve` or the MCP compile tool. `data/seed.sql` is beside `rocky.toml` for a whole-project compile, and one level above the directory with `--models <PATH>`.

```json
{
  "version": "1.11.0",
  "command": "compile",
  "models": 14,
  "execution_layers": 4,
  "has_errors": false,
  "diagnostics": [],
  "compile_timings": { "load_ms": 8, "resolve_ms": 2, "typecheck_ms": 42 },
  "models_detail": [
    {
      "name": "fct_revenue",
      "strategy": { "type": "full_refresh" },
      "target": { "catalog": "acme_warehouse", "schema": "gold", "table": "fct_revenue" },
      "freshness": { "time_column": "order_date", "warn_after": "24h" },
      "contract_source": "auto",
      "cost_hint": {
        "estimated_rows": 10000,
        "estimated_bytes": 2560000,
        "estimated_cost_usd": 0.0000228,
        "confidence": "high"
      },
      "depends_on": ["stg_orders", "stg_refunds"],
      "tags": { "domain": "finance", "tier": "gold", "owner": "analytics", "region": "emea" }
    }
    /* one entry per model */
  ]
}
```

`models_detail` carries each model's declarative shape: `name`, the materialization `strategy` (wire form `{"type": "..."}`), the `target` coordinates and the direct `depends_on` list always. `freshness`, `contract_source` (`"auto"` for a sibling `.contract.toml`, `"explicit"` for one from `--contracts`) and `cost_hint` (when upstream statistics support an estimate) appear only when they apply. `tags` is the model's `[tags]` merged over any config-group baseline, sidecar winning. Rocky omits empty `tags`, empty `depends_on` and absent optional fields.

Every diagnostic carries a severity (`"error"`, `"warning"`, `"info"`), a code (`E###`, `W###`, `P###` portability lints, or `V###` validation), the owning model, and, when the compiler can locate it, a `span` and `suggestion`.

#### Selecting one model

`--model` is exact: an unknown name is an error. Rocky still compile-checks the full project for dependency and type context, but the counts, layers, model details, diagnostics and failure state describe only the selected model. `execution_layers` counts only the layers containing it, so it reports `1`.

The selector filters diagnostics by exact attribution: a diagnostic is reported only when its owning model equals the selector, and the exit status follows that set.

- A hard compilation failure, such as SQL that cannot be parsed, fails the command whatever the selector. So do failures in semantic-graph construction and contract loading.
- An error diagnostic attributed to another model is filtered out, so the selected model reads clean even though `rocky run` would exclude that other model as a compile error. Selecting a model tells you nothing about whether its upstreams compile.
- Import-level diagnostics (`E033`, `E034`, `W012`) carry the import name, and `W011` a contract name, so no selector surfaces them. Run without a selector.
- A target collision (`E036`) attaches to every participating model, so selecting any one reports it.

To check whether a model can be built, compile without a selector. `rocky run --model` builds only that model and needs `--defer` to resolve upstreams you did not build.

`--target-dialect bq` reports each non-portable construct (such as `NVL`) as a `P001` `error` diagnostic with its `span` and a `suggestion`.
The `--target-dialect` flag and the `[portability]` config block (see [Configuration](/reference/configuration/)) drive the same check. Project-wide allow-lists and per-model `-- rocky-allow: …` pragmas exempt specific constructs. See [Portability linting](/concepts/linters/#p001--dialect-portability).

`--with-seed` runs `data/seed.sql` in an in-memory DuckDB and gives the compiler the column types of the tables it made, so inference gets concrete types instead of `RockyType::Unknown`. It errors if `data/seed.sql` is missing or fails. A reference to a column the seed lacks is `W041`. A read of a table the seed lacks, in a schema it creates, is `W045`. The compile still exits `0`. `--strict-sources` makes them the `E041` and `E045` errors.

#### Attach to a dbt project (experimental)

:::caution
`--dbt-project` is an experimental spike. Its behavior can change in any release.
:::

Compile a dbt project without migrating it:

```bash
dbt compile --full-refresh
rocky compile --dbt-project path/to/dbt
```

```
<DIR>/target/manifest.json ──▶ version check ──▶ import-dbt rules ──▶ temp dir ──▶ compile
   (+ run_results.json)          (v12 only)       (in memory)         (removed)
```

- Rocky reads `<DIR>/target/manifest.json` and `run_results.json` on every run, and writes nothing under `<DIR>`. The translated project lives in a temp directory that Rocky removes at the end.
- Rocky refuses the same models as `rocky import-dbt`, naming each model and construct, and exits non-zero. An incremental model needs a matching `run_results.json` from `dbt compile --full-refresh`.
- Rocky accepts manifest schema `v12` (dbt 1.8 and later) and refuses any other `dbt_schema_version`.
- `--config` is ignored. The adapter comes from `<DIR>/profiles.yml`.
- `--output json` prints the normal compile JSON. Attach notes go to stderr, and diagnostic paths point into the temp directory.

### Related Commands

- [`rocky lineage`](#rocky-lineage) -- trace column-level dependencies
- [`rocky test`](#rocky-test) -- run local model tests
- [`rocky ci`](#rocky-ci) -- compile + test in one step
- [`rocky serve`](/reference/commands/development/#rocky-serve) -- expose the semantic graph via HTTP

---

## `rocky lineage`

Show column-level lineage for a model, tracing how each output column is derived from upstream sources.

```bash
rocky lineage <target> [flags]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `target` | `string` | **(required)** | Model name, or `model.column` to trace a specific column. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--models <PATH>` | `PathBuf` | `models` | Directory containing model files. |
| `--column <NAME>` | `string` | | Specific column to trace (alternative to `model.column` syntax). |
| `--format <FORMAT>` | `string` | | Output format. Use `dot` for Graphviz DOT output. |
| `--downstream` | `bool` | `false` | Walk the column-level graph forward (consumers) instead of backward (sources). Mutually exclusive with `--upstream`. |
| `--upstream` | `bool` | `true` | Walk the column-level graph backward (sources). Default; the flag exists for explicitness in scripted callers. |

### Examples

Show lineage for a model. Returns the model's columns, its upstream and downstream models, and every column-level edge with the transform kind:

```bash
rocky lineage fct_revenue
```

```json
{
  "version": "1.6.0",
  "command": "lineage",
  "model": "fct_revenue",
  "columns": [
    { "name": "customer_id" },
    { "name": "revenue_amount" }
  ],
  "upstream": ["stg_orders", "stg_refunds"],
  "downstream": [],
  "edges": [
    {
      "source": { "model": "stg_orders", "column": "customer_id" },
      "target": { "model": "fct_revenue", "column": "customer_id" },
      "transform": "direct"
    },
    }
  ]
}
```

When the project declares [downstream consumers](/concepts/downstream-consumers/) that read the model, directly or through downstream models, the output adds a `consumers` array. Each entry has `name`, `kind`, `direct`, and `owner`, `url` and `description` when set. The human view prints a `Consumers:` list. The array is left out when no consumer reads the model.

Tracing a single column returns a flat trace shape instead. Use either `--column` or `model.column` syntax:

```bash
rocky lineage fct_revenue --column revenue_amount
```

```json
{
  "version": "1.6.0",
  "command": "lineage",
  "model": "fct_revenue",
  "column": "revenue_amount",
  "direction": "upstream",
  "trace": [ /* LineageEdgeRecord entries, same shape as edges above */ ]
}
```

Export a column trace as Graphviz DOT (the `model.column` shorthand works too):

```bash
rocky lineage fct_revenue.revenue_amount --format dot | dot -Tpng -o lineage.png
```

`rocky lineage stg_orders.customer_id --downstream` walks forward ("what breaks if I change this?"). The output has `"direction": "downstream"` and a `trace` of edges in the same shape. Upstream output has `"direction": "upstream"`.

#### Value derivation and row selection

A column trace reports two kinds of edge, labelled separately:

- **Value derivation** (`trace`): the source column feeds the output value. `SUM(s.amount) AS total` derives `total` from `amount`.
- **Row selection** (`row_selection`): the source column decides which rows or groups exist. It feeds no output value directly.

```text
SELECT c.region, SUM(s.amount) AS total
FROM stg s JOIN cust c ON s.customer_id = c.customer_id   <- join_key: stg.customer_id, cust.customer_id
WHERE s.status = 'paid'                                   <- filter:   stg.status
GROUP BY c.region                                         <- group_by: cust.region
                     SUM(s.amount) ─────────────────────▶ value:    stg.amount -> total
```

Each entry names the `source` column, the `target_model` whose rows it affects, and a `kind`: `join_key`, `filter`, `group_by`, `having`, `qualify`, `window_partition`, `window_order`, `distinct_on`, or `order_limit`. A `distinct_on` key comes from `DISTINCT ON (...)`, or from the `ORDER BY` of a `DISTINCT ON` query, because that order picks which row of each group survives. An `order_limit` key is an `ORDER BY` key in a query that also has `LIMIT`, `OFFSET`, `FETCH`, or `TOP`. A `NATURAL JOIN` records no join key, because the shared columns are known only to the warehouse catalog. A window key also carries `target_column`, the one output column its window feeds. Without `target_column`, the edge affects every column of `target_model`.

Upstream, `row_selection` lists the row-selection inputs of every model on the value trace. Downstream (`--downstream`), it lists the models whose rows the traced column, or a column derived from it, filters, joins, groups, or partitions. The table output prints the same edges under a `Row selection` heading. `trace` and `edges` stay value-only, so existing consumers see no change. JSON omits `row_selection` when it is empty.

Row selection is read from the model's top-level `SELECT`. These constructs produce no row-selection edge yet:

- predicates inside a `WITH` body, a derived table, or a subquery expression (`IN (SELECT …)`, `EXISTS`)
- correlated references
- `GROUP BY ALL` and `NATURAL` joins
- set operations (`UNION`, `INTERSECT`, `EXCEPT`)
- a named window that references another named window
- an unqualified column in a multi-table query, because its table cannot be determined statically

`GROUP BY 1` and `GROUP BY <alias>` resolve to the projected expression's source columns.

#### Incomplete lineage

When a model uses one of the constructs above, its lineage is missing edges, and impact through it can be under-reported. `rocky lineage` says so. JSON output has an `incomplete_lineage` list, sorted by model name:

- It covers the model and every model upstream and downstream of it, for a model view and for a column trace (upstream or `--downstream`).
- It follows model dependencies, not column edges, so a reader that joins with `NATURAL JOIN` is listed even though no column edge reaches it.
- `rocky lineage-diff` and `rocky ci-diff` report it too, for the changed models and the models downstream of them (see [incomplete lineage in a diff](#incomplete-lineage-in-a-diff)).
- The table output prints the same models under a `Lineage incomplete` heading.

`gaps` names each construct: `natural_join`, `group_by_all`, `set_operation`, `cte_body_row_selection`, `derived_table_row_selection`, `subquery_expression`, `nested_named_window`, `unmodelled_relation` (`PIVOT`, `UNNEST`, a table function, `VALUES`), `unattributed_column` (an unqualified column in a multi-table query), `unresolved_projection`, and `unresolved_star` (a `SELECT *` over a relation with unknown columns). An empty list (`[]`) means no model in scope has a known gap. A missing field, or `gaps: null`, means Rocky did not compute it: treat that as possibly incomplete, never as complete.

### Related Commands

- [`rocky compile`](#rocky-compile) -- build the semantic graph that lineage reads
- [`rocky ai-explain`](/reference/commands/ai/#rocky-ai-explain) -- generate natural language descriptions of model logic

---

## `rocky lineage-diff`

Report the downstream blast radius of a change between two git refs, for PR review. It combines the structural diff from `rocky ci-diff` with the downstream consumers from `rocky lineage --downstream`. Together they show which downstream columns each changed column reaches.

By default the report describes the HEAD commit. Uncommitted edits are ignored. Pass `--working-tree` to include them. See [snapshot modes](#snapshot-modes) under `rocky ci-diff`.

```bash
rocky lineage-diff [base_ref] [flags]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `base_ref` | `string` | `main` | Git ref to compare against. Uses the same git-diff mechanism as [`rocky ci-diff`](#rocky-ci-diff). |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--models <PATH>` | `PathBuf` | `models` | Directory containing model files. |
| `--working-tree` | flag | off | Compare the working tree instead of the HEAD commit, including staged, unstaged, untracked, renamed, and deleted files. |
| `-o, --output <FORMAT>` | `json` \| `table` \| `md` | terminal-aware | `json` emits the full payload, including the pre-rendered report in a `markdown` field. `table` and `md` print that same report directly. |

### Examples

Diff the current branch against `main`. `--output table` prints the Markdown report directly, with a collapsible table of column changes and downstream consumers for each model. Use JSON in a CI pipeline:
```bash
rocky lineage-diff main -o json
```

```json
{
  "version": "1.74.0",
  "command": "lineage-diff",
  "base_ref": "main",
  "head_ref": "HEAD",
  "summary": { "total_models": 2, "unchanged": 0, "modified": 2, "added": 0, "removed": 0 },
  "results": [
    {
      "model_name": "fct_revenue",
      "status": "modified",
      "column_changes": [
        { "column_name": "total_revenue", "change_type": "added", "new_type": "Unknown" },
        { "column_name": "total_tax", "change_type": "added", "new_type": "Unknown" },
        { "column_name": "total", "change_type": "removed", "old_type": "Unknown" }
      ]
    },
    {
      "model_name": "stg_orders",
      "status": "modified",
      "column_changes": [
        { "column_name": "amount_usd", "change_type": "added", "new_type": "Unknown",
          "downstream_consumers": [{ "model": "fct_revenue", "column": "total_revenue" }] }
      ]
    }
  ],
  "markdown": "### Rocky Lineage Diff\n\n..."
}
```

A removed column has no downstream trace on HEAD. JSON omits an empty `downstream_consumers`, so default a missing key to an empty list.

#### Consumers of a removed column

A removed column carries `consumer_impact` instead. Rocky compares the base and HEAD lineage graphs and classifies every model that read the column directly. A renamed column shows up as one removed and one added column, so its consumers are classified the same way.

| `status` | Meaning |
|----------|---------|
| `newly_broken` | HEAD still reads the removed column, or a model now reads it. This consumer breaks. |
| `unknown` | Rocky cannot decide. HEAD did not compile, or the consumer mentions the column where lineage cannot tell which relation it reads. |
| `deleted` | The consumer model no longer exists on HEAD. |
| `repaired` | The consumer still exists on HEAD and provably no longer reads the column. |

A read counts through either edge kind: a value read, or a row-selection read such as a join key or filter (see [`rocky lineage`](#value-derivation-and-row-selection)). Each entry carries `model`, `status`, `columns` (the consumer's output columns involved), `via` (`value` or a row-selection kind), and a one-line `reason`. For example `{ "model": "fct_broken", "status": "newly_broken", "columns": ["amount"], "via": ["value"], "reason": "HEAD still reads `stg_orders.amount`, which no longer exists" }`.

The Markdown report adds a **Consumers of removed columns** table per model. `unknown` is the conservative answer: Rocky reports `repaired` only when HEAD's lineage proves it. A model that reads the upstream with `SELECT *` while HEAD cannot list the upstream's columns is `unknown`, and so is an unqualified column name in a join.

#### Incomplete lineage in a diff

The impact report reads the same edges as `rocky lineage`, so a model whose lineage is incomplete can hide impact. Both `rocky lineage-diff` and `rocky ci-diff` have an `incomplete_lineage` list, with the same shape as in [`rocky lineage`](#incomplete-lineage):

- It covers every changed model (added, modified or removed) and every model downstream of one on HEAD. A removed model's downstream models are found on the base side.
- `[]` means none of them has a known gap.
- `null` means Rocky does not know: models changed, but the HEAD side did not compile. A payload with no field comes from an older build. Treat both as possibly incomplete, never as complete.
- The Markdown report, and so the table output, adds a **Lineage incomplete** list, or one line when it is not known.

A filter inside a `WITH` body marks a model incomplete, so the list can be long on CTE-heavy models.

`rocky lineage-diff` reports and never fails a build on changed columns. It exits non-zero only on an error: an invalid `base_ref`, a failed `git diff`, or invalid or unreadable project configuration.

### Related Commands

- [`rocky ci-diff`](#rocky-ci-diff) -- the structural diff alone, without the downstream trace
- [`rocky lineage`](#rocky-lineage) -- trace a single column's lineage directly

---

## `rocky catalog`

Write a project-wide column-level lineage snapshot to disk: a `catalog.json` front door plus `edges.parquet` and `assets.parquet`. BI tools, governance dashboards and review bots can then query lineage without the engine.

```bash
rocky catalog [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--models <PATH>` | `PathBuf` | `models` | Directory containing `.sql` and `.toml` model files. |
| `--out <PATH>` | `PathBuf` | `.rocky/catalog/` | Output directory. `catalog.json` is written to `<out>/catalog.json`; the Parquet artifacts to `<out>/edges.parquet` and `<out>/assets.parquet`. |
| `--format <FORMAT>` | `json` \| `parquet` \| `both` | `both` | Which artefact family to emit. `json` writes only `catalog.json`; `parquet` writes only `edges.parquet` + `assets.parquet`; `both` writes all three. |
| `--catalog <NAME>` | `string` | | Scope the snapshot to a single warehouse catalog. Only assets whose FQN sits in the named catalog are emitted, and edges referencing dropped assets are pruned. |

### Behaviour

The stdout is a short summary in `--output table` mode, or the same JSON payload in `--output json` mode. The artifact contains:

- `assets`: one entry per model or upstream source, with columns (name, inferred type and nullability when known, sidecar `[columns]` `description`), upstream and downstream lists, and the model's intent description.
- `edges`: one entry per column-level lineage edge: source column, target column, transform kind (`direct`, `cast`, `try_cast`, `expression`, `aggregation: <fn>`; a nested call such as `MAX(LENGTH(n))` is `expression`), and a confidence grade (`High` for explicit projections, `Medium` for star-expanded edges, `Low` reserved).
- `stats`: `asset_count`, `edge_count`, `column_count`, `assets_with_star`, `orphan_columns`, `duration_ms`.
- `config_hash`: a fingerprint of `rocky.toml`, so consumers can tell whether the catalog matches the current configuration.

### Examples

```bash
rocky catalog                               # the default snapshot
rocky catalog --output json | jq '.stats'
rocky catalog --out build/catalog           # a custom directory, such as a per-PR artifact
```

### Limitations

- Per-asset `last_run_id` and `last_materialized_at` are populated from the state store when a matching successful run exists; they stay `null` for assets that have never been materialized (or built before the run history was captured).
- Lineage extraction inherits the existing extractor's value-lineage coverage: CTEs, set operations, and `CASE WHEN` projections are not yet surfaced as edges. Row-selection edges (join keys, filters, group and window keys) are reported by [`rocky lineage --column`](#value-derivation-and-row-selection) but are not yet written to the catalog. Asset-level partial lineage is flagged via `stats.assets_with_star`.

### Related Commands

- [`rocky lineage`](#rocky-lineage) -- per-model lineage exploration with `--column` traces
- [`rocky compile`](#rocky-compile) -- build the semantic graph that the catalog reads

---

## `rocky dag`

Print the whole project as one graph. Every pipeline stage is a node, the dependencies between stages are edges, and the nodes are grouped into the layers they execute in. `rocky run --dag` executes that same graph.

```bash
rocky dag [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--models <PATH>` | `PathBuf` | each pipeline's own configured location | Override the models directory for every transformation pipeline. Rocky refuses a project with more than one such pipeline, rather than build each model twice. |
| `--seeds <PATH>` | `PathBuf` | | Seeds directory. |
| `--contracts <PATH>` | `PathBuf` | | Contracts directory. |
| `--column-lineage` | `bool` | `false` | Also emit column-level lineage edges. This one requires a compile, so it costs more than the plain graph. |

### What a node carries

Each node has an id `{kind}:{name}`, for example `transformation:stg_orders`. The kind is `source`, `load`, `transformation`, `quality`, `snapshot`, `seed`, `test` or `replication`. A node also carries the fields that apply to its kind: the pipeline name, target table, materialization strategy, per-model freshness expectation, partition shape, and the ids it depends on.

### How execution layers work

`execution_layers` is the graph sorted into runnable groups.

```
   execution_layers        what the grouping means
   ────────────────        ────────────────────────────────────────
   layer 0  [ A , B ]      A and B depend on nothing, so Rocky can
              │            run them at the same time
              ▼
   layer 1  [ C , D ]      C and D depend only on earlier layers,
              │            never on each other
              ▼
   layer 2  [ E ]          E waits for every layer before it
```

### Reading `column_lineage` correctly

`column_lineage` is empty unless you pass `--column-lineage`. An empty list on its own does not mean the project has no lineage.

- You did not pass `--column-lineage`: nothing was computed, and you can conclude nothing from the empty list.
- You passed it and `column_lineage_unavailable` is absent: the list is the complete answer, empty included.
- You passed it and `column_lineage_unavailable` is present: it carries a human-readable reason, and the empty list must **not** be read as "no lineage".

### Examples

Run `rocky dag` for the whole project, and add `--column-lineage` for column-level edges.

### Related Commands

- [`rocky emit-sql`](#rocky-emit-sql) -- render the SQL for the models this graph orders
- [`rocky catalog`](#rocky-catalog) -- the same lineage, written to disk as a queryable snapshot
- [`rocky run --dag`](/reference/commands/core-pipeline/#rocky-run) -- execute every pipeline as one graph

---

## `rocky emit-sql`

Render the runnable SQL each transformation model would emit, with no warehouse connection. It uses the same generation path as `rocky run`, including surrogate-key columns wrapped as at materialization.

```bash
rocky emit-sql [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--models <PATH>` | `PathBuf` | `models`, or the `--pipeline`'s own models location | Directory containing `.sql` and `.toml` model files. |
| `--model <NAME>` | `string` | | Render one model by exact name. Cannot be combined with `--select`. |
| `--select <SELECTOR>...`, `-s` / `--exclude <SELECTOR>...` / `--state-ref <REF>` / `--state-working-tree` | `string` | | Render only the [selected models](/reference/node-selection/). |
| `--out-dir <PATH>` | `PathBuf` | | Write one `<model>.sql` file per model into this directory, in dependency order. When omitted, the concatenated SQL is printed to stdout (also in dependency order). |
| `--var <NAME=VALUE>` | `string` (repeatable) | | Bind a run variable, so the emitted SQL shows the resolved `@var(name)` text. |
| `--pipeline <NAME>` | `string` | the only transformation pipeline | Transformation pipeline whose target adapter sets the SQL dialect. Required when the project has more than one transformation pipeline. A name that matches no transformation pipeline is an error. |

### Dialect and the runnable guarantee

The dialect is the project's configured target adapter type, read from `rocky.toml`. No credentials are needed and no connection is opened. All models render in this one resolved dialect, so for a project whose models target more than one adapter, the emitted SQL matches `rocky run` only for the models whose target uses that dialect.

Three outcomes, and the middle one is the point of the no-credentials promise:

| `rocky.toml` | What happens |
|---|---|
| No file at the given path | Renders in DuckDB, the default dialect. |
| Present, with `${VAR}` placeholders in an adapter's connection fields you have not exported | Renders in the configured dialect. A credential is never sent anywhere, so it does not have to resolve. An unset placeholder anywhere else — an adapter `type`, an `[imports]` path — still refuses, because it changes what the config means. |
| Present but malformed, or it breaks a config rule | Refuses, and names the file. Rendering a broken Snowflake project in DuckDB would answer a question you did not ask. |

With a config present, `emit-sql` also runs the per-model-target checks of [`rocky compile`](#rocky-compile) (E042/E043, E044, E049, E051, E053, E054). It refuses to emit SQL that the pipeline's warehouse cannot run, such as a `merge` model on ClickHouse (E053).

A placeholder written as a bare value, such as `port = ${PORT}`, is not valid TOML. That is row three, and the error names `PORT`.

Full-refresh models emit a complete `CREATE OR REPLACE TABLE … AS …` that runs as-is against a fresh warehouse and matches what a run executes in the resolved dialect. Merge and `delete_insert` models emit their steady-state statement, which operates on an existing target. `rocky run` creates the table on first build and a static emit cannot, so those files carry a leading `-- NOTE: merge/delete_insert statement` comment.

`emit-sql` refuses a project with any compile error, before it filters by model. `type = "incremental"` on a transformation model fails with `E037`, and an invalid `ephemeral` use fails with `E038`. Either stops the whole export, even when `--model` names a different model.

An `ephemeral` model gets no file of its own and is reported as skipped. Each consumer's statement carries it as a `__rocky_ephemeral__<model>` CTE.

A model whose SQL cannot be rendered offline is reported on stderr rather than silently dropped. So you never mistake the emitted set for the complete project. A Snowflake dynamic table is one: it needs a live compute-warehouse name.

### Examples

Print the whole project's SQL to stdout in dependency order:

```bash
rocky emit-sql
```

The output is `-- model: <name>` followed by that model's statement, in dependency order.

Write one file per model (`--out-dir`), render one model, or capture everything in one file:

```bash
rocky emit-sql --out-dir build/sql/
rocky emit-sql --model fct_revenue
rocky emit-sql > build/all.sql
```

A model that cannot be emitted as standalone SQL is reported on stderr, for example `emit-sql: 1 model(s) not emitted:` followed by `- dim_session (cannot render offline: <the dialect's reason>)`.

### Related Commands

- [`rocky dag`](#rocky-dag) -- inspect the dependency order `emit-sql` renders in
- [`rocky catalog`](#rocky-catalog) -- the same compiled graph, exported as a lineage snapshot rather than runnable SQL
- [No lock-in](/guides/no-lock-in/) -- the full fallback recipe for stepping away from the engine

---

## `rocky lint`

Check model SQL for style problems. The rules find queries that are hard to read or easy to break, such as a bare `JOIN` or a column with no table name in a two-table query. They do not check that a query is correct. `rocky compile` does that.

```bash
rocky lint                          # Lint every .sql file under models/
rocky lint models/marts/            # Lint one directory
rocky lint models/fct_orders.sql    # Lint one file
rocky lint --fix                    # Rewrite the fixable findings in place
rocky lint --output json            # Machine-readable findings
```

`rocky lint` reads `.sql` files. It does not read `.rocky` files. Use [`rocky fmt`](/reference/cli/#rocky-fmt) for those.

**Arguments and flags:**

| Argument or flag | Default | Description |
|------------------|---------|-------------|
| `[PATHS]...` | `models` | `.sql` files or directories. Directories are searched recursively. Hidden directories and `target` are skipped. |
| `--fix` | off | Rewrite the findings marked "fixable" in the table below. Rocky does not write a file if the fix would make a readable file stop parsing. |

### Rules

| Code | Name | Default severity | Flags | Fix |
|------|------|------------------|-------|-----|
| `S001` | `ambiguous-column` | warning | A column with no table qualifier in a query that reads two or more tables. `JOIN ... USING` columns, output aliases, subquery columns and lambda parameters are not flagged. | Report only |
| `S002` | `implicit-inner-join` | warning | A bare `JOIN`. Write `INNER JOIN`. | `--fix` inserts `INNER` |
| `S003` | `select-star` | info | `SELECT *` or `t.*` in the final result of a statement. A `SELECT *` in a CTE or a subquery is not flagged. | Report only |
| `S004` | `target-order` | info | A plain column listed after a calculated column. Order the list as wildcards, plain columns, then calculations. | Report only |
| `S005` | `keyword-case` | warning | A keyword whose capitalisation differs from the rest of the file. The majority style wins. | `--fix` recases |
| `S006` | `trailing-whitespace` | warning | Spaces or tabs at the end of a line. Lines inside a multi-line string are not touched. | `--fix` trims |
| `S007` | `tab-character` | warning | A tab outside a string or a comment. | `--fix` writes spaces |

`S004` is report only because moving a column changes the model's output schema. `S003` complements the `P002` lint in [Linters](/concepts/linters/). `P002` fires only when a downstream model reads specific columns. `S003` fires on every final `SELECT *`.

`S001`, `S003` and `S004` need a parse. If a file does not parse, Rocky skips these three rules for that file, prints a note, and still runs the text rules.

### Configuration

Switch rules off or change their severity in `rocky.toml`. An unknown rule code is an error.

```toml
[lint]
disable = ["S003", "S004"]

[lint.severity]
S001 = "error"
```

Severity is `"error"`, `"warning"` or `"info"`. The command exits with code `1` when at least one finding has `error` severity. Warnings and info findings never fail the run.

### JSON output

`rocky lint --output json` prints a `LintOutput` object:

```json
{
  "version": "1.78.0",
  "command": "lint",
  "files_checked": 3,
  "findings": [
    {
      "code": "S002",
      "rule": "implicit-inner-join",
      "severity": "warning",
      "file": "models/fct_orders.sql",
      "line": 8,
      "col": 1,
      "message": "bare JOIN does not say which kind of join it is",
      "hint": "write INNER JOIN",
      "fixable": true
    }
  ],
  "counts": { "error": 0, "warning": 1, "info": 0 },
  "fixed": 0,
  "files_fixed": [],
  "ast_rules_skipped": []
}
```

With `--fix`, `findings` lists what is left after the rewrite. `fixed` counts the findings that were rewritten.

### Related Commands

- [`rocky compile`](#rocky-compile) -- correctness diagnostics, including the `P001` and `P002` lints
- [Linters](/concepts/linters/) -- the semantic lints that run inside `rocky compile`

---

## `rocky test`

Run local model tests via DuckDB without needing warehouse credentials. Validates model SQL, contract compliance, and user-defined test assertions.

```bash
rocky test [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--models <PATH>` | `PathBuf` | `models/` beside the config file | Directory containing model files. |
| `--contracts <PATH>` | `PathBuf` | | Directory containing data contract definitions. Default: the project `contracts/` directory beside the models directory. |
| `--model <NAME>` | `string` | | Run tests for a single model only. |
| `--select <SELECTOR>...`, `-s` / `--exclude <SELECTOR>...` / `--state-ref <REF>` / `--state-working-tree` | `string` | | Report only the [selected models](/reference/node-selection/). Every model still runs. Not with `--declarative`. |
| `--var <NAME=VALUE>` | `string` (repeatable) | | Bind a run variable, as in `rocky compile --var`. |
| `--declarative` | `bool` | `false` | Run the `[[tests]]` of model sidecars against the warehouse instead of DuckDB. |
| `--pipeline <NAME>` | `string` | | With `--declarative`: run the tests of the models in `--models` against this pipeline's warehouse. Without it and without `--models`, every transformation pipeline runs its own models' tests against its own warehouse. |

`rocky test` runs `data/seed.sql` first, when the project has one, and types its compile from the tables the seed made. The models then run on those tables. So a contract type mismatch (`E011`) or a column the seed lacks (`W041`) is reported before any model runs.

With a `rocky.toml`, the compile also runs the per-model-target checks of [`rocky ci`](#rocky-ci) (`E042`/`E043`, `E057`, `E044`, `E049`, `E051`, `E053` and `E054`). An error from them fails `rocky test` before any model runs.

### Examples

Run all model tests:

```bash
rocky test
```

```json
{
  "version": "1.6.0",
  "command": "test",
  "total": 14,
  "passed": 12,
  "failed": 2,
  "failures": [
    { "name": "fct_orders.not_null(order_id)", "error": "found 3 null values" },
    { "name": "fct_orders.unique(order_id)",   "error": "found 1 duplicate" }
  ]
}
```

Add `--model fct_revenue --contracts contracts/` to test one model with contracts.

The default `rocky test` path also runs fixture-driven `[[test]]` unit tests declared in model sidecars. Each `[[test]]` block mocks upstream inputs with inline rows (`given`) and asserts the model's output rows (`expect`), executed in-memory against DuckDB. When at least one model declares a `[[test]]` block, the JSON output gains a `unit_tests` summary; the key is omitted entirely when no model declares one. A failing unit test makes `rocky test` exit non-zero, the same as a failing model assertion.

Each `results` entry carries the model name, the `[[test]]` block's `test` name, a `passed` flag, an `error` message (`null` when the test passed), and the `mismatches` array of row-level diffs. Each mismatch renders its row as `col=val, col=val`. A mismatch `kind` is `missing` (expected but not produced), `extra` (produced but not expected), or `value_diff` (same positional row, differing values, from an `ordered` expectation).

`--declarative` is a separate surface: it adds a `declarative` block summarising `[[tests]]` (plural) declared in model sidecars, run against the configured warehouse adapter rather than DuckDB. See [Checks and Tests](/concepts/data-quality-checks/) for both surfaces.

### A selector that matches nothing fails

`rocky test` exits `1` when a selector names something the project does not have. This holds with and without `--declarative`.

| What you passed | Message on stderr |
|---|---|
| `--model <NAME>` naming no model in the project | `model '<NAME>' not found (no transformation model with that name)` |
| `--models <PATH>` naming a missing or empty directory | `no models found in <PATH>` |

Rocky refuses before it writes any output. Under `--output json` stdout stays empty, so read the exit code and stderr rather than the payload.

A model that exists but declares no tests is not an error. It still exits `0` and reports `total: 0`.

### Related Commands

- [`rocky compile`](#rocky-compile) -- compile models before testing
- [`rocky ci`](#rocky-ci) -- compile + test in one step
- [`rocky ai-test`](/reference/commands/ai/#rocky-ai-test) -- generate test assertions from model intent

---

## `rocky ci`

Run the full CI pipeline: compile all models and run all tests. Designed for use in CI/CD environments where no warehouse credentials are available. Returns a non-zero exit code if any compilation error or test failure occurs.

```bash
rocky ci [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--models <PATH>` | `PathBuf` | every pipeline | Run only the models in this directory. Without it, `rocky ci` runs the models of every transformation pipeline. |
| `--contracts <PATH>` | `PathBuf` | | Directory containing data contract definitions. Default: the project `contracts/` directory beside the models directory, which is `models/` beside the config file when you pass no `--models`. |
| `--strict-contracts` | `bool` | `false` | Refuse a contract column whose declared type Rocky cannot check: the `I003` note becomes the `E059` error. Same as `[contracts] strict = true`. |
| `--var <NAME=VALUE>` | `string` (repeatable) | | Bind a run variable, as in `rocky compile --var`. |

With no `--models`, `rocky ci` compiles every transformation pipeline's models as one project graph, as [`rocky compile`](#the-whole-project-by-default) does. Then it runs them all in one in-memory DuckDB, in dependency order. So a model in a downstream pipeline reads the tables its upstream pipelines made.

```
data/seed.sql ─► in-memory DuckDB ─► compile (typed from the seed) ─► run every model, upstream first
```

The seed is `data/seed.sql` beside `rocky.toml`. The compile is typed from the tables it made, so `rocky ci` finds a contract type mismatch (`E011`) from the seed alone.

The compile runs the same per-model-target checks as [`rocky compile`](#rocky-compile). These are `E042`/`E043` (operand types), `E057` (unknown function), `E044` (`GROUP BY`), `E049`, `E051`, `E053` and `E054`. Each model is judged against the warehouse of the pipeline that loads it, as read from `rocky.toml`. An error from these checks fails `rocky ci` before any model runs, and its code is in `diagnostics`. `E054` is judged on the SQL each model runs, with its ephemeral upstreams inlined, as `rocky run` sends it.

The project files are found beside the config file, not the working directory. So `rocky --config sub/rocky.toml ci` run from the directory above reads `sub/contracts/` and `sub/functions/`. `rocky compile` and `rocky test` do the same when you pass no `--models`.

A dependency cycle is the `E058` error, one for each model on the cycle. `rocky ci` prints its JSON with these diagnostics, runs no model, and exits `1`. See [Dependency cycles](/concepts/compiler/#dependency-cycles-e058).

`exit_code` in the JSON is the code the process exits with: `0` when compile and tests pass, `1` when either fails. Warnings do not change it. To act on warnings, read the `"severity": "Warning"` entries in `diagnostics`.

### Examples

Run CI with default paths:

```bash
rocky ci
```

```json
{
  "version": "1.6.0",
  "command": "ci",
  "compile_ok": true,
  "tests_ok": true,
  "models_compiled": 14,
  "tests_passed": 14,
  "tests_failed": 0,
  "exit_code": 0,
  "diagnostics": [],
  "failures": []
}
```

Run CI with contracts. On a compile error, no tests run: `tests_passed` and `tests_failed` are `0`, and `exit_code` is `1`:

```bash
rocky ci --models src/models --contracts src/contracts
```

### Related Commands

- [`rocky compile`](#rocky-compile) -- compile step only
- [`rocky test`](#rocky-test) -- test step only
- [`rocky ci-diff`](#rocky-ci-diff) -- structural diff of changed models vs a base git ref
- [`rocky validate`](/reference/commands/core-pipeline/#rocky-validate) -- validate config (often run before CI)

---

## `rocky ci-diff`

Detect which models changed between a base git ref and `HEAD`, compile both sides, and report added/modified/removed columns. Emits both JSON (for CI pipelines) and a pre-rendered Markdown block suitable for posting as a PR comment.

```bash
rocky ci-diff [base_ref] [flags]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `base_ref` | `string` | `main` | Git ref to compare against. Rocky shells out to `git diff --name-status <base_ref>...HEAD` to find changed `.sql`, `.rocky`, and sidecar `.toml` files. |

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--models <PATH>` | `PathBuf` | `models` | Directory containing model files. |
| `--working-tree` | flag | off | Compare the working tree instead of the HEAD commit. See [snapshot modes](#snapshot-modes). |
| `--semantic` | flag | off | Also run the typed-IR semantic breaking-change classifier and surface findings under `breaking_findings` in the JSON output. Informational only — even a `Breaking` finding does not change `ci-diff`'s exit code. The hard gate lives on [`rocky branch promote`](/reference/commands/core-pipeline/#rocky-branch). |

### Examples

Diff the current branch against `main`:

```bash
rocky ci-diff
```

```json
{
  "version": "1.31.0",
  "command": "ci-diff",
  "base_ref": "main",
  "head_ref": "HEAD",
  "summary": {
    "added": 1,
    "modified": 2,
    "removed": 0
  },
  "models": [
    {
      "model": "fct_orders",
      "status": "modified",
      "columns": [
        { "name": "order_status", "change": "added" },
        { "name": "amount_cents", "change": "type_changed", "from": "INT", "to": "BIGINT" }
      ]
    }
  ],
  "markdown": "### Model diff vs `main`\n\n| Model | Status | ... |"
}
```

Diff against another base and models directory with `rocky ci-diff release/2026-04 --models src/models`. In GitHub Actions, `jq -r .markdown` the JSON output into `gh pr comment`.

The output also has an `incomplete_lineage` list, the same list as in [`rocky lineage-diff`](#incomplete-lineage-in-a-diff). The table output prints it under a `Lineage incomplete` heading.

`--semantic` adds a `breaking_findings` array, omitted when empty or without the flag. Each finding has a tagged `change` object (a `kind` discriminator such as `column_type_changed`, with `model`, `column`, `old_type`, `new_type` and `narrowing`) and a `severity` (`breaking`, `warning` or `info`).
Use `--semantic` to surface findings on every PR. [`rocky branch promote`](/reference/commands/core-pipeline/#rocky-branch) blocks the promote. The findings are JSON-only: `--output table` does not print them.

### Snapshot modes

The changed-file list and the compiled files always come from the same snapshot. The JSON output reports which one ran in `mode`, and the commit the base side was read from in `base_commit`.

| `mode` | Changed files | Head side compiled from | Base side compiled from |
|--------|---------------|-------------------------|-------------------------|
| `head` (default) | `git diff <base_ref>...HEAD` | the HEAD commit, read from git | the merge base of `base_ref` and HEAD |
| `working_tree` (`--working-tree`) | `git diff -M <merge base>`, plus untracked files that git does not ignore | the files on disk | the merge base of `base_ref` and HEAD |

In `head` mode, uncommitted edits never reach the report. A CI run and a dirty local checkout of the same commit produce the same diff. `--working-tree` covers staged, unstaged, untracked, renamed, and deleted files, for a local preview before you commit. Its `head_ref` is `WORKTREE`.

The base side is the merge base, the same commit `base_ref...HEAD` selects files against. Later commits on the base branch do not show up as changes in your branch. In a shallow clone without the merge base, both selection and the base compile fall back to `base_ref` itself, and `base_commit` is omitted.

### Related Commands

- [`rocky ci`](#rocky-ci) -- full compile + test for CI
- [`rocky compile`](#rocky-compile) -- compile a single branch without diffing
- [`rocky preview`](#rocky-preview) -- pruned re-run + sampled data diff + cost delta on top of `ci-diff`'s structural diff
- [`rocky branch promote`](/reference/commands/core-pipeline/#rocky-branch) -- promote a branch's tables to production with a hard semantic breaking-change gate

---

## `rocky preview`

Preview a change before it merges. Rocky identifies changed model files and downstream models through sidecar `depends_on`. It copies the other models from the base schema into a per-PR branch.

Three subcommands compose into one review artifact. `preview create` prepares the branch, `preview diff` reports what changed, and `preview cost` reports the cost delta against base. A fourth, `preview rows`, is separate: it samples the output rows of a single model.

For the prune set, adapter copy methods, and diff coverage, see [How Preview Works](/concepts/preview-internals/). For a walkthrough, see [Preview a PR](/guides/preview-a-pr/).

```bash
rocky preview create --base <ref> [--name <branch_name>]
rocky preview diff   --name <branch_name> [--base <ref>]
rocky preview cost   --name <branch_name>
rocky preview rows   --model <name> [--cte <name>] [--limit <N>]
```

`preview diff` and `preview cost` put a pre-rendered Markdown report in the `markdown` field of their JSON output. It is ready to post as a PR comment. `preview create` has no such field. There is no `--output markdown`: the valid values are `json`, `table`, and `md`, and `md` behaves like `table` here.

### `rocky preview create`

Compute the prune set and copy the rest from the base schema into a per-PR branch. It does not run the prune set: it reports `run_status: "planned"` with an empty `run_id`. Run `rocky run --branch <name>` before `preview diff` or `preview cost`. That command has no selector for a set of models: it builds the whole pipeline, or one model with `--model`. Both preview commands read only the newest branch run. They find it by its recorded `rocky_branch`, the literal `--branch` value, not by the git branch you have checked out. The base run must be an ordinary production run, without `--branch` or `--shadow`.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--base <REF>` | `string` | `main` | Git ref the change-set is computed against. Rocky shells out to `git diff --name-only <base>...HEAD` against the models directory. |
| `--name <NAME>` | `string` | `pr_preview_<current git branch>` | Branch name to register in the state store. Use 1 to 64 characters from `[A-Za-z0-9_]`. The default is `pr_preview_` plus the git branch name, with every other character replaced by `_`, cut to 64 characters. On a detached HEAD, it is `pr_preview_` plus a timestamp. The branch's `schema_prefix` becomes `branch__<name>` and is the target schema for the pruned run. |
| `--models <PATH>` | `PathBuf` | `models` | Directory containing model files. |

**Example.** Diff against `main` and create a preview branch:

```bash
rocky preview create --base main
```

```json
{
  "version": "1.18.0",
  "command": "preview-create",
  "branch_name": "pr_preview_fix_price",
  "branch_schema": "branch__pr_preview_fix_price",
  "base_ref": "main",
  "head_ref": "HEAD",
  "prune_set": [
    { "model_name": "fct_revenue", "reason": "changed" },
    { "model_name": "rev_by_region", "reason": "downstream_of_changed" }
  ],
  "copy_set": [
    { "model_name": "stg_orders", "source_schema": "main", "target_schema": "branch__pr_preview_fix_price", "copy_strategy": "ctas" }
  ],
  "skipped_set": [],
  "run_id": "",
  "run_status": "planned",
  "duration_ms": 4321
}
```

`changed_columns` exists in the output type but stays empty and is omitted from JSON. `copy_strategy` reports `"ctas"` for every successful copy, though Databricks uses `SHALLOW CLONE`, BigQuery `CREATE TABLE … COPY` and Snowflake `CREATE TABLE … CLONE`.

### `rocky preview diff`

Compare the branch run with the base run, for every model the branch run executed.

Rocky finds both runs in the state store. The branch run is the newest run recorded with `rocky run --branch <name>`. The base run is the newest run made without `--branch` on the git branch or commit that `--base` names. If there is no such base run, the diff stays empty and `base_note` says why.

By default this compares the `rows_affected` the two run records hold. It reports `rows_added` and `rows_removed`, leaves `rows_changed` at 0, returns no samples and no column-level delta, and sets `coverage: "not_yet_sampled"` with `coverage_warning: true`.

Two limits follow. A change that rewrites values without changing row counts shows nothing. And an ordinary transformation run records no `rows_affected` at all.

A model with no recorded row count has an unknown delta. When it ran on both sides, `rows_added` and `rows_removed` are `null`. When it ran only on the branch, `rows_added` is `null` and `rows_removed` is `0`. The Markdown shows `?` for `null`. `summary.models_unknown` counts these models. Rocky counts them as neither changed nor unchanged, and leaves them out of `total_rows_added` and `total_rows_removed`.

Pass `--algorithm bisection` to compare row content. It needs a `Merge` model whose single `unique_key` holds whole numbers: the bounds are parsed as integers, so a decimal key falls back to the default comparison without saying so. Read each model's `algorithm.kind` before you treat its result as a content comparison.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--name <NAME>` | `string` | **(required)** | Branch name created by `preview create`. Rocky matches it against the `rocky_branch` recorded on each run. |
| `--base <REF>` | `string` | `main` | Git branch name or commit that the base run was recorded on. A commit can be a full sha or a prefix of at least 7 characters. It must differ from `--name`. |
| `--models <PATH>` | `PathBuf` | `models` | Models directory. Bisection reads each model's primary-key column from here. |
| `--algorithm <ALGO>` | `sampled` \| `bisection` | `sampled` | Comparison method. Hidden from `--help`. See the `bisection` limits above. |

**Example.** Print a Markdown report ready to post on a PR:

```bash
rocky preview diff --name pr_preview_fix_price --output json | jq -r .markdown
```

The report lives in the `markdown` field of `PreviewDiffOutput`, which also carries the per-model `sampling_window` block with `coverage_warning`.

### `rocky preview cost`

Per-model cost delta between the branch run and a base run. The branch run is the newest run recorded with `rocky run --branch <name>`. The base run is the newest run other than the branch's own. A run made without `--branch` on a git branch with the same name counts as the branch's own. Any other run can be the base, including a run made with another `--branch` name. `preview cost` has no `--base` flag.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--name <NAME>` | `string` | **(required)** | Branch name created by `preview create`. Rocky matches it against the `rocky_branch` recorded on each run. |
| `--models <PATH>` | `PathBuf` | `models` | Models directory. Rocky reads per-model `[budget]` blocks from the sidecars here, so a projected breach can name a single model. |

**Example.**

```bash
rocky preview cost --name pr_preview_fix_price --output json | jq -r .markdown
```

The JSON shape (`PreviewCostOutput`) carries the Markdown report in its `markdown` field. It reports per-model `delta_usd`, `branch_duration_ms`, `base_duration_ms`, and bytes scanned, plus an aggregate `summary.delta_usd`, `summary.savings_from_copy_usd`, and `models_skipped_via_copy`. Underlying cost math is identical to [`rocky cost`](/reference/commands/administration/#rocky-cost) (Databricks / Snowflake duration × DBU rate; BigQuery bytes × $/TB; DuckDB zero). USD fields are left out when the adapter does not surface USD. With no base run, `base_run_id` is left out and `per_model` is empty.

### `rocky preview rows`

Sample the result rows of one transformation model, or of one CTE inside it. Classified columns are masked inline in the output.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--model <NAME>` | `string` | **(required)** | Model to preview. |
| `--cte <NAME>` | `string` | | Preview one named CTE inside the model instead of the model's final output. |
| `--limit <N>` | `u32` | `100` | Maximum number of rows to return. |
| `--allow-warehouse` | `bool` | `false` | Permit execution against a warehouse other than DuckDB. That may cost money, so it is opt-in. A local DuckDB project does not need the flag. |
| `--pipeline <NAME>` | `string` | | Pipeline whose target adapter to run against. Required when the project defines more than one pipeline. |
| `--models <PATH>` | `PathBuf` | `models` | Models directory. |
| `--sql-file <PATH>` | `PathBuf` | | Preview an ad-hoc SQL snippet read from this file instead of the model's compiled SQL. This backs the editor's "Preview Selection". Mutually exclusive with `--cte`. |

`--sql-file` still needs `--model`, which names the enclosing model. If that model has any masked column, Rocky refuses the ad-hoc preview rather than risk leaking a pre-mask value.

**Masking by adapter.** The `hash` and `partial` masks work on Databricks, Snowflake and DuckDB. The code also accepts the `spark` adapter type for both. On every other adapter, such as BigQuery, Rocky has no inline expression for them. `rocky preview rows` then returns the `unmaskable_column` error and no rows. The error names the columns. The `redact` mask works on every adapter.

**Example.** Peek at a model's rows during local development:

```bash
rocky preview rows --model customer_orders --limit 20
```

### Output shapes

`rocky export-schemas` exports the wire contracts as `schemas/preview_create.schema.json`, `preview_diff.schema.json`, `preview_cost.schema.json` and `preview_rows.schema.json`. See [JSON Output](/reference/json-output/).

### Related Commands

- [`rocky ci-diff`](#rocky-ci-diff) -- structural diff alone, without the pruned re-run or row-level sampling
- [`rocky branch`](/reference/commands/core-pipeline/#rocky-branch) -- the schema-prefix branches `preview create` registers
- [`rocky cost`](/reference/commands/administration/#rocky-cost) -- the per-run cost rollup `preview cost` diffs across base and branch
- [`rocky compare`](/reference/cli/#rocky-compare) -- ad-hoc shadow comparison; `preview diff` extends the same kernel with sampled row-level diffing

---

## `rocky publish-ir`

Publish this project's compiled schema so another team can check their models against it. Rocky compiles the project and writes its typed `ProjectIr` as JSON. The consumer vendors that file and points an [`[imports.<name>]`](/reference/configuration/#importsname) block at it. Their `rocky compile` then fails (`E030`) when you drop a column they still read.

```bash
rocky publish-ir [flags]
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--models <PATH>` | `PathBuf` | `models` | Models directory. |
| `--contracts <PATH>` | `PathBuf` | | Contracts directory. |
| `--out <PATH>` | `PathBuf` | `project-ir.json` | Where to write the snapshot. |
| `--with-seed` | `bool` | `false` | Run `data/seed.sql` against an in-memory DuckDB before compiling, so leaf models get concrete column types. |

### Examples

```bash
rocky publish-ir --with-seed --out project-ir.json
```

Pass `--with-seed` for a self-contained DuckDB producer. Without concrete types, the snapshot gives the consumer's contract nothing to check against.

Rocky reads `rocky.toml` (`--config`) to find the warehouse each model runs on, as `rocky compile` and `rocky run` do. A cast whose type differs between warehouses, such as `CAST(x AS FLOAT)`, gets that warehouse's type. The snapshot's recipe hash for a model then equals the one `rocky run` records. A missing `rocky.toml` leaves such casts `Unknown`. An unreadable one stops the command.

### Related Commands

- [`rocky imports`](#rocky-imports) -- the consumer side: advance the vendored baseline
- [Cross-team contracts](/concepts/cross-team-contracts/) -- the full producer and consumer workflow

---

## `rocky imports`

Maintain the producer-contract baselines declared in `[imports.<name>]`. An import is a vendored snapshot of another team's compiled IR. Your `rocky compile` diffs the baseline you accepted against the snapshot you vendored, and fails when the producer made a breaking change.

```bash
rocky imports update           # advance every baseline to its snapshot
rocky imports update --check   # CI guard: report and exit non-zero, write nothing
```

### `rocky imports update`

Advance the vendored baselines to the current snapshot. This is the explicit "I reviewed the producer's current state and accept it" gesture. It also reports any stale pin. It never rewrites `rocky.toml`.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--check` | `bool` | `false` | Read-only CI guard. Report what is out of date, exit non-zero, and write nothing. |

### Where the baseline sits

```
   producer project           consumer project
   ────────────────           ─────────────────────────────────────
   rocky publish-ir           [imports.<name>] in rocky.toml
        │                       baseline = the IR you already accepted
        │ writes an IR          snapshot = the vendored file
        ▼ snapshot              pin      = optional recipe hash
   snapshot file ─ vendored ─►        │
                                      ▼
                                rocky compile
                                  diffs baseline against snapshot,
                                  fails on a breaking change
                                      │
                                      ▼
                                rocky imports update
                                  moves the baseline up to the snapshot
```

### Related Commands

- [`rocky compile`](#rocky-compile) -- the command that reads the baselines and raises the diagnostics
- [Cross-team contracts](/concepts/cross-team-contracts/) -- the full producer and consumer workflow
