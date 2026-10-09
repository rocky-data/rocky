---
title: Node selection
description: Syntax and semantics of --select, --exclude, and --state-ref for choosing which models a command works on
sidebar:
  order: 6
---

Choose a subset of transformation models with `--select` and `--exclude`. The syntax follows dbt's [node selection syntax](https://docs.getdbt.com/reference/node-selection/syntax), so a dbt selector usually works unchanged.

## Commands that accept `--select`

Every flag on this page is optional. Without `--select` or `--exclude`, a command works on every model, as before.

| Command | What the selection does |
|---|---|
| `rocky list` / `rocky list models` | Lists only the selected models. This is the Rocky form of `dbt ls`. |
| `rocky compile` | Compiles the whole project, then reports and fails on the selected models only. |
| `rocky test` | Runs every model in DuckDB, then reports the selected models only. |
| `rocky emit-sql` | Emits SQL for the selected models only. |
| `rocky run` | Builds the selected models only. Replication is skipped. |
| `rocky plan` | Plans the selected model. The selection must resolve to exactly one model. |
| `rocky docs` | Documents the selected models only. |

`rocky test --declarative` and `rocky lineage` do not take `--select` yet.

## Flags

| Flag | Meaning |
|---|---|
| `--select <SELECTOR>...`, `-s` | Models to include. Repeat the flag or pass several values. |
| `--exclude <SELECTOR>...` | Models to remove from the selection. Without `--select`, Rocky removes them from every model. |
| `--state-ref <REF>` | Git ref that `state:` methods compare against. Default: `main`. |
| `--state-working-tree` | `state:` methods compare the working tree (staged, unstaged and untracked files) with the merge base of `--state-ref`, as `rocky ci-diff --working-tree` does. Not on `rocky plan`. |

## Selector syntax

A selector is a model name, a method, or either of those with graph operators.

```text
--select "stg_orders tag:finance+"   space   = union
--select "+fct_revenue,path:marts"   comma   = intersection
--exclude "tag:deprecated"           exclude = subtract, after graph operators
```

```
 values ──► split on spaces ──► each term ──► split on commas ──► atoms
                (union)                         (intersection)
                                                      │
              ┌───────────────────────────────────────┘
              ▼
   method match ──► graph operators ──► term result ──► union ──► minus --exclude
```

### Names and globs

A bare value selects models by name. `*` matches any run of characters and `?` matches one character.

| Selector | Selects |
|---|---|
| `stg_orders` | The model named `stg_orders`. |
| `stg_*` | Every model whose name starts with `stg_`. |
| `models/staging` | A bare value with `/` selects by path, as `path:models/staging` does. |
| `stg_orders.sql` | A bare file name that ends in `.sql` or `.rocky` selects by file name, as `file:stg_orders.sql` does. |

### Graph operators

Graph operators add a model's upstream or downstream models. See dbt's [graph operators](https://docs.getdbt.com/reference/node-selection/graph-operators).

| Selector | Selects |
|---|---|
| `+m` | `m` and every model upstream of it. |
| `m+` | `m` and every model downstream of it. |
| `+m+` | `m`, its upstream models, and its downstream models. |
| `2+m` | `m` and its upstream models up to 2 steps away. |
| `m+3` | `m` and its downstream models up to 3 steps away. |
| `@m` | `m`, every model downstream of it, and every upstream model of those models. |

Rocky takes the edges from the compiled DAG. That covers sidecar `depends_on` and the model references Rocky reads from the SQL.

### Methods

A method has the form `method:value`. The value accepts globs. See dbt's [node selection methods](https://docs.getdbt.com/reference/node-selection/methods).

| Method | Selects models that |
|---|---|
| `name:<name>` | have this name. `fqn:` is an alias. |
| `tag:<value>` | have a `[tags]` key or value equal to `<value>`. |
| `tag:<key>=<value>` | have the `[tags]` pair `key = "value"`. |
| `path:<dir or file>` | live in this directory, or are this file. The path is relative to the project root (`models/staging`) or to the models directory (`staging`). |
| `file:<name>` | have this file name, with or without the extension. |
| `config.materialized:<strategy>` | use this strategy, such as `view`, `incremental`, or `merge`. `table` matches `full_refresh`. `config.strategy` and `config.type` are aliases. |
| `config.schema:<schema>` | write to this target schema. |
| `config.catalog:<catalog>` | write to this target catalog. `config.database` is an alias. |
| `config.table:<table>` | write to this target table. `config.alias` is an alias. |
| `source:<relation>` | read this external table. `source:raw` matches `raw.orders`. `source:raw.orders` matches `raw.orders` and `warehouse.raw.orders`. |
| `selector:<name>` | match the saved selector `<name>`. See [Saved selectors](#saved-selectors). |
| `state:modified` | changed since `--state-ref`. New models are included, as in dbt. |
| `state:new` | do not exist at `--state-ref`. |

Rocky tags are the key-value `[tags]` block in the model sidecar:

```toml
# models/marts/fct_revenue.toml
[tags]
domain = "finance"
tier = "gold"
```

Both `tag:finance` and `tag:domain=finance` select this model. `tag:tier` selects it too, because `tier` is a key.

### How `state:` finds changed models

`state:modified` and `state:new` use the change detection of `rocky ci-diff`. Rocky runs `git diff <ref>...HEAD` and maps the changed files to models. Only committed changes count. Commit your work before you select on state, or pass `--state-working-tree`. When a model has uncommitted edits that the committed diff leaves out, Rocky logs a warning that names it.

```sh
rocky list --select state:modified+ --state-ref origin/main
```

## Saved selectors

Name an expression you use often in the `[selectors]` table of `rocky.toml`:

```toml
[selectors]
nightly = "tag:nightly+ config.materialized:incremental"
finance = "path:marts/finance,tag:certified"
finance_nightly = "selector:finance,selector:nightly"
```

Then pass `selector:<name>` to `--select` or `--exclude` on any command that takes them:

```bash
rocky run --select selector:nightly
rocky list models --select "+selector:finance" --exclude selector:nightly
```

A saved selector uses the same syntax as `--select`. It may hold graph operators, `state:` and other `selector:` terms. Graph operators outside it apply to its result (`+selector:finance`). Space-separated terms inside it union, and the comma intersects, with the saved expression kept as one unit. An unknown name, an empty expression, or saved selectors that refer to each other in a loop is an error.

## Rules and errors

These rules decide what happens at the edges of the syntax.

- An unknown method, such as `owner:x`, is an error. The message lists the supported methods.
- `test_type:` is not supported. Rocky tests are not separate nodes in the DAG.
- `@` with `+` (such as `@+m`) is an error, as in dbt.
- A `--select` term that names one model, tag, path, file or source with no glob, and matches nothing, is an error. Every selecting command exits non-zero and does nothing. The term is almost always a typo.
- A glob (`stg_*`), a `config.` term, or a `state:` term that matches no model logs a warning: `The selection criterion 'x' does not match any enabled nodes`. An `--exclude` term that matches nothing also only warns.
- A selection that matches no model logs `Nothing to do`. `list`, `compile`, `test`, `emit-sql`, and `run` then exit 0 with nothing done. With `--output json`, `rocky run` prints an empty, successful run result. `plan` and `docs` refuse, because they cannot write an empty plan or catalog.

## How `--select` works with `--model`

`--model <name>` is the older single-model flag. It keeps working unchanged.

- `--model <name>` alone behaves exactly as before. A name that does not exist is still an error.
- `--model <name>` with `--exclude` is the same as `--select name:<name>` with that exclude. The name must still exist.
- `--model` with `--select` is an error. Put the model in the selector instead.

## How `rocky run --select` builds models

`rocky run --select` builds only the selected models, in dependency order. A selected model reads its unselected upstream models as they already exist in the warehouse. This is dbt's behavior.

```
  select: fct_revenue           build fct_revenue
            │                        │
            ▼                        ▼ reads the existing table
  stg_orders (not selected) ──► main.stg_orders   (not rebuilt)
```

Add `--defer` to read unselected upstreams from their production schema instead. Add `--defer-to <schema>` to read them from one schema.

A selection that resolves to one model runs exactly like `--model <name>`. `rocky run --select` cannot be combined with `--dag`, `--watch`, `--all`, `--filter`, `--contracts`, `--resume`, or `--resume-latest`.

## Examples

```sh
# List staging models and everything downstream of them
rocky list --select "path:models/staging+" --output json

# Compile the finance models, but not the deprecated ones
rocky compile --select tag:finance --exclude tag:deprecated

# Build one mart and its upstream models
rocky run --select +fct_revenue

# Build only what changed on this branch, plus its downstream models
rocky run --select state:modified+ --state-ref origin/main

# Emit SQL for views that read the raw schema
rocky emit-sql --select "source:raw,config.materialized:view"
```

## Related pages

- [CLI Filters](/reference/filters/) — `--filter` chooses replication sources, not models.
- [Migrate from dbt](/guides/migrate-from-dbt/) — the dbt-to-Rocky mapping.
