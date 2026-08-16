# Compare demo

This example shows shadow mode and `rocky compare`. Shadow mode writes a run's
output to renamed copies of the target tables. `rocky compare` then checks each
shadow table against the production table it shadows.

## The two models

`models/` holds two Rocky DSL models, each with a TOML sidecar.

- `stg_orders` drops cancelled orders and derives `order_amount_usd` and
  `is_completed`.
- `fct_daily_orders` aggregates completed orders by `order_date`.

The sidecar `fct_daily_orders.toml` declares `depends_on = ["stg_orders"]`.

## How shadow mode and compare fit together

```
                ┌─────────────────────┐  writes   ┌──────────────────────┐
 rocky run ────►│ --shadow            │──────────►│ <table><suffix>      │
                │ --shadow-suffix     │           └──────────┬───────────┘
                └─────────────────────┘                      │
                                                             │ compares
                                                             │ row count
                                                             │ + schema
                ┌─────────────────────┐  reads    ┌──────────▼───────────┐
 rocky compare ►│ --shadow-suffix     │──────────►│ <table>  (production)│
                └─────────────────────┘           └──────────────────────┘
```

`--shadow-schema` replaces the suffix instead of adding one. The table keeps its
name and moves to the schema you name.

## Type-check the models

Run this from the example directory. It needs no warehouse data.

```bash
cd engine/examples/compare-demo
rocky compile --models models/
```

Rocky prints:

```
  ✓ stg_orders (6 columns)
  ✓ fct_daily_orders (4 columns)
  Compiled: 2 models, 0 errors, 0 warnings
```

## Run the two shadow steps

```bash
rocky run --shadow --shadow-suffix _shadow
rocky compare --shadow-suffix _shadow
```

`--shadow-suffix` defaults to `_rocky_shadow` on both commands. Leave it off if
that name suits you. Pass the same value to both commands when you do set it.

## What compare reports

`rocky compare` opens both tables in a pair, the shadow one and the production
one. It reads two things from them.

- **Row count.** It reports both counts, the difference, and the percentage
  difference.
- **Schema.** It reports each column that differs between the two tables.

It does not diff rows. `--thresholds` takes JSON and overrides three settings:
`row_count_diff_pct_warn` (default `0.01`), `row_count_diff_pct_fail` (default
`0.05`), and `allow_column_order_diff` (default `true`).

## What this example cannot execute

`rocky.toml` declares a DuckDB adapter with no `path`, so Rocky opens an
in-memory database. That database has no `raw_orders` table and no catalog named
`warehouse`. Two consequences follow.

The pipeline is a replication pipeline, so `rocky run --shadow` discovers its
tables from the source. It finds none and copies nothing:

```
Copied 0 tables in 0.1s (run_id: run-20260816-144334-637)
```

`rocky compare` enumerates the same discovered tables, so it compares nothing:

```
  Rocky Compare

  Tables: 0 compared, 0 passed, 0 warned, 0 failed
  Overall: PASS
```

To see real table pairs, give the adapter a catalog that exists, then create
schemas named `raw__<source>` inside it. `raw__` is the pipeline's
`schema_pattern` prefix. Discovery lists schemas by that prefix and returns the
tables inside each one, so a table called `raw_orders` never enters the
comparison.

The two models are a separate matter. `rocky run --shadow --models models/`
compiles them and writes `stg_orders_shadow` and `fct_daily_orders_shadow`.
`rocky compare` still ignores them: this pipeline is a replication pipeline, so
compare takes its targets from source discovery. Only a transformation pipeline
reads compare targets off its models.

## Flags

| Flag | Command | Description |
|------|---------|-------------|
| `--shadow` | `run` | Write to shadow targets instead of production |
| `--shadow-suffix <s>` | `run`, `compare` | Suffix appended to the table name (default `_rocky_shadow`) |
| `--shadow-schema <s>` | `run`, `compare` | Write shadow tables to this schema instead of adding a suffix |
| `--models <dir>` | `run` | Models directory for transformation execution |
| `--all` | `run` | Execute both replication and compiled models |
| `--thresholds <json>` | `compare` | Comparison thresholds, for example `'{"row_count_diff_pct_fail": 0.05}'` |
| `--pipeline <name>` | `run`, `compare` | Select a pipeline when the config declares more than one |
| `--filter <key>=<value>` | `run`, `compare` | Restrict the run to sources whose parsed component matches |
| `--output json` | `run`, `compare` | Emit `RunOutput` / `CompareOutput` instead of the text report |

`rocky run --branch <name>` is the branch equivalent. It behaves like
`--shadow --shadow-schema <prefix>`, and it conflicts with `--shadow` and
`--shadow-schema`.
