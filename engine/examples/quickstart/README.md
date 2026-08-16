# Quickstart

A three-model pipeline that shows the core Rocky workflow. One model is plain
SQL. Two models use the Rocky DSL. All three compile into one typed graph.

## The model graph

```
  raw_orders            stg_orders             fct_revenue
  (.sql)         ──►    (.rocky)        ──►    (.rocky)
  reads                 drops cancelled        groups by order_date
  source.raw.orders     adds computed cols     sums revenue
```

- **raw_orders** reads `source.raw.orders` and selects nine columns.
- **stg_orders** drops cancelled orders and adds `order_amount_usd` and
  `is_completed`.
- **fct_revenue** groups completed orders by `order_date` and sums revenue.

## Files

```
quickstart/
  rocky.toml               # DuckDB adapter, local state store
  models/
    raw_orders.sql         # Plain SQL
    raw_orders.toml        # Sidecar: name, strategy, target
    stg_orders.rocky       # Rocky DSL
    stg_orders.toml
    fct_revenue.rocky      # Rocky DSL
    fct_revenue.toml
```

A model is a body file plus a `.toml` sidecar. The body is either `.sql` or
`.rocky`. The sidecar sets the model name, the materialization strategy, and
the target table.

## Run the example

Run these from the repository root. Rocky reads `rocky.toml` from the working
directory by default, so the `cd` is what lets the rest omit `--config`.

```bash
cd engine/examples/quickstart
rocky compile
```

`rocky compile` type-checks every model and reports the column count:

```
  ✓ raw_orders (9 columns)
  ✓ stg_orders (6 columns)
  ✓ fct_revenue (4 columns)
  Compiled: 3 models, 0 errors, 0 warnings
```

Print the SQL each model would run:

```bash
rocky emit-sql
```

Print the SQL for one model:

```bash
rocky emit-sql --model stg_orders
```

```sql
-- model: stg_orders
CREATE OR REPLACE TABLE warehouse.staging.stg_orders AS
SELECT order_id, customer_id, order_date, status, amount AS order_amount_usd, status = 'completed' AS is_completed
FROM raw_orders
WHERE status IS DISTINCT FROM 'cancelled';
```

`rocky compile` and `rocky emit-sql` need no warehouse connection.

## Build a plan

```bash
rocky plan
```

`rocky plan` executes no SQL. It writes a plan file to `.rocky/plans/` and
prints the plan id:

```
Run plan persisted — 3 model(s) across 3 layer(s)
Plan ID:   170efd38be46b340994a6cce423a194b5aa812b9812fdd3d4b3ab4e62bdc4ef4
Apply with: rocky apply 170efd38be46b340994a6cce423a194b5aa812b9812fdd3d4b3ab4e62bdc4ef4
```

`rocky apply <id>` executes a stored plan. A plan built by bare `rocky plan`
carries the replication stage, so applying it reports `Copied 0 tables` and
exits `0`. A plan built by `rocky plan --models models/` carries the models,
and applying that one hits the catalog error described in the next section.

## What `rocky run` does here

`rocky run` on its own executes the replication stage, not the models. This
pipeline is `type = "replication"`. Its source pattern matches no schema in a
fresh DuckDB database, so the command reports `Copied 0 tables` and exits `0`.

```bash
rocky run              # Copied 0 tables — the models are untouched
rocky run --models models/   # executes the models, and fails here
```

`rocky run --models models/` fails on `raw_orders` and exits `1`. The last
line it prints is:

```
Error: 1 table(s) failed during parallel execution (run_id: run-20260816-155426-513, use --resume run-20260816-155426-513 to retry)
```

The run id changes on every run, so yours will differ. Rocky logs the cause
first, as a `WARN table error` line. Its `error` field holds:

```
model 'raw_orders' failed: DuckDB error: Catalog Error: Catalog with name warehouse does not exist!: Catalog Error: Catalog with name warehouse does not exist!: Error code 1: Unknown error code
```

The sidecars target `warehouse.staging.raw_orders`, and a DuckDB session has
no catalog called `warehouse`. Setting `auto_create_catalogs = true` in
`rocky.toml` produces the same error.

To execute this graph, retarget the sidecars at a catalog your warehouse has,
and point `raw_orders.sql` at a table that exists. `engine/examples/seed-demo`
shows the loading half with `rocky seed`.

## Where `--config` goes

`--config` is a top-level flag, not a per-command flag. It comes before the
subcommand, never after:

```bash
rocky --config rocky.toml compile   # works
rocky compile --config rocky.toml   # error: unexpected argument '--config' found
```

The commands above omit it, because `--config` already defaults to
`rocky.toml` in the working directory.

To run from somewhere else, pass `--models` as well. It defaults to `models`
relative to the working directory, not relative to the config:

```bash
rocky --config engine/examples/quickstart/rocky.toml compile \
  --models engine/examples/quickstart/models/
```

## What to notice

- `.sql` and `.rocky` models live in the same project and depend on each
  other. Rocky compiles both into one graph.
- A `.rocky` file reads top to bottom. Each step feeds the next.
- `!=` in the Rocky DSL compiles to `IS DISTINCT FROM`, shown in the emitted
  SQL above. `NULL IS DISTINCT FROM 'cancelled'` is true, so rows with a NULL
  `status` survive the filter. SQL's `!=` drops them.
