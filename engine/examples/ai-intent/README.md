# AI Intent

Two models that carry an `intent` field in their `.toml` sidecar, plus four
example test files in `tests/`.

## What `intent` is

`intent` is a plain-English sentence in a model's sidecar. It states what the
model should produce. The `rocky ai-*` commands send it to the Anthropic API
alongside the compiled schema, so the request carries real column names and
types.

`stg_orders.toml` declares:

```toml
name = "stg_orders"
intent = "Clean raw order data for downstream consumption. Exclude cancelled orders and orders with non-positive amounts. One row per order. order_id is the unique grain."
```

## Files

```
ai-intent/
  rocky.toml
  models/
    stg_orders.rocky           + stg_orders.toml         # has intent
    fct_daily_revenue.rocky    + fct_daily_revenue.toml  # has intent
  tests/                                                 # example fixtures
    test_stg_orders_not_null.sql
    test_stg_orders_no_cancelled.sql
    test_fct_daily_revenue_positive.sql
    test_fct_daily_revenue_unique.sql
```

## Prerequisite

`rocky ai`, `rocky ai-test`, `rocky ai-explain`, and `rocky ai-sync` all call
the Anthropic API. Set `ANTHROPIC_API_KEY` first. Without it each one exits
with this error:

```
Error: ANTHROPIC_API_KEY not set. Set it to use `rocky ai`.

Caused by:
    environment variable not found
```

Run these from the repository root:

```bash
export ANTHROPIC_API_KEY=...
cd engine/examples/ai-intent
```

`rocky compile` needs no key and no warehouse:

```bash
rocky compile
```

## Generate tests from intent

`rocky ai-test` takes a model name as a positional argument, or `--all` for
every model. It prints the assertions. Add `--save` to write them to disk.

```bash
rocky ai-test stg_orders
rocky ai-test --all --save
```

Running `rocky ai-test` with neither a model name nor `--all` prints
`Specify a model name or use --all.` and generates nothing.

`--save` writes one `.sql` file per assertion into a `tests/` directory beside
the models directory. It names each file `<model>_<assertion>.sql` and
lower-cases the assertion name. A saved file therefore always starts with the
model name, `stg_orders_` or `fct_daily_revenue_`. The four files already in
`tests/` start with `test_`, so `--save` adds files beside them. It replaces
none of them.

## Explain a model

`rocky ai-explain` also takes the model name as a positional argument. There
is no `--model` flag.

```bash
rocky ai-explain stg_orders
rocky ai-explain --all --save
```

`--save` writes the generated sentence back into the model's `.toml` sidecar
as its `intent`.

### `--all` means two different things

`--all` does not select the same models on both commands. Read the help text
before you use it:

| Command | What `--all` selects |
|---|---|
| `rocky ai-test --all` | every model |
| `rocky ai-explain --all` | only the models that declare no `intent` |

Both models in this example declare `intent`, so `rocky ai-explain --all`
selects nothing. It prints this, writes no file, and exits `0`:

```
No models to explain. Specify a model name or use --all.
```

Adding `--save` changes nothing, because there is no explanation to save.
Name a model instead to explain one that already has an `intent`.

## Propose updates after a schema change

```bash
rocky ai-sync --with-intent
```

`--with-intent` limits the report to models that declare an `intent`. The
command is a dry run by default. Add `--apply` to write the proposed changes.

## Generate a new model

```bash
rocky ai "Monthly active customers who placed at least one order"
```

The intent is a positional argument. `--models` supplies the schema context
and the destination directory. `--materialization`, `--watermark`,
`--unique-key`, and `--target` fill in the generated sidecar. An existing file
at the destination stops the command unless you pass `--overwrite`.

## What the generated tests look like

`rocky ai-test --save` writes two comment lines, then the assertion SQL:

```
-- Test: <assertion name>
-- <assertion description>
<assertion SQL>
```

Rocky checks each assertion name before it builds the path. The name may hold
only letters, digits, `_`, and `-`. Anything else, such as a space, a dot, or
a slash, stops the save with an error. That guard keeps a generated name from
writing outside `tests/`.

## The four files in `tests/`

The four files are example fixtures. They ship with this example so you can
read the shape of an assertion without an API key. Each one is a query that
returns zero rows when the assertion holds. `tests/test_stg_orders_not_null.sql`
holds:

```sql
-- AI-generated test assertion
-- Intent: order_id is the unique grain and must never be null
-- Returns 0 rows when assertion holds

SELECT *
FROM warehouse.staging.stg_orders
WHERE order_id IS NULL
```

Their header is not the header `rocky ai-test --save` writes, so treat them as
fixtures rather than as a record of a past run.

No Rocky command reads a file in `tests/` back. `rocky ai-test --save` only
writes. Run the queries yourself, or paste them into your own test job. For
assertions Rocky executes, put `[[tests]]` blocks in the model sidecar and run
`rocky test --declarative`. See `engine/examples/test-declarative`.

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
rocky --config engine/examples/ai-intent/rocky.toml compile \
  --models engine/examples/ai-intent/models/
```
