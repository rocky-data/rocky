# Compare demo

This example shows `rocky compare`. The command pairs each production table
with its shadow copy and reports whether they match — row counts and schema.
Use it to test a pipeline change safely: run the changed pipeline into shadow
tables, compare, and only promote when the pairs agree.

`rocky compare` pairs tables from replication source discovery, so this
example is a replication pipeline. There are no model files here.

## The files

```
compare-demo/
  rocky.toml        # DuckDB adapter, one replication pipeline
  seeds/
    orders.csv      # 6 source rows
    orders.toml     # sidecar: land the seed in schema raw__shopify
```

## The flow

```
rocky seed        raw__shopify.orders            (source, from the CSV)
      │
rocky run         staging__shopify.orders        (production)
      │
rocky run --shadow --shadow-suffix _shadow
                  staging__shopify.orders_shadow (shadow copy)
      │
rocky compare --shadow-suffix _shadow
                  pairs the two, reports pass or fail
```

The seed sidecar targets schema `raw__shopify`. The pipeline's
`schema_pattern` (`prefix = "raw__"`) makes discovery read that schema as
source `shopify`, and `schema_template = "staging__{source}"` routes its
tables to `staging__shopify`.

## Run it

```bash
cd engine/examples/compare-demo
rocky seed --seeds seeds/
rocky run
rocky run --shadow --shadow-suffix _shadow
rocky compare --shadow-suffix _shadow
```

The compare reports one pair, matching:

```json
{
  "tables_compared": 1,
  "tables_passed": 1,
  "results": [
    {
      "production_table": "warehouse.staging__shopify.orders",
      "shadow_table": "warehouse.staging__shopify.orders_shadow",
      "row_count_match": true,
      "production_count": 6,
      "shadow_count": 6,
      "schema_match": true,
      "verdict": "pass"
    }
  ],
  "overall_verdict": "pass"
}
```

The first run creates `warehouse.duckdb` next to `rocky.toml` — a DuckDB
file's catalog is its file stem, so the `warehouse` catalog exists. Git
ignores the file; delete it to start over.

## See it catch a difference

Make the source and the shadow disagree, then compare again:

```bash
echo '7,acme,300.00,paid,2026-01-11' >> seeds/orders.csv
rocky seed --seeds seeds/                       # source now has 7 rows
rocky run --shadow --shadow-suffix _shadow      # shadow picks them up
rocky compare --shadow-suffix _shadow           # production still has 6
```

The command exits 1 and the pair fails:

```
production_count: 6, shadow_count: 7, verdict: fail
overall_verdict: fail
```

That is the promotion gate: the shadow run saw data the production run has
not, so the change is not safe to promote as-is. Re-run `rocky run` to bring
production up to date and the compare passes again. (Afterwards, delete the
added CSV line to restore the example.)

## Flags

- `--shadow-suffix <s>` must match the suffix the shadow run used.
- `--filter key=value` narrows the comparison to matching sources.
- `--output` is a global flag and goes before the subcommand:
  `rocky --output table compare --shadow-suffix _shadow` prints a
  human-readable summary instead of the JSON shown above.
