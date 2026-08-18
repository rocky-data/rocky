# Watch demo

This example shows two watchers. `rocky watch` recompiles on every save, and one
Ctrl-C stops it. `rocky run --watch` re-runs the whole pipeline on every save.

## The files

```
watch-demo/
  rocky.toml                   # DuckDB adapter, one replication pipeline
  models/
    orders_summary.rocky       # the model to edit while a watcher runs
    orders_summary.toml        # sidecar: name, strategy, target
  seeds/
    raw_orders.csv             # source rows for `rocky run --watch`
    raw_orders.toml            # sidecar: land the seed in schema `main`
```

## `rocky watch` — recompile on save

```bash
cd engine/examples/watch-demo
rocky watch --models models/
```

`rocky watch` never uses the adapter and never connects to a warehouse. It
works on a project that has no adapter configured at all. (The CLI still
reads `rocky.toml` once at startup, to namespace the state store.)

```
     start
       │
       ▼
  ┌──────────┐  prints ✓ or errors  ┌───────────────────┐
  │ compile  │─────────────────────►│ wait for a change │
  │ every    │                      └─────────┬─────────┘
  │ model    │                                │ a .rocky, .sql
  └──────────┘                                │ or .toml file is
       ▲                                      │ saved under --models
       │                                      ▼
       │                            ┌───────────────────┐
       └────────────────────────────│ wait 200 ms, then │
        recompile the whole dir     │ coalesce the      │
                                    │ burst of saves    │
                                    └───────────────────┘
```

Every pass compiles the whole directory. There is no per-model incremental
recompile.

What you see on start, then after one save:

```
[watch] compiling...
  ✓ orders_summary (3 columns)
  Compiled: 1 models, 0 errors, 0 warnings
[watch] compilation succeeded
[watch] waiting for changes...
[watch] file changed: /path/to/watch-demo/models/orders_summary.rocky
[watch] compiling...
  ✓ orders_summary (3 columns)
  Compiled: 1 models, 0 errors, 0 warnings
[watch] compilation succeeded
[watch] waiting for changes...
```

## Try it

1. Start `rocky watch --models models/`.
2. Open `models/orders_summary.rocky` and add a derived column:

```rocky
from raw_orders
group status {
    order_count: count(),
    total_revenue: sum(amount),
    avg_order_value: sum(amount) / count()
}
sort total_revenue desc
```

3. Save. The watcher recompiles and reports four columns instead of three.
4. Delete the group's closing brace `}` and the `sort` line under it, then save
   again. Delete the brace alone and the parser reaches `sort` instead, which
   gives a different message with a byte offset in it. The watcher reports the
   error and keeps waiting:

```
[watch] compilation failed: failed to parse .rocky file 'models/orders_summary.rocky': unexpected end of file: expected identifier
```

5. Press Ctrl-C. The watcher prints `[watch] stopped` and exits with status 0.

## See the generated SQL

`rocky watch` prints compile results, not SQL. Use `rocky emit-sql` for that:

```bash
rocky emit-sql --models models/
```

For the model as shipped, that prints:

```sql
-- model: orders_summary
CREATE OR REPLACE TABLE warehouse.main.orders_summary AS
SELECT status, COUNT() AS order_count, SUM(amount) AS total_revenue
FROM raw_orders
GROUP BY status
ORDER BY total_revenue DESC;
```

Add `--out-dir <dir>` to write one `.sql` file per model instead of printing.

## `rocky run --watch` — re-run on save

This one does connect to the warehouse, so the source table has to exist
first. Load it once from the shipped seed:

```bash
rocky seed --seeds seeds/
rocky run --watch --models models/
```

The seed puts `raw_orders` into `warehouse.main`, where the model's bare
`FROM raw_orders` resolves. The first run creates `warehouse.duckdb` next to
`rocky.toml`; git ignores it, and deleting it starts over.

The watcher watches `rocky.toml` and the models directory, coalesces saves
over the same 200 ms window, and re-runs the entire pipeline each time. It
reloads `rocky.toml` on every pass, so an edit to the config takes effect on
the next run. One save produces this:

```
[watch] watching /path/to/watch-demo/rocky.toml, /path/to/watch-demo/models (Ctrl-C to stop)
[watch] run completed in 118ms
[watch] detected change: /path/to/watch-demo/models/orders_summary.rocky
[watch] run completed in 104ms

[watch] stopped
```

Skip the seed and every pass fails instead:
`Table with name raw_orders does not exist!`. The loop keeps waiting either
way — a failing run is reported, not fatal. `rocky watch` needs no seed and
no warehouse, and works as shipped.

Banner lines go to stderr. With `--output json`, each pass writes one compact
`RunOutput` object on its own line to stdout, so you can pipe the stream into
`jq`.

A Ctrl-C that lands between runs stops the watcher at once. A Ctrl-C that
lands during a run reaches the run first, and the watcher stops once that run
reports the interrupt back to it.

`--watch` is rejected at parse time alongside `--dag`, `--resume`,
`--resume-latest`, `--idempotency-key`, `--model`, and `--assume-fresh-state`.
