# Watch Demo

Demonstrates `rocky watch` -- a file watcher that auto-recompiles `.rocky` models whenever you save a change. Useful during development to get instant feedback on syntax errors and generated SQL.

## Project Structure

```
watch-demo/
  rocky.toml                   # DuckDB backend, local state
  models/
    orders_summary.rocky       # Model to edit while watching
    orders_summary.toml        # Sidecar config
```

## Running

Start the watcher in a terminal:

```bash
rocky --config engine/examples/watch-demo/rocky.toml watch \
  --models engine/examples/watch-demo/models/
```

The watcher prints compiled SQL for each model on startup, then monitors for changes.

## Try It

1. Start the watcher (command above)
2. Open `models/orders_summary.rocky` in your editor
3. Change the aggregation -- for example, add a new derived column:

```rocky
from raw_orders
group status {
    order_count: count(),
    total_revenue: sum(amount),
    avg_order_value: sum(amount) / count()
}
sort total_revenue desc
```

4. Save the file -- the watcher detects the change and recompiles immediately
5. Introduce a syntax error (e.g., remove a closing brace) to see diagnostic output
6. Press `Ctrl+C` to stop

## What to Observe

- On startup, Rocky compiles all models in the directory and prints generated SQL
- On each save, only the changed model is recompiled (incremental)
- Syntax and type errors appear inline with file path and line number
- The watcher uses OS-native file notifications (inotify/kqueue) for near-instant response
