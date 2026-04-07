# Fmt Demo

Demonstrates `rocky fmt` -- an opinionated formatter for `.rocky` files, similar to `rustfmt` or `prettier`. No `rocky.toml` is needed because formatting operates on files directly.

## Project Structure

```
fmt-demo/
  models/
    messy_model.rocky    # Deliberately poorly formatted
    clean_model.rocky    # Already well-formatted (no-op)
```

## Before / After

**Before** (`messy_model.rocky`):

```rocky
from    raw_orders
  where   status   !=    "cancelled"
derive    {
      order_amount_usd:     amount,
  is_high_value:amount>100,
        days_since_order: current_date  -   order_date
  }
  select   {
order_id,
      customer_id,
  order_amount_usd,
            is_high_value,
    days_since_order
          }
    sort    order_amount_usd    desc
```

**After** (formatted):

```rocky
from raw_orders
where status != "cancelled"
derive {
    order_amount_usd: amount,
    is_high_value: amount > 100,
    days_since_order: current_date - order_date
}
select {
    order_id,
    customer_id,
    order_amount_usd,
    is_high_value,
    days_since_order
}
sort order_amount_usd desc
```

## Running

```bash
# Check formatting without modifying files (exits non-zero if changes needed)
rocky fmt --check engine/examples/fmt-demo/models/

# Format files in place
rocky fmt engine/examples/fmt-demo/models/

# Format a single file
rocky fmt engine/examples/fmt-demo/models/messy_model.rocky
```

## CI Usage

Use `rocky fmt --check` in CI pipelines to enforce consistent formatting:

```yaml
- name: Check Rocky formatting
  run: rocky fmt --check models/
```

The `--check` flag prints a diff of what would change and exits with code 1 if any file needs formatting -- useful as a pre-commit hook or CI gate.
