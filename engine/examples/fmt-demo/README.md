# Fmt demo

This example shows `rocky fmt`, the formatter for `.rocky` files. The command
takes file and directory paths, so it needs no `rocky.toml`.

## The two files

```
fmt-demo/
  models/
    messy_model.rocky    # indentation is wrong; rocky fmt rewrites it
    clean_model.rocky    # already formatted; rocky fmt leaves it alone
```

Both files hold the same pipeline. `clean_model.rocky` is hand-spaced, so it
shows the house style, not the formatter's output.

## What the formatter changes

`rocky fmt` applies four rules.

1. It trims trailing whitespace from every line.
2. It re-indents each line by brace depth, four spaces per level. Pipeline
   keywords such as `from`, `where`, and `select` sit at column 0.
3. It collapses three or more blank lines into two.
4. It ends the file with exactly one newline.

It does not change spacing inside a line. `amount>100` stays `amount>100`, and
`where   status` keeps its three spaces.

## Before and after

`models/messy_model.rocky` before:

```rocky
-- This file is deliberately poorly formatted to demonstrate rocky fmt
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

The same file after `rocky fmt models/`:

```rocky
-- This file is deliberately poorly formatted to demonstrate rocky fmt
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

Every line now starts at the right depth. The spacing inside each line is
untouched.

## Run it

Check without writing. Rocky names each file it would change and exits 1:

```bash
cd engine/examples/fmt-demo
rocky fmt --check models/
```

```
would reformat: models/messy_model.rocky
Error: 1 file(s) would be reformatted
```

Rewrite the files in place:

```bash
rocky fmt models/
```

```
reformatted: models/messy_model.rocky
```

Format one file:

```bash
rocky fmt models/messy_model.rocky
```

A directory path is searched recursively. Rocky picks up every `.rocky` file
below it and ignores every other extension. With no path at all, `rocky fmt`
searches the current directory.

## Use it as a CI gate

`--check` never writes. It exits 1 when any file needs formatting, so it works
as a build step or a pre-commit hook:

```yaml
- name: Check Rocky formatting
  run: rocky fmt --check models/
```

`--check` reports which files differ. It does not print the diff. Run
`rocky fmt` locally and read the result with `git diff`.
