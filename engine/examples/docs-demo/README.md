# Docs demo

This example shows `rocky docs`. The command reads your models and their TOML
sidecars, then writes one self-contained HTML page. The page has no external
CSS, no external JavaScript, and no server.

## The four models

`models/` mixes both model formats. Rocky reads plain SQL files and Rocky DSL
files from the same directory.

```
raw_customers ──► stg_customers ──┐
                                  ├──► fct_customer_orders
raw_orders ───────────────────────┘
```

- `raw_customers` and `raw_orders` are `.sql` files, each with a `.toml`
  sidecar.
- `stg_customers` is a `.rocky` file. It keeps active customers and derives a
  full name and a tenure.
- `fct_customer_orders` is a `.rocky` file. It aggregates orders per customer.

## Generate the page

```bash
cd engine/examples/docs-demo
rocky docs --models models/
```

Rocky prints:

```
Documentation generated: docs/catalog.html (4 models, 2 ms)
```

Open `docs/catalog.html` in a browser.

Pass `--output-path <file>` to write somewhere else. The default is
`docs/catalog.html`. Rocky creates the parent directory if it is missing.

## What the page contains

A banner at the top counts models, pipelines, and adapters. Below it sits one
filterable table. Each row is one model, with these columns: Model, Target,
Strategy, Deps, and Tests. Deps and Tests are counts.

Click a model name to expand a panel below it. The panel shows the model's
`intent` text, then three sections: Columns, Dependencies, and Tests.

The search box filters rows by model name. It does not search descriptions,
targets, or column names.

## Why the Columns section is empty here

`rocky docs` builds the page from `rocky.toml` and the model files. It opens no
warehouse adapter, so it has no column types to show. Every model in this
example therefore reports `No column metadata available`.

`rocky.toml` is not optional. The command reads it before it reads a model, and
it counts the configured pipelines and adapters for the banner. Run `rocky docs`
in a directory that holds `models/` but no config and it stops with exit 1:

```
  × No rocky.toml found at 'rocky.toml'
  help: Run `rocky init` to create a new project, or use `--config <path>` to
        specify a config file.
        Run `rocky playground` to try Rocky with a sample DuckDB project.
```

## Sidecar fields the page reads

| Field | What the page does with it |
|-------|----------------------------|
| `name` | Names the row and the expandable panel |
| `intent` | Renders as the model's description |
| `depends_on` | Fills the Deps count and the Dependencies list |
| `[strategy]` | Renders as the Strategy badge, for example `full_refresh` |
| `[target]` | Renders as the Target column, as `catalog.schema.table` |
| `[[tests]]` | Fills the Tests count and the Tests list, with each test's severity |

A sidecar may also carry a `[columns]` table of per-column descriptions. Rocky
parses that table, but `rocky docs` does not render it today. Each description
hangs off a column entry, and the page has no column entries to hang one on. No
flag and no earlier command changes that.
