# Docs demo

This example shows `rocky docs`. The command reads your models and their TOML
sidecars, then writes a static documentation site. The site has no external
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

## Generate the site

```bash
cd engine/examples/docs-demo
rocky docs --models models/
```

Rocky writes a directory and prints one summary line, in this shape:

```
Documentation generated: docs/site (site, 4 models, ... files, ... ms)
```

Open `docs/site/index.html` in a browser.

```
docs/site/
  index.html        # overview: counts, models table, sources
  lineage.html      # interactive lineage graph
  models/<name>.html
  sources/<name>.html
  assets/           # site.css, site.js, data.js
```

Pass `--output-path <dir>` to write somewhere else. The default is
`docs/site`. To get one self-contained page instead, pass an `--output-path`
that ends in `.html`, such as `docs/catalog.html`. `--format parquet` writes
the project graph as Parquet tables.

## What the site contains

The overview page counts models, sources, pipelines, adapters, and tests.
Below the counts sits one filterable table. Each row is one model, with these
columns: Model, Description, Strategy, Columns, Tests, Contract, and
Freshness. The filter box matches model names and descriptions.

Click a model name to open its page. The page shows these sections: Details
(target, file, owner), Columns, Tests, Contract, Lineage, and the SQL.

Press `/` to search from any page. The search matches model names, column
names, and descriptions. `lineage.html` draws the whole graph and traces a
single column.

## Where the Columns section comes from

`rocky docs` compiles the project offline, the same way `rocky compile` does.
The Columns section lists each column the compiler infers, with its type and
whether it is nullable. The command opens no warehouse adapter. In this example
no source schemas are cached, so every type shows as `?` and every column as
nullable.

If every model loads but the project does not compile, the page still
renders. Rocky logs a warning. The overview says the project did not compile,
and each model shows `No column metadata available.` A model that cannot be PARSED is different:
the loader is strict, so a malformed `.sql`, `.rocky` or sidecar `.toml` fails
`rocky docs` before it renders anything.

`rocky.toml` is not optional. The command reads it before it reads a model, and
it counts the configured pipelines and adapters for the overview page. Run
`rocky docs` in a directory that holds `models/` but no config and it stops with exit 1:

```
  × No rocky.toml found at 'rocky.toml'
  help: Run `rocky init` to create a new project, or use `--config <path>` to
        specify a config file.
        Run `rocky playground` to try Rocky with a sample DuckDB project.
```

## Sidecar fields the page reads

| Field | What the page does with it |
|-------|----------------------------|
| `name` | Names the row and the model page |
| `intent` | Renders as the model's description in the overview table |
| `depends_on` | Feeds the Lineage section and the lineage graph |
| `[strategy]` | Renders as the Strategy badge, for example `full_refresh` |
| `[target]` | Renders as the Target row, as `catalog.schema.table` |
| `[[tests]]` | Fills the Tests count and the Tests section, with each test's severity |

A sidecar may also carry a `[columns]` table of per-column descriptions. The
models in this example have none. Add one and `rocky docs` renders the
description under the column name in the Columns section:

```toml
[columns.customer_id]
description = "Stable customer key from the source system."
```

The column must be one the compiler infers for that model. Matching ignores
ASCII case. If a description names a column the compiler does not see, Rocky
logs a warning and does not render that description.
