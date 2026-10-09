---
title: Downstream consumers
description: Record the dashboards, notebooks, ML jobs and applications that read your models, so a change shows who it reaches.
sidebar:
  order: 9.5
---

A dashboard, a notebook, an ML job or an application reads your models. Rocky
does not run it. A **consumer** is a small file that records that it exists, who
owns it, and which models it reads.

With the record in place, Rocky can answer "who reads this model?" It can also
refuse a build when a consumer points at a model that no longer exists.

## Declare a consumer

Put one TOML file per consumer in a `consumers/` directory. The directory sits
beside `models/`, in the same way as `functions/`.

```
my-project/
├── rocky.toml
├── models/
│   └── fct_orders.sql
└── consumers/
    └── weekly_board.toml
```

```toml
# consumers/weekly_board.toml
name = "weekly_board"            # optional; defaults to the file name
kind = "dashboard"
owner = "finance-analytics"
url = "https://bi.example.com/d/weekly-board"
description = "Revenue by region, reviewed every Monday"
depends_on = ["fct_orders", "dim_customers"]
```

| Field | Required | Meaning |
|-------|----------|---------|
| `name` | no | Letters, digits and underscores. Defaults to the file name without `.toml`. |
| `kind` | no | `dashboard`, `notebook`, `ml`, `application`, `analysis` or `other`. Default `other`. |
| `owner` | no | A team, a person or an email. Free text. |
| `url` | no | Where to find the consumer. |
| `description` | no | What the consumer is for. |
| `depends_on` | no | Names of the models it reads. |

An unknown key is an error, so a typo such as `ownr` does not pass silently.

## What Rocky checks

`rocky compile` reads every consumer file. It reports `E059` for each problem:

- A file that does not parse, or has an unknown `kind` or key.
- Two consumers with the same name. Rocky refuses both, because nothing says which one a selector means.
- A `depends_on` entry that is not a model in the project.

```
error[E059] weekly_board: consumer `weekly_board` depends on `fct_order`, which is not a model in this project
  help: did you mean `fct_orders`?
```

`depends_on` takes model names only. A source table or a seed is not a model.
This check is what catches a dashboard that still reads a model someone removed.

`E059` is an ordinary compile error. `rocky ci`, strict compiles and
`rocky run --dag` refuse it in the same way as any other error.

One bad consumer file also stops `rocky docs` from showing column types and
lineage, as any other compile error does. The consumer list itself still renders.

## Where consumers show up

```
  consumers/*.toml
        │
        ├── rocky compile       E059 for a bad record
        ├── rocky lineage       "Consumers" for a model, direct or through downstream models
        ├── rocky docs          Consumers table, "Read by" row on a model page,
        │                       consumers.parquet
        └── --select            consumer:<name> selects the models it reads
```

### Lineage

`rocky lineage <model>` lists the consumers that read the model, or read a model
downstream of it. The JSON output has a `consumers` array. Each entry has
`direct: true` when the consumer reads the model itself. The `downstream` array
still holds models only.

### Selection

`consumer:<name>` selects the models a consumer reads. Add `+` to build
everything the consumer needs:

```bash
rocky run --select +consumer:weekly_board
```

`consumer:` takes globs and the usual graph operators. A name that matches no
consumer is an error, like a misspelled model name.

### Docs

`rocky docs` adds a Consumers table to the overview and a "Read by" row to each
model page. A URL becomes a link only when it starts with `http://` or
`https://`. `--format parquet` writes `consumers.parquet`, one row for each
consumer and model it reads.

## Import from dbt

`rocky import-dbt` turns each dbt exposure into a consumer file. The `type`,
`owner`, `url` and `description` carry over. A dbt `type` that Rocky has no kind
for becomes `other`.

A consumer keeps a dependency only when it names a model that was imported. A
source, a seed or a model that failed to import is not carried over, because the
emitted project must compile. `MIGRATION-NOTES.md` lists what was left out.

## Not covered yet

- `rocky serve --ui` and the language server do not show consumers.
- The MCP `dependents` tool lists models only.
