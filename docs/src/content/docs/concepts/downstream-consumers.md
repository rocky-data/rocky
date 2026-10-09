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

Put one TOML file per consumer in a `consumers/` directory at the project root,
next to `rocky.toml`.

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

`rocky compile` reads every consumer file. It reports `E060` for each problem:

- A file that does not parse, or has an unknown `kind` or key.
- Two consumers with the same name. Rocky refuses both, because nothing says which one is meant. `consumer:<name>` selects neither.
- A `depends_on` entry that is not a model in the project.
- A `consumers/` path that exists but cannot be read: it is a file, a link to
  nowhere, or a directory without read permission. Rocky does not treat it as
  an empty project.

```
error[E060]: consumer `weekly_board` depends on `fct_order`, which is not a model in this project
  help: did you mean `fct_orders`?
```

`depends_on` takes model names only. A source table or a seed is not a model.
This check is what catches a dashboard that still reads a model someone removed.

`E060` is an ordinary compile error. `rocky compile`, `rocky ci` and
`rocky test` fail on it, and so do strict compiles. `rocky ci` and `rocky test`
report it as a diagnostic, not as a failed model: it never appears in
`model_results`, and the model tests still run before the command exits with an
error.

`rocky test` fails on `E060` only when it covers the whole project. With
`--model`, `--select` or `--exclude` it still lists the problem in
`diagnostics`, but does not fail on it, so a scoped test is not blocked by a
consumer file it never selected. `rocky ci` always covers the whole project.

`rocky run`, `rocky run --select`, `rocky run --dag`, `rocky plan`,
`rocky propose` and the compile check of `rocky fulfill` do not stop for it. A
consumer is metadata about the readers of your models, so a wrong record cannot
make a model unsafe to write. The run writes the models, does not count the
consumer as a failed table, and lists each problem in the
`consumer_diagnostics` array of its JSON output. Run `rocky compile` or
`rocky ci` first if you want the refusal. `rocky docs`, `rocky emit-sql` and
`rocky publish-ir` also ignore `E060` and work from the models.

Rocky reads `consumers/` from the project root, the directory of `rocky.toml`.
A scoped command reads the same consumers as a whole-project one: `--models
models/marts`, a pipeline `models` glob, and a project with several pipelines
all see them. A `depends_on` entry is valid when it names a model anywhere in
the project, not only a model in the part being compiled. Without a
`rocky.toml`, Rocky looks beside the models directory.

## Where consumers show up

```
  consumers/*.toml
        │
        ├── rocky compile       E060 for a bad record
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
consumer is an error, like a misspelled model name. A consumer that exists but
reads no known model is also an error, and the message says so: its
`depends_on` names nothing that is a model, so there is nothing to select.

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

- `rocky serve --ui` does not show consumers.
- The language server shows `E060` on the consumer file, but does not recompile
  when you edit a file under `consumers/`. Save a model, or restart, to refresh it.
- The MCP `dependents` tool lists models only.
