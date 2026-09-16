# Multi-Layer Pipeline

A bronze, silver, gold pipeline with a data contract on the gold model. It
shows how Rocky resolves a layered graph and how a contract gates the compile.

## The layer graph

```
  BRONZE                 SILVER                     GOLD
  models/bronze/         models/silver/             models/gold/

  raw_events.sql  ──►    stg_events.rocky   ──┐
  reads                  drops NULL keys      ├──►  fct_user_activity.rocky
  source.raw.events      adds event flags     │     joins, groups by user_id
                                              │           │
                         stg_users.rocky   ───┘           ▼
                         reads source.raw.users     contracts/
                         drops NULL keys            fct_user_activity.contract.toml
```

Bronze copies the source columns with no transformation. Silver removes rows
with NULL keys and adds computed columns. Gold joins the two silver models and
aggregates per user.

## Files

```
multi-layer/
  rocky.toml
  models/
    bronze/raw_events.sql          + raw_events.toml
    silver/stg_events.rocky        + stg_events.toml
    silver/stg_users.rocky         + stg_users.toml
    gold/fct_user_activity.rocky   + fct_user_activity.toml
  contracts/
    fct_user_activity.contract.toml
```

Rocky walks `models/` recursively, so the layer directories are for humans.
The graph comes from the model bodies, not from the folder names.

## Compile the graph

Run these from the repository root. Rocky reads `rocky.toml` from the working
directory by default, so the `cd` is what lets the rest omit `--config`.

```bash
cd engine/examples/multi-layer
rocky compile
```

```
  ✓ raw_events (9 columns)
  ✓ stg_events (10 columns)
  ✓ stg_users (6 columns)
  ✓ fct_user_activity (12 columns)
  Compiled: 4 models, 0 errors, 0 warnings
```

## The contract gate

`contracts/fct_user_activity.contract.toml` declares nine columns with a type
and a nullability flag, plus two rules:

- `required` lists `user_id`, `total_events`, `first_event_date`, and
  `last_event_date`. Each must appear in the model's output columns.
- `protected` lists `user_id`. It must also appear in the output, so dropping
  it from the model is a violation.

The compiler raises a different error code for each contract check:

| Check | Code | Raised when |
|---|---|---|
| `required` column present | `E010` | a `required` column is missing from the model output |
| `protected` column present | `E013` | a `protected` column is missing from the model output |
| declared `type` matches | `E011` | the inferred type differs from the declared one |
| declared `nullable` matches | `E012` | the column is nullable and the contract declares `nullable = false` |
| no new nullable column | `E014` | `[rules] no_new_nullable = true` and the model outputs a nullable column the contract does not declare |

`E014` is opt-in. This contract does not set `no_new_nullable`, so it never
fires here.

The type check does not guess. When Rocky cannot infer a column's type, it
does not compare types. It reports `I003` instead, at info severity, and the
compile does not fail on it. So `E011` means the two types disagree, never that
Rocky could not tell.

A declared column that the model does not output is a warning, not an error,
unless `required` lists it. The code is `W010` and the message is
`contract column '<name>' not found in model output`. This contract declares
nine columns, and all nine are in the model output, so no `W010` appears here.

The plain `rocky compile` line above reports 12 columns for
`fct_user_activity`, not nine. The
`keep` block in the join outputs `full_name`, `email`, and `country`, and the
`group` block outputs them again, so each name appears twice.

A contract is loaded only when a command receives `--contracts <dir>`. The
commands that accept the flag are `compile`, `test`, `ci`, `dag`, `publish-ir`,
`serve`, and `watch`. `rocky plan` and `rocky run` have no `--contracts` flag,
so neither one checks a contract.

```bash
rocky compile --contracts contracts/
```

## This example fails its own contract

The contract marks four columns `nullable = false`. The compiler infers all
four as nullable in `fct_user_activity`. The compile therefore reports `E012`
four times and exits `1`.

The example ships no source schemas, so Rocky cannot infer any of the nine
declared types. It also reports `I003` once per declared column, nine times in
all. Those lines are info, not errors:

```
  ✓ raw_events (9 columns)
  ✓ stg_events (10 columns)
  ✓ stg_users (6 columns)
  ✗ fct_user_activity
  x info[I003]: column 'user_id' declares type Int64 in the contract, but Rocky could not work out the column's type, so it did not check the declared type
  help: give `rocky compile` source schemas so `user_id`'s type resolves — ...

  x error[E012]: column 'user_id' must be non-nullable per contract, but is nullable
  help: filter out NULLs (e.g. `WHERE user_id IS NOT NULL`) or COALESCE `user_id` to a default, or relax `nullable = true` in the contract

  (I003 repeats for the other eight declared columns; E012 repeats for
   total_events, first_event_date, last_event_date)

  Compiled: 4 models, 4 errors, 0 warnings
Error: compilation failed with errors
```

That is the gate doing its job. Follow either branch of the `help` line to
clear it: add a NULL filter or a `COALESCE` in `fct_user_activity.rocky`, or
set `nullable = true` for those columns in the contract.

## What `rocky run` does here

`rocky run` on its own executes the replication stage, not the models. Its
source pattern matches no schema in a fresh DuckDB database, so it reports
`Copied 0 tables` and exits `0`.

`rocky run --models models/` executes the models and fails. The sidecars
target `warehouse.bronze`, `warehouse.silver`, and `warehouse.gold`, and a
DuckDB session has no catalog called `warehouse`. The models also read
`source.raw.events` and `source.raw.users`, which this example does not ship.

`rocky compile` and `rocky plan` need neither a catalog nor a source table.

## Where `--config` goes

`--config` is a top-level flag, not a per-command flag. It comes before the
subcommand, never after:

```bash
rocky --config rocky.toml compile   # works
rocky compile --config rocky.toml   # error: unexpected argument '--config' found
```

The commands above omit it, because `--config` already defaults to
`rocky.toml` in the working directory.

To run from somewhere else, pass `--models` and `--contracts` as well. Both
resolve against the working directory, not against the config:

```bash
rocky --config engine/examples/multi-layer/rocky.toml compile \
  --models engine/examples/multi-layer/models/ \
  --contracts engine/examples/multi-layer/contracts/
```
