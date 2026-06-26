# Per-run variables with `@var()`

## Feature

`@var(name)` markers in a model's SQL are resolved at run time from
`rocky run --var name=value` (also accepted by `rocky compile` and
`rocky emit-sql`). A marker may carry an inline default — `@var(name, default)`
— and a required marker with no value and no default is a compile error.

## Why it's distinctive

`@var()` is the run-time half of Rocky's two-layer parameterisation, and it is
deliberately distinct from the config-time `${ENV}` interpolation:

- `${ENV}` is resolved while `rocky.toml` is parsed. It feeds connection and
  config values into the engine before any model is seen.
- `@var()` is resolved at compile/render time, per run, inside model SQL. It is
  how one run differs from the next: which region to load, which cutoff date to
  use.

The marker stays visible in the source, so the SQL reads honestly and the
operator owns any quoting (`where region = '@var(region)'`). A required
variable that nobody supplied does not silently render to nothing. It fails the
compile with **E028**, naming the variable and suggesting the fix.

## Layout

```
rocky.toml                      Transformation pipeline over models/**
models/
  regional_sales.sql/.toml      Filters on @var(region) (required) and
                                @var(channel, web) (optional, inline default)
data/seed.sql                   raw__sales.sales across three regions x two channels
run.sh                          Runs with --var, emits resolved SQL, and shows
                                the missing-variable compile error
```

Each marker sits inside a string literal (`'@var(region)'`), which is how
operator-owned quoting works and also keeps the pre-substitution SQL parseable.

## Run

```bash
cd examples/playground/pocs/00-foundations/15-run-variables
./run.sh
```

No credentials required — DuckDB only.

## Expected output

```
==== 2. rocky run --var region=us — materialize ONLY us rows (channel defaults to web) ====
OK: only us rows landed, channel defaulted to web (2 rows)

==== 3. repeated --var: region=us AND channel=mobile selects a different slice ====
OK: overriding the optional channel var changed the materialized slice

==== 4. emit-sql --var region=us — markers resolve to literal values ====
WHERE region  = 'us'
  AND channel = 'web'
OK: @var(region) resolved to 'us'; no @var() marker remains in the executable SQL

==== 5. the optional @var(channel, web) resolved to its inline DEFAULT ====
OK: @var(channel, web) resolved to 'web' with no --var supplied

==== 6. a REQUIRED @var with no --var is a compile error (E028) ====
  [E028] regional_sales: model references run variable `@var(region)` but no value was supplied and it has no inline default
  suggestion: pass `--var region=<value>` or give the reference an inline default `@var(region, <default>)`
OK: missing required @var(region) failed compile with E028 naming the variable

PASS: @var() run variables resolve at run time ...
```
