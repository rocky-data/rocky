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

A marker works in either position. Inside a string literal the operator owns
the quoting (`region = '@var(region)'`); bare in numeric or expression position
it renders without quotes (`amount >= @var(min_amount, 0)`). Substitution runs
before the SQL is parsed for lineage and type checking, so a bare marker never
trips the parser.

## Layout

```
rocky.toml                      Transformation pipeline over models/**
models/
  regional_sales.sql/.toml      Filters on @var(region) (required, quoted),
                                @var(channel, web) (optional, quoted) and
                                @var(min_amount, 0) (optional, BARE numeric)
data/seed.sql                   raw__sales.sales across three regions x two channels
run.sh                          Runs with --var, emits resolved SQL, and shows
                                the missing-variable compile error
```

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

==== 4. a BARE @var(min_amount, 0) in numeric position works on the run path ====
rows with region=us AND amount >= 200: 1 (bob)
OK: a bare numeric @var filtered the run — no string-literal wrapper needed

==== 5. emit-sql --var region=us — markers resolve to literal values ====
WHERE region  = 'us'
  AND channel = 'web'
  AND amount >= 0
OK: quoted @var(region) -> 'us' and bare @var(min_amount, 0) -> amount >= 0; no marker remains

==== 6. the optional @var(channel, web) resolved to its inline DEFAULT ====
OK: @var(channel, web) resolved to 'web' with no --var supplied

==== 7. a REQUIRED @var with no --var is a compile error (E028) ====
  [E028] regional_sales: model references run variable `@var(region)` but no value was supplied and it has no inline default
  suggestion: pass `--var region=<value>` or give the reference an inline default `@var(region, <default>)`
OK: missing required @var(region) failed compile with E028 naming the variable

PASS: @var() run variables resolve at run time ...
```
