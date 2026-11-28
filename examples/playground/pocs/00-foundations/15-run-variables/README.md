# Per-run variables with `@var()`

## Feature

`@var(name)` markers in model SQL are resolved per run from `rocky run --var name=value` (also accepted by `rocky compile` and `rocky emit-sql`). A marker may carry an inline default, `@var(name, default)`. A required marker with no value and no default fails the compile with **E028**, naming the variable and suggesting the fix.

This differs from `${ENV}`, which is resolved while `rocky.toml` is parsed and feeds connection and config values. `@var()` is resolved at compile/render time, inside model SQL.

Inside a string literal you own the quoting (`region = '@var(region)'`). In numeric or expression position the marker renders without quotes (`amount >= @var(min_amount, 0)`). Substitution runs before SQL parsing, so a bare marker never trips the parser.

The model `regional_sales` filters on `region` (required), `channel` (default `web`) and `min_amount` (default `0`).

## Run

```bash
cd examples/playground/pocs/00-foundations/15-run-variables
./run.sh
```

No credentials required (DuckDB only). `run.sh` needs `rocky`, the `duckdb` CLI and `jq` on `PATH`. `rocky emit-sql` writes rendered SQL to `build/`.

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
