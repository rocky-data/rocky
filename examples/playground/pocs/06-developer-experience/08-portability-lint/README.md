# 08-portability-lint — compile-time portability gate

> **Category:** 06-developer-experience
> **Credentials:** none (no warehouse needed, lint is AST-based)
> **Runtime:** < 2s
> **Rocky features:** `--target-dialect`, P001 `NonPortableConstruct`, `[portability]` block, `-- rocky-allow` pragma

## What it shows

`rocky compile --target-dialect bq` walks each model's parsed SQL (`sqlparser-rs` AST) and emits error-severity **P001** diagnostics for constructs the target warehouse doesn't support (NVL, QUALIFY, DATEADD, etc., 20 known constructs). `NVL` inside a string literal does not trip it.

Three escape hatches:

1. **Rewrite** — `NVL(x, y)` → `COALESCE(x, y)`. The diagnostic carries a `suggestion` string.
2. **Per-file pragma** — `-- rocky-allow: NVL` at the top of a `.sql` file suppresses that construct in that file.
3. **Project-wide allowlist** — `[portability] allow = ["QUALIFY"]` in `rocky.toml`.

Limitation: the diagnostic's span is file-level (`line: 1`); per-construct byte offsets aren't tracked yet.

## Run

```bash
./run.sh
```

## What to expect

1. **Baseline compile** (no dialect flag): no portability diagnostic, because `rocky.toml` declares `[portability] target_dialect = "snowflake"`, where NVL is native. Without that line, `rocky compile` refuses NVL with `E057`, since the project's adapter is DuckDB and DuckDB has no NVL. Declaring the dialect tells Rocky not to judge function names against the DuckDB adapter.
2. **`rocky compile --target-dialect bq`** overrides the configured dialect and fires exactly one P001, on `non_portable_nvl.sql`. `portable.sql` is clean; `suppressed_via_pragma.sql` is suppressed.
3. The diagnostic carries `severity: Error`, a file:line span, the model name and a `suggestion` (`replace NVL(...) with IFNULL(...) or COALESCE(...)`).

## Related

- Engine source: `engine/crates/rocky-sql/src/portability.rs`, `engine/crates/rocky-sql/src/pragma.rs`
- CLI surface: `rocky compile --target-dialect {dbx,sf,bq,duckdb}`
- Companion POC (P002 `SELECT *` blast-radius lint): [`09-sql-types-blast-radius/`](../09-sql-types-blast-radius/)
