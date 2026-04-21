# 08-portability-lint — Trust arc 6: compile-time portability gate

> **Category:** 06-developer-experience
> **Credentials:** none (no warehouse needed — lint is AST-based)
> **Runtime:** < 2s
> **Rocky features:** `--target-dialect`, P001 `NonPortableConstruct`, `[portability]` block, `-- rocky-allow` pragma

## What it shows

Compile-time rejection of dialect-divergent SQL. `rocky compile
--target-dialect bq` parses every model with `sqlparser-rs`, walks the
AST, and emits error-severity **P001** diagnostics for any construct
that the target warehouse doesn't support (NVL, QUALIFY, DATEADD, etc —
20 known constructs).

Three escape hatches:

1. **Rewrite** — `NVL(x, y)` → `COALESCE(x, y)`. The diagnostic carries
   a concrete `suggestion` string.
2. **Per-file pragma** — `-- rocky-allow: NVL` at the top of a `.sql`
   file suppresses that construct just in that file. Use when you
   intentionally run only on the non-portable dialect.
3. **Project-wide allowlist** — `[portability] allow = ["QUALIFY"]` in
   `rocky.toml` suppresses across all models.

## Why it's distinctive

- **AST-based, not regex.** `NVL(x, y)` inside a string literal doesn't
  trip the lint; a real function-call does. The same walker can point
  at the construct's exact byte range for editor squiggles.
- **Polyglot correctness at compile time.** dbt packages you hope
  travel; Rocky rejects the non-portable construct at compile-time
  with a targeted suggestion — before the warehouse sees it.

## Layout

```
.
├── README.md                       this file
├── rocky.toml                      [portability] target + QUALIFY allowlist
├── run.sh                          compile with & without --target-dialect
└── models/
    ├── _defaults.toml              catalog=poc, schema=demo
    ├── portable.sql                COALESCE only — portable under every dialect
    ├── non_portable_nvl.sql        NVL — fires P001 under --target-dialect=bq
    └── suppressed_via_pragma.sql   same NVL, suppressed via -- rocky-allow: NVL
```

## Run

```bash
./run.sh
```

## What happened

1. **Baseline compile** — no dialect flag, no portability diagnostics;
   every model type-checks.
2. **`rocky compile --target-dialect bq`** — fires exactly one P001 on
   `non_portable_nvl.sql`. `portable.sql` is clean (COALESCE works
   everywhere); `suppressed_via_pragma.sql` is suppressed per-file.
3. The diagnostic carries `severity: Error`, a file:line span, the
   offending model name, and a human-readable `suggestion` (`replace
   NVL(...) with IFNULL(...) or COALESCE(...)`).

## Related

- Engine source: `engine/crates/rocky-compiler/src/portability.rs`,
  `engine/crates/rocky-sql/src/pragma.rs`
- CLI surface: `rocky compile --target-dialect {dbx,sf,bq,duckdb}`
- Companion arc: Arc 7 blast-radius lint (P002 `SELECT *`) — see
  [`09-sql-types-blast-radius/`](../09-sql-types-blast-radius/)
