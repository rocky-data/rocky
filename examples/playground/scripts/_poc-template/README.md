# <POC ID> — <Human Name>

> **Category:** <00-foundations | 01-quality | ...>
> **Credentials:** none (DuckDB) | ANTHROPIC_API_KEY | Databricks | Snowflake | Fivetran
> **Runtime:** < 30s
> **Rocky features:** <comma-separated list>

## What it shows

<One paragraph: what feature this POC demonstrates and what it produces.>

## Why it's distinctive

- <One bullet: what dbt can't do here, or what no other POC covers>
- <Optional second bullet>

## Layout

```
.
├── README.md         this file
├── rocky.toml        pipeline config
├── run.sh            end-to-end demo
├── models/           SQL / .rocky models + .toml sidecars
├── contracts/        (if applicable)
└── seeds/            sample data (≤1000 rows)
```

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI for seeding (`brew install duckdb`)
- <credentials, if any>

## Run

```bash
./run.sh
```

## Expected output

```text
<excerpted JSON or stdout — keep < 30 lines>
```

## What happened

1. <Step 1 — what Rocky did>
2. <Step 2>
3. ...

## Related

- Rocky docs: [feature page](https://rocky-data.dev/...)
- Source: `rocky/crates/...`
- Companion example (if any): [`rocky/examples/...`](https://github.com/rocky-data/rocky/tree/main/examples/...)
