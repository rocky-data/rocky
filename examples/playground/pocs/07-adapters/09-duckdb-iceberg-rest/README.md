# 09-duckdb-iceberg-rest — Iceberg tables through DuckDB and a REST catalog

> **Category:** 07-adapters
> **Credentials:** none (Docker daemon required for the live run)
> **Runtime:** about 30s on a warm Docker pull; the offline part runs in under 2s
> **Rocky features:** `[[adapter.<name>.attach]]` on a DuckDB adapter, Iceberg full refresh as one in-place transaction, append incremental

## What it shows

A `type = "duckdb"` adapter attaches a [Lakekeeper](https://docs.lakekeeper.io/)
Iceberg REST catalog as `lk`. Every model whose target catalog is `lk` is
written as an Iceberg table. The data files land in a local SeaweedFS S3
bucket. There is no vendor warehouse.

```
rocky run ──▶ DuckDB (in-memory) ──ATTACH (TYPE iceberg)──▶ Lakekeeper :8181
                    │                                            │
                    └────────── Parquet data files ──▶ SeaweedFS S3 :8333
```

## Why it's distinctive

DuckDB-Iceberg has no `CREATE OR REPLACE TABLE` and no rename inside a
write transaction. So Rocky rewrites an existing table in place, as one
Iceberg commit:

```
BEGIN TRANSACTION
  CREATE OR REPLACE TEMP TABLE temp.main.__rocky_overwrite AS <model SQL>
  ALTER TABLE … ADD / DROP / ALTER COLUMN … TYPE   (only when the model's columns changed)
  DELETE FROM lk.demo.orders
  INSERT INTO lk.demo.orders BY NAME SELECT * FROM temp.main.__rocky_overwrite
COMMIT                       any failure → ROLLBACK, the old table and rows stay
```

A re-run with no model change runs no `ALTER`. A change Iceberg cannot make
in place (a narrowing, a nested type change, a new nested column, dropping a
partition column) is
refused and the table is left as it was.

`rocky emit-sql` shows the same transaction without
connecting: the attachments in `rocky.toml` tell the dialect which catalogs
are Iceberg.

## Layout

```
rocky.toml           DuckDB adapter + [[adapter.attach]] (Iceberg REST, S3 secret)
docker-compose.yml   Postgres, Lakekeeper (migrate + serve), SeaweedFS; pinned by digest
setup.sh             creates the S3 bucket and the Lakekeeper warehouse `demo`
models/
  orders              full_refresh, 100 generated rows
  customer_totals     full_refresh, reads lk.demo.orders
  order_log           append incremental on ordered_at
```

## Run

```bash
./run.sh
```

## Expected output

- `rocky validate`, `rocky compile` and `rocky emit-sql` pass with no network.
- Run 1 creates `lk.demo.orders`, `lk.demo.customer_totals` and `lk.demo.order_log`.
- Run 2 overwrites the two full-refresh tables in place and appends to `order_log`.
- The Iceberg REST API lists the three tables in namespace `demo`.

## Scope (v1)

Supported on an attached Iceberg catalog: `full_refresh` and append
`incremental`. Refused before any model runs, with a message naming the
strategy and the catalog: views, `merge`, `delete_insert`, `time_interval`,
`microbatch`, snapshots, quarantine, branch promote, seeds and `rocky load`.
Catalog authentication other than `authorization = "none"` comes later.
See [`docs/adr/ADR-ICEBERG-DUCKDB.md`](../../../../../docs/adr/ADR-ICEBERG-DUCKDB.md).
