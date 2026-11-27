# 09-duckdb-iceberg-rest — Iceberg tables through DuckDB and a REST catalog

> **Credentials:** none (Docker daemon required for the live run)
> **Runtime:** about 30s on a warm Docker pull; offline part under 2s

## What it shows

A DuckDB adapter attaches a [Lakekeeper](https://docs.lakekeeper.io/) Iceberg REST catalog as `lk`. Models targeting `lk` become Iceberg tables with data in a local SeaweedFS S3 bucket. `docker-compose.yml` runs the stack and `setup.sh` creates the bucket and the warehouse `demo`.

DuckDB-Iceberg has no `CREATE OR REPLACE TABLE` and no rename inside a write transaction, so Rocky rewrites an existing table in place, as one Iceberg commit:

```
BEGIN TRANSACTION
  CREATE OR REPLACE TEMP TABLE temp.main.__rocky_overwrite AS <model SQL>
  ALTER TABLE … ADD / DROP / ALTER COLUMN … TYPE   (only when the model's columns changed)
  DELETE FROM lk.demo.orders
  INSERT INTO lk.demo.orders BY NAME SELECT * FROM temp.main.__rocky_overwrite
COMMIT                       any failure → ROLLBACK, the old table and rows stay
```

A change Iceberg cannot make in place (a narrowing, a nested type change, a new nested column, dropping a partition column) is refused and the table is left as it was.

`rocky emit-sql` shows the same transaction without connecting.

## Scope (v1)

Supported on an attached Iceberg catalog: `full_refresh` and append `incremental`. Refused before any model runs, naming the strategy and catalog: views, `merge`, `delete_insert`, `time_interval`, `microbatch`, snapshots, quarantine, branch promote, seeds and `rocky load`. Catalog authentication other than `authorization = "none"` comes later. See [`docs/adr/ADR-ICEBERG-DUCKDB.md`](../../../../../docs/adr/ADR-ICEBERG-DUCKDB.md).

## Run

```bash
./run.sh
```

Without a reachable Docker daemon (the live run uses `docker compose`), only the offline steps run.

## Expected output

- `rocky validate`, `rocky compile` and `rocky emit-sql` pass with no network.
- Run 1 creates `lk.demo.orders`, `lk.demo.customer_totals` and `lk.demo.order_log`.
- Run 2 overwrites the two full-refresh tables in place and appends to `order_log`.
- The Iceberg REST API lists the three tables in namespace `demo`.
