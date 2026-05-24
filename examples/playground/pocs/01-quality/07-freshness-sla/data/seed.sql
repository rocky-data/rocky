-- Source tables for the staging models. `--with-seed` runs this in an in-memory
-- DuckDB and reads column types from information_schema, so the leaf models get
-- concrete types (in particular the TIMESTAMP columns that drive W005) instead
-- of degrading to Unknown.
CREATE SCHEMA IF NOT EXISTS seeds;

CREATE OR REPLACE TABLE seeds.orders AS
SELECT
    CAST(1 AS BIGINT)                        AS order_id,
    CAST(101 AS BIGINT)                      AS customer_id,
    TIMESTAMP '2026-05-01 10:15:00'          AS order_ts,
    CAST(49.50 AS DECIMAL(10, 2))            AS amount;

CREATE OR REPLACE TABLE seeds.shipments AS
SELECT
    CAST(1 AS BIGINT)               AS shipment_id,
    CAST(1 AS BIGINT)               AS order_id,
    TIMESTAMP '2026-05-01 18:00:00' AS shipped_at;

CREATE OR REPLACE TABLE seeds.customers AS
SELECT
    CAST(101 AS BIGINT)    AS customer_id,
    CAST('Acme' AS VARCHAR) AS name;
