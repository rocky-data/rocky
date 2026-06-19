-- Seed data for the emit-sql exit-path POC.
--
-- `rocky test` auto-loads this file into an in-memory DuckDB. The exit-path
-- replay in run.sh loads it into the persistent `poc.duckdb` (the same file
-- the emitted SQL writes into) before replaying the emitted `<model>.sql`
-- files:
--   duckdb poc.duckdb < data/seed.sql

CREATE SCHEMA IF NOT EXISTS raw__sales;

CREATE OR REPLACE TABLE raw__sales.orders AS
SELECT
    CASE WHEN i % 2 = 0 THEN 'acme' ELSE 'globex' END AS tenant,
    i AS order_id,
    1 + (i % 40) AS customer_id,
    CASE WHEN i % 3 = 0 THEN 'emea' ELSE 'us_west' END AS region,
    ROUND(CAST(5.0 + (i * 7 % 490) AS DECIMAL(10,2)), 2) AS amount,
    CASE WHEN i % 19 = 0 THEN 'cancelled'
         WHEN i % 11 = 0 THEN 'pending'
         ELSE 'completed' END AS status,
    CAST(DATE '2025-06-01' + CAST(i % 90 AS INTEGER) AS DATE) AS order_date
FROM generate_series(1, 300) AS t(i);
