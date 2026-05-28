-- Seed data for quickstart playground.
--
-- `rocky test` auto-loads this file into an in-memory DuckDB before
-- executing models, so the playground is runnable end-to-end with no
-- manual setup.
--
-- For the `rocky discover/plan/run` flow you need to seed the persistent
-- file referenced by `path` in rocky.toml first:
--   duckdb playground.duckdb < data/seed.sql

CREATE SCHEMA IF NOT EXISTS raw__orders;

CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT
    i AS order_id,
    1 + (i % 50) AS customer_id,
    1 + (i % 20) AS product_id,
    ROUND(CAST(5.0 + random() * 495.0 AS DECIMAL(10,2)), 2) AS amount,
    CASE WHEN random() < 0.05 THEN 'cancelled'
         WHEN random() < 0.10 THEN 'pending'
         ELSE 'completed' END AS status,
    CAST(TIMESTAMP '2025-06-01' + INTERVAL (i * 3600) SECOND AS DATE) AS order_date,
    TIMESTAMP '2026-01-01' + INTERVAL (i * 60) SECOND AS _updated_at
FROM generate_series(1, 500) AS t(i);
