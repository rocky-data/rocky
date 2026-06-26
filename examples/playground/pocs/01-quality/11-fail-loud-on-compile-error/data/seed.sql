-- Seed data for the fail-loud-on-compile-error POC.
--
-- run.sh loads this into the persistent poc.duckdb before running the
-- pipeline:  duckdb poc.duckdb < data/seed.sql
--
-- A single raw source table feeds both the clean staging model and the
-- broken time_interval model. All rows land on 2026-04-07 so the partition
-- the run targets is non-empty.

CREATE SCHEMA IF NOT EXISTS raw__sales;

CREATE OR REPLACE TABLE raw__sales.orders AS
SELECT
    i AS order_id,
    CASE WHEN i % 2 = 0 THEN 'us' ELSE 'emea' END AS region,
    (i * 7 % 500) + 5 AS amount,
    CAST(TIMESTAMP '2026-04-07 00:00:00' + ((i % 6) || ' hours')::INTERVAL AS TIMESTAMP) AS order_at
FROM generate_series(1, 30) AS t(i);
