-- Source table for the snapshot example. Run it once:
--   duckdb warehouse.duckdb < seed.sql
CREATE SCHEMA IF NOT EXISTS raw;
CREATE OR REPLACE TABLE raw.customers AS
SELECT * FROM (VALUES
    (1, 'Ada',   TIMESTAMP '2026-01-01 00:00:00'),
    (2, 'Grace', TIMESTAMP '2026-01-01 00:00:00'),
    (3, 'Linus', TIMESTAMP '2026-01-01 00:00:00')
) AS t(customer_id, name, updated_at);
