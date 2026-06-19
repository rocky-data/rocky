-- Seed data for the surrogate-keys POC.
--
-- `rocky test` auto-loads this file into an in-memory DuckDB before executing
-- models, so the POC is runnable end-to-end with no manual setup.
--
-- For the `rocky run` flow you need to seed the persistent file referenced by
-- `path` in rocky.toml first:
--   duckdb poc.duckdb < data/seed.sql

CREATE SCHEMA IF NOT EXISTS raw__orders;

-- A small orders table keyed by (order_id, customer_id). The surrogate key the
-- model injects hashes exactly those two columns. One row deliberately leaves
-- customer_id NULL to exercise the dbt-utils NULL sentinel coalesce.
CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT * FROM (VALUES
    (1001,  42, 'acme',   'emea',    129.50, DATE '2026-01-03'),
    (1002,  42, 'acme',   'emea',    310.00, DATE '2026-01-05'),
    (1003,  17, 'globex', 'us_west',  58.25, DATE '2026-01-06'),
    (1004,  17, 'globex', 'us_west', 442.10, DATE '2026-01-09'),
    (1005,   8, 'acme',   'emea',     12.00, DATE '2026-01-11'),
    (1006,   8, 'acme',   'us_west', 199.99, DATE '2026-01-12'),
    (1007,  91, 'globex', 'emea',    777.00, DATE '2026-01-15'),
    (1008, NULL, 'globex', 'us_west', 64.40, DATE '2026-01-18')
) AS t(order_id, customer_id, owner, region, amount, order_date);
