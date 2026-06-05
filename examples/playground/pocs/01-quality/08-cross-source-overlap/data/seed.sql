-- Seed for the cross-source-overlap POC.
--
-- Two regional Shopify connectors land an `orders` table each. The schema
-- pattern (prefix `raw__`, components region + source) parses them as
-- region=us/eu, source=shopify — siblings that replicate to two target
-- schemas but share the same table name.
--
-- For the discover/run flow, load this into the persistent DuckDB first:
--   duckdb poc.duckdb < data/seed.sql

CREATE SCHEMA IF NOT EXISTS raw__us__shopify;
CREATE SCHEMA IF NOT EXISTS raw__eu__shopify;

-- US region. order_ids 1001–1006; one order per (customer, day).
CREATE OR REPLACE TABLE raw__us__shopify.orders (
    order_id    INTEGER,
    customer_id INTEGER,
    order_date  DATE,
    amount      DECIMAL(10, 2)
);
INSERT INTO raw__us__shopify.orders VALUES
    (1001, 1, DATE '2026-01-01', 50.00),
    (1002, 2, DATE '2026-01-02', 60.00),
    (1003, 3, DATE '2026-01-03', 70.00),
    (1004, 4, DATE '2026-01-04', 80.00),   -- also onboarded under EU
    (1005, 5, DATE '2026-01-05', 90.00),   -- also onboarded under EU
    (1006, 6, DATE '2026-01-06', 100.00);  -- also onboarded under EU

-- EU region. order_ids 1004–1009 — the first three collide with US (the same
-- orders onboarded under both connectors). Order 1009 is a derived-key dup:
-- same customer + day as 1008 under a fresh surrogate order_id.
CREATE OR REPLACE TABLE raw__eu__shopify.orders (
    order_id    INTEGER,
    customer_id INTEGER,
    order_date  DATE,
    amount      DECIMAL(10, 2)
);
INSERT INTO raw__eu__shopify.orders VALUES
    (1004, 4, DATE '2026-01-04', 80.00),   -- shared order_id with US
    (1005, 5, DATE '2026-01-05', 90.00),   -- shared order_id with US
    (1006, 6, DATE '2026-01-06', 100.00),  -- shared order_id with US
    (1007, 7, DATE '2026-01-07', 110.00),
    (1008, 8, DATE '2026-01-08', 120.00),
    (1009, 8, DATE '2026-01-08', 125.00);  -- same (customer 8, 2026-01-08) as 1008
