-- Seed data for the dag_mode POC.
-- Creates a small orders table that the replication pipeline copies
-- and the transformation models aggregate.

CREATE SCHEMA IF NOT EXISTS raw__orders;
CREATE SCHEMA IF NOT EXISTS poc.staging;
CREATE SCHEMA IF NOT EXISTS poc.gold;

CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT
    i AS order_id,
    1 + (i % 30) AS customer_id,
    ROUND(CAST(10.0 + random() * 490.0 AS DECIMAL(10,2)), 2) AS amount,
    CASE WHEN random() < 0.05 THEN 'cancelled' ELSE 'completed' END AS status,
    CAST(TIMESTAMP '2025-06-01' + INTERVAL (i * 3600) SECOND AS DATE) AS order_date,
    TIMESTAMP '2026-01-01' + INTERVAL (i * 60) SECOND AS _updated_at
FROM generate_series(1, 200) AS t(i);
