-- Source schema for the replication pipelines (matches schema_pattern
-- prefix = "raw__"). All three strategies read from raw__orders.orders.

CREATE SCHEMA IF NOT EXISTS raw__orders;

CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT
    CAST(i AS BIGINT)                                   AS order_id,
    CAST(((i - 1) % 20) + 1 AS BIGINT)                  AS customer_id,
    ROUND(10 + (i * 1.7), 2)                            AS amount,
    CASE WHEN i % 7 = 0 THEN 'cancelled'
         WHEN i % 5 = 0 THEN 'pending'
         ELSE 'completed' END                           AS status,
    TIMESTAMP '2026-04-01 09:00:00'
        + (INTERVAL '17 minutes' * (i - 1))             AS ordered_at
FROM generate_series(1, 100) AS t(i);
