CREATE SCHEMA IF NOT EXISTS raw__orders;

CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT
    i AS order_id,
    1 + (i % 30) AS customer_id,
    CAST(10.0 + i AS DECIMAL(10,2)) AS amount,
    TIMESTAMP '2026-04-01' + INTERVAL (i * 60) SECOND AS _updated_at
FROM generate_series(1, 100) AS t(i);
