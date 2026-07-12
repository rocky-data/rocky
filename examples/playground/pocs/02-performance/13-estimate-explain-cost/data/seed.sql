CREATE SCHEMA IF NOT EXISTS raw__sales;
CREATE SCHEMA IF NOT EXISTS demo;

-- 1000 order lines so the planner's row/byte estimate is non-trivial.
CREATE OR REPLACE TABLE raw__sales.orders AS
SELECT
    i                                                      AS order_id,
    1 + (i % 200)                                          AS customer_id,
    ROUND(CAST(5.0 + random() * 495.0 AS DECIMAL(10,2)), 2) AS amount,
    CASE WHEN i % 13 = 0 THEN 'cancelled' ELSE 'completed' END AS status
FROM generate_series(1, 1000) AS t(i);

CREATE OR REPLACE TABLE raw__sales.customers AS
SELECT
    i                          AS customer_id,
    'customer_' || i           AS name,
    (i % 5)                    AS tier
FROM generate_series(1, 200) AS t(i);
