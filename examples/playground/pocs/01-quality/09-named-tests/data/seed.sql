CREATE SCHEMA IF NOT EXISTS seeds;

-- Orders: amount > 0 and status in {pending, shipped, delivered}.
CREATE OR REPLACE TABLE seeds.orders AS
SELECT
    CAST(i AS BIGINT)                                            AS order_id,
    CAST(1 + (i % 12) AS BIGINT)                                AS customer_id,
    ROUND(CAST(5.0 + random() * 95.0 AS DECIMAL(10,2)), 2)      AS amount,
    (CASE WHEN i % 3 = 0 THEN 'shipped'
          WHEN i % 3 = 1 THEN 'pending'
          ELSE 'delivered' END)                                 AS status
FROM generate_series(1, 60) AS t(i);

-- Shipments: a `fulfilment_state` column carrying the same vocabulary as
-- `status`, so the same named test can be re-bound to a differently-named
-- column at the use site.
CREATE OR REPLACE TABLE seeds.shipments AS
SELECT
    CAST(i AS BIGINT)                                            AS shipment_id,
    CAST(1 + (i % 12) AS BIGINT)                                AS customer_id,
    (CASE WHEN i % 2 = 0 THEN 'shipped' ELSE 'delivered' END)   AS fulfilment_state
FROM generate_series(1, 40) AS t(i);
