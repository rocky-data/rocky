CREATE SCHEMA IF NOT EXISTS raw__orders;

CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT i AS order_id FROM generate_series(1, 10) AS t(i);
