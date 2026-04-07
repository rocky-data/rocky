CREATE SCHEMA IF NOT EXISTS raw__orders;
CREATE SCHEMA IF NOT EXISTS raw__customers;
CREATE SCHEMA IF NOT EXISTS raw__products;

CREATE OR REPLACE TABLE raw__orders.orders     AS SELECT i AS id FROM generate_series(1, 50) AS t(i);
CREATE OR REPLACE TABLE raw__customers.customers AS SELECT i AS id FROM generate_series(1, 50) AS t(i);
CREATE OR REPLACE TABLE raw__products.products AS SELECT i AS id FROM generate_series(1, 50) AS t(i);
