CREATE SCHEMA IF NOT EXISTS raw__orders;
CREATE SCHEMA IF NOT EXISTS raw__customers;

CREATE OR REPLACE TABLE raw__orders.orders       AS SELECT i AS id FROM generate_series(1, 10) AS t(i);
CREATE OR REPLACE TABLE raw__customers.customers AS SELECT i AS id FROM generate_series(1, 10) AS t(i);
