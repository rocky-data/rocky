CREATE SCHEMA IF NOT EXISTS raw__orders;

-- 20 tables to simulate parallel processing pressure
CREATE OR REPLACE TABLE raw__orders.orders_01 AS SELECT i AS id, 'a' AS val, TIMESTAMP '2026-04-01' AS ts FROM generate_series(1, 100) AS t(i);
CREATE OR REPLACE TABLE raw__orders.orders_02 AS SELECT i AS id, 'b' AS val, TIMESTAMP '2026-04-01' AS ts FROM generate_series(1, 100) AS t(i);
CREATE OR REPLACE TABLE raw__orders.orders_03 AS SELECT i AS id, 'c' AS val, TIMESTAMP '2026-04-01' AS ts FROM generate_series(1, 100) AS t(i);
CREATE OR REPLACE TABLE raw__orders.orders_04 AS SELECT i AS id, 'd' AS val, TIMESTAMP '2026-04-01' AS ts FROM generate_series(1, 100) AS t(i);
CREATE OR REPLACE TABLE raw__orders.orders_05 AS SELECT i AS id, 'e' AS val, TIMESTAMP '2026-04-01' AS ts FROM generate_series(1, 100) AS t(i);
CREATE OR REPLACE TABLE raw__orders.orders_06 AS SELECT i AS id, 'f' AS val, TIMESTAMP '2026-04-01' AS ts FROM generate_series(1, 100) AS t(i);
CREATE OR REPLACE TABLE raw__orders.orders_07 AS SELECT i AS id, 'g' AS val, TIMESTAMP '2026-04-01' AS ts FROM generate_series(1, 100) AS t(i);
CREATE OR REPLACE TABLE raw__orders.orders_08 AS SELECT i AS id, 'h' AS val, TIMESTAMP '2026-04-01' AS ts FROM generate_series(1, 100) AS t(i);
CREATE OR REPLACE TABLE raw__orders.orders_09 AS SELECT i AS id, 'i' AS val, TIMESTAMP '2026-04-01' AS ts FROM generate_series(1, 100) AS t(i);
CREATE OR REPLACE TABLE raw__orders.orders_10 AS SELECT i AS id, 'j' AS val, TIMESTAMP '2026-04-01' AS ts FROM generate_series(1, 100) AS t(i);
CREATE OR REPLACE TABLE raw__orders.orders_11 AS SELECT i AS id, 'k' AS val, TIMESTAMP '2026-04-01' AS ts FROM generate_series(1, 100) AS t(i);
CREATE OR REPLACE TABLE raw__orders.orders_12 AS SELECT i AS id, 'l' AS val, TIMESTAMP '2026-04-01' AS ts FROM generate_series(1, 100) AS t(i);
CREATE OR REPLACE TABLE raw__orders.orders_13 AS SELECT i AS id, 'm' AS val, TIMESTAMP '2026-04-01' AS ts FROM generate_series(1, 100) AS t(i);
CREATE OR REPLACE TABLE raw__orders.orders_14 AS SELECT i AS id, 'n' AS val, TIMESTAMP '2026-04-01' AS ts FROM generate_series(1, 100) AS t(i);
CREATE OR REPLACE TABLE raw__orders.orders_15 AS SELECT i AS id, 'o' AS val, TIMESTAMP '2026-04-01' AS ts FROM generate_series(1, 100) AS t(i);
CREATE OR REPLACE TABLE raw__orders.orders_16 AS SELECT i AS id, 'p' AS val, TIMESTAMP '2026-04-01' AS ts FROM generate_series(1, 100) AS t(i);
CREATE OR REPLACE TABLE raw__orders.orders_17 AS SELECT i AS id, 'q' AS val, TIMESTAMP '2026-04-01' AS ts FROM generate_series(1, 100) AS t(i);
CREATE OR REPLACE TABLE raw__orders.orders_18 AS SELECT i AS id, 'r' AS val, TIMESTAMP '2026-04-01' AS ts FROM generate_series(1, 100) AS t(i);
CREATE OR REPLACE TABLE raw__orders.orders_19 AS SELECT i AS id, 's' AS val, TIMESTAMP '2026-04-01' AS ts FROM generate_series(1, 100) AS t(i);
CREATE OR REPLACE TABLE raw__orders.orders_20 AS SELECT i AS id, 't' AS val, TIMESTAMP '2026-04-01' AS ts FROM generate_series(1, 100) AS t(i);
