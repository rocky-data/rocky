-- Seed data for the partition-checksum POC.
--
-- 9 orders spanning 5 days (2026-04-04 → 2026-04-08), 2 customers,
-- varying revenue. Designed to demonstrate the time_interval
-- materialization strategy:
--
--   - Each row has a real TIMESTAMP `order_at` (not just a DATE), so
--     the model can group by DATE(order_at) and the @start_date /
--     @end_date filter can scope precisely on the timestamp.
--
--   - The data spans multiple daily partitions so we can run different
--     `--partition` flags and see only that day's rows recompute.
--
--   - The late-arrival scenario at the bottom of run.sh inserts a
--     row dated 2026-04-07 AFTER the 04-07 partition has already
--     been computed; re-running the same partition picks it up.

CREATE SCHEMA IF NOT EXISTS raw__orders;

DROP TABLE IF EXISTS raw__orders.orders;

CREATE TABLE raw__orders.orders (
    order_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    order_at TIMESTAMP NOT NULL,
    amount DOUBLE NOT NULL
);

INSERT INTO raw__orders.orders VALUES
    (1, 1, '2026-04-04 09:00:00',  100.0),
    (2, 2, '2026-04-04 14:00:00',   50.0),
    (3, 1, '2026-04-05 10:00:00',  200.0),
    (4, 2, '2026-04-06 11:00:00',   75.0),
    (5, 2, '2026-04-06 16:00:00',   25.0),
    (6, 1, '2026-04-07 08:00:00',  300.0),
    (7, 1, '2026-04-07 13:00:00',  150.0),
    (8, 2, '2026-04-07 18:00:00',   80.0),
    (9, 1, '2026-04-08 12:00:00',  500.0);

-- The marts schema must exist before the model runs. Rocky's
-- bootstrap-on-first-run handles the *table* creation automatically:
-- on the first partition run, execute_time_interval_model detects the
-- target table is missing and emits a CREATE TABLE AS with the model
-- SQL + an empty [start, start) window, materializing a zero-row table
-- with the correct output schema. We only need to create the schema.
CREATE SCHEMA IF NOT EXISTS marts;
