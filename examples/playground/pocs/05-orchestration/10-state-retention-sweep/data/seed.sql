-- Bootstrap raw__orders.orders. run.sh seeds this once, then replays six
-- full-refresh replication runs against it to exercise the sweep paths.
CREATE SCHEMA IF NOT EXISTS raw__orders;

CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT
    i AS order_id,
    100 + (i % 5) AS customer_id,
    'completed' AS status,
    10.0 * i AS amount
FROM range(1, 6) AS t(i);
