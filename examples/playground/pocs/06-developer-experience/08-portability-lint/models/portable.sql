-- A portable model — no dialect-divergent constructs.
-- Compiles cleanly for any --target-dialect (dbx / sf / bq / duckdb).

SELECT
    order_id,
    customer_id,
    amount,
    COALESCE(status, 'unknown') AS status
FROM raw__orders.orders
