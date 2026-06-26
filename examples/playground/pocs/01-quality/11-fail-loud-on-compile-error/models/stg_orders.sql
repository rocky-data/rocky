-- A clean staging model. Full refresh, no run-time placeholders, compiles
-- and materializes on every run. This is the "good data" that must still
-- land even when a sibling model in the same run fails to compile.
SELECT
    order_id,
    region,
    amount,
    order_at
FROM raw__sales.orders
