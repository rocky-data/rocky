-- Orders staged from raw. `order_ts` passes through from the source as a
-- TIMESTAMP, but this model declares no freshness expectation, so the compiler
-- raises W005 to nudge the author toward an explicit SLA. (Fix: add a
-- [freshness] block to the sidecar — see stg_shipments for the shape.)
SELECT
    order_id,
    customer_id,
    order_ts,
    amount
FROM seeds.orders
