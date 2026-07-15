-- Staging model: clean raw orders for downstream consumption.
-- `rocky ai-test` reads the intent (below, in the sidecar) plus these
-- typed output columns to generate SQL assertions.

SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    status
FROM raw__orders.orders
WHERE status != 'cancelled'
  AND amount > 0
