-- Breaking variant: the producer team drops `shipped_at` from the output.
-- run.sh swaps this in for orders.sql at step 3 to simulate the producer
-- shipping a breaking change.
SELECT
    id,
    customer_id,
    amount
FROM raw__orders.orders
