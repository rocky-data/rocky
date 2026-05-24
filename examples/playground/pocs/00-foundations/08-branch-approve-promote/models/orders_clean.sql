-- A transformation model on top of the replicated orders. Its file bytes are
-- folded into the branch_state_hash, so editing this SQL after an approval is
-- signed drifts the hash and invalidates the approval (the soundness property
-- this POC's step 8 demonstrates).
SELECT
    order_id,
    customer_id,
    amount,
    ordered_at
FROM poc.prod__orders.orders
WHERE status <> 'cancelled'
