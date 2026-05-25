SELECT
    order_id,
    customer_id,
    amount,
    status
FROM raw__sales.orders
WHERE status = 'completed'
