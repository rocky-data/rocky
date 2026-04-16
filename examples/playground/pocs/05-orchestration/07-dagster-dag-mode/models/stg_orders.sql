SELECT
    order_id,
    customer_id,
    amount,
    status,
    order_date
FROM raw__orders.orders
WHERE status != 'cancelled'
