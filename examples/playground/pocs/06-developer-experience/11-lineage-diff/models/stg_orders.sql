SELECT
    order_id,
    customer_id,
    amount,
    status,
    order_date
FROM raw_orders
WHERE status != 'cancelled'
