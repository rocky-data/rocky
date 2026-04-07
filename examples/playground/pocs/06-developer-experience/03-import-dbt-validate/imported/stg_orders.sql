SELECT
    order_id,
    customer_id,
    amount,
    LOWER(status) AS status
FROM raw.orders
WHERE status != 'cancelled'