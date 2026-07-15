SELECT order_id, customer_id, amount, status
FROM raw_orders
WHERE status != 'cancelled'
