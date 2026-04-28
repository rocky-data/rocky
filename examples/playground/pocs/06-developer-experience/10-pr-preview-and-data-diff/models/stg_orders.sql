SELECT
    order_id,
    customer_id,
    amount,
    status,
    order_date
FROM poc.demo.raw_orders
WHERE status != 'cancelled'
