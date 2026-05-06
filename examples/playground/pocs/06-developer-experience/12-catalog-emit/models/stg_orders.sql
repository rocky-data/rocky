SELECT order_id, customer_id, amount, status
FROM poc.demo.raw_orders
WHERE status != 'cancelled'
