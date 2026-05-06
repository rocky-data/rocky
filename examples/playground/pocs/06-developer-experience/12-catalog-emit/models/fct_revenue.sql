SELECT
    customer_id,
    SUM(amount)   AS total,
    COUNT(order_id) AS orders
FROM poc.demo.stg_orders
GROUP BY customer_id
