SELECT
    customer_id,
    SUM(amount)   AS total,
    COUNT(order_id) AS orders
FROM stg_orders
GROUP BY customer_id
