SELECT
    customer_id,
    COUNT(*)   AS order_count,
    SUM(amount) AS total_revenue
FROM poc.mart.stg_orders
GROUP BY customer_id
