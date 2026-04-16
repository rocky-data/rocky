SELECT
    customer_id,
    SUM(amount) AS total_revenue,
    COUNT(*) AS order_count,
    MIN(order_date) AS first_order
FROM staging.stg_orders
GROUP BY customer_id
