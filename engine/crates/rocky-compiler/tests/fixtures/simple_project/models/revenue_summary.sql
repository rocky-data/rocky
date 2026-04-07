SELECT
    customer_id,
    total_revenue,
    order_count,
    total_revenue / order_count AS avg_order_value
FROM customer_orders
WHERE total_revenue > 100
