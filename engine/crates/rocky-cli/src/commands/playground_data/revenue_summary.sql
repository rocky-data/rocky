SELECT
    customer_id,
    total_revenue,
    order_count,
    total_revenue / order_count AS avg_order_value,
    first_order
FROM customer_orders
WHERE order_count >= 2
