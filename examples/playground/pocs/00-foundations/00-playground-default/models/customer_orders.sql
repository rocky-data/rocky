-- Customer orders aggregation: per-customer revenue from non-cancelled orders.
SELECT
    customer_id,
    SUM(amount) AS total_revenue,
    COUNT(*) AS order_count,
    MIN(order_date) AS first_order
FROM raw_orders
WHERE status <> 'cancelled'
GROUP BY customer_id
HAVING SUM(amount) > 0
