SELECT
    tenant,
    region,
    COUNT(*) AS order_count,
    SUM(amount) AS total_revenue,
    ROUND(AVG(amount), 2) AS avg_order_value
FROM stg_orders
WHERE status = 'completed'
GROUP BY tenant, region
