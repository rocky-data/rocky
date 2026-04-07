SELECT
    DATE(order_date) AS revenue_date,
    region,
    COUNT(*)         AS order_count,
    SUM(amount)      AS total_revenue,
    AVG(amount)      AS avg_order_value
FROM raw_orders
GROUP BY DATE(order_date), region
