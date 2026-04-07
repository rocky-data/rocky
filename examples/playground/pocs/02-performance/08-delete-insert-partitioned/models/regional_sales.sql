SELECT
    sale_date,
    region,
    COUNT(*)    AS order_count,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_order_value
FROM seeds.daily_sales
GROUP BY sale_date, region
