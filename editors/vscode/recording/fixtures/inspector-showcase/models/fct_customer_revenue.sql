SELECT
    country,
    COUNT(*) AS customers,
    SUM(lifetime_value_cents) AS revenue_cents,
    SUM(total_orders) AS orders
FROM demo.customer_360
GROUP BY country
