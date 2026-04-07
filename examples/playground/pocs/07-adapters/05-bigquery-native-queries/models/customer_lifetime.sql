SELECT
    customer_id,
    MIN(order_date)  AS first_order,
    MAX(order_date)  AS last_order,
    COUNT(*)         AS total_orders,
    SUM(amount)      AS lifetime_value,
    AVG(amount)      AS avg_order_value
FROM raw_orders
GROUP BY customer_id
