SELECT
    c.customer_id,
    c.country,
    COUNT(o.order_id) AS order_count,
    SUM(o.amount) AS total_amount,
    MAX(o.status) AS last_status
FROM stg_customers AS c
LEFT JOIN stg_orders AS o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.country
