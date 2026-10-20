SELECT c.customer_id, c.region, COUNT(*) AS order_count, SUM(o.amount) AS total
FROM stg_orders o
JOIN stg_customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.region
