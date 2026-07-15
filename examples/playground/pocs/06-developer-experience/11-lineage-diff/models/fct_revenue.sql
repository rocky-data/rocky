SELECT
    s.customer_id,
    c.segment,
    c.region,
    SUM(s.amount) AS total
FROM stg_orders s
JOIN dim_customers c USING (customer_id)
GROUP BY s.customer_id, c.segment, c.region
