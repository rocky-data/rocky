SELECT
    s.customer_id,
    c.segment,
    c.region,
    SUM(s.amount) AS total
FROM poc.demo.stg_orders s
JOIN poc.demo.dim_customers c USING (customer_id)
GROUP BY s.customer_id, c.segment, c.region
