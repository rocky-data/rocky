SELECT
    c.tier,
    COUNT(o.order_id)  AS orders,
    SUM(o.amount)      AS revenue
FROM raw__sales.orders    AS o
JOIN raw__sales.customers AS c
    ON o.customer_id = c.customer_id
WHERE o.status = 'completed'
GROUP BY c.tier
