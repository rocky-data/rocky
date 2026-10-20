SELECT order_date, SUM(amount) AS revenue
FROM stg_orders
GROUP BY order_date
