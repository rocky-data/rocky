SELECT customer_id, SUM(amount) AS total_revenue, COUNT(*) AS order_count
FROM RAW__ORDERS.ORDERS
GROUP BY customer_id
