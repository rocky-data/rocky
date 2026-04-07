SELECT customer_id, SUM(amount) AS total_revenue, COUNT(*) AS order_count
FROM main.raw__orders.orders
GROUP BY customer_id
