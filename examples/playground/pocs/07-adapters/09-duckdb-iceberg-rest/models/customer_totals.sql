SELECT customer, COUNT(*) AS orders, SUM(amount) AS total
FROM lk.demo.orders
GROUP BY customer
