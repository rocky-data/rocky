SELECT customer, COUNT(*) AS orders, SUM(amount) AS total
FROM dl.demo.orders
GROUP BY customer
