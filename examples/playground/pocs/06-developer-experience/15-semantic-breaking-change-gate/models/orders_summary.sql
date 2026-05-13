-- Baseline shape: per-customer totals.
-- The feature branch will drop the `total_amount` column — a Breaking-
-- severity change the pre-promote gate must reject.
SELECT
    customer_id,
    SUM(amount) AS total_amount
FROM poc.demo.raw_orders
GROUP BY customer_id
