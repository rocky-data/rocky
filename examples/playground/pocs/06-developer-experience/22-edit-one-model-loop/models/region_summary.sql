SELECT region, COUNT(*) AS customers
FROM stg_customers
GROUP BY region
