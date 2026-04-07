-- AI-generated test assertion
-- Intent: one row per calendar date (order_date is the unique grain)
-- Returns 0 rows when assertion holds

SELECT order_date, COUNT(*) AS row_count
FROM warehouse.analytics.fct_daily_revenue
GROUP BY order_date
HAVING COUNT(*) > 1
