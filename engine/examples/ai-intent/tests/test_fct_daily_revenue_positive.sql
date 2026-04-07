-- AI-generated test assertion
-- Intent: total_revenue must be positive for every day
-- Returns 0 rows when assertion holds

SELECT *
FROM warehouse.analytics.fct_daily_revenue
WHERE total_revenue <= 0
