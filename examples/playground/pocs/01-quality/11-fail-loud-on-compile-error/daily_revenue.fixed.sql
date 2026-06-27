-- The corrected SELECT for daily_revenue: it now outputs `order_date`, the
-- column its sidecar declares as `time_column`. run.sh copies this over
-- models/daily_revenue.sql in its final step to prove the same pipeline runs
-- green once the compile error is fixed.
--
-- This file lives outside models/ on purpose so the pipeline's `models/**`
-- glob never loads it as a second model.
SELECT
    CAST(order_at AS DATE) AS order_date,
    region,
    SUM(amount)            AS revenue
FROM raw__sales.orders
WHERE order_at >= @start_date
  AND order_at <  @end_date
GROUP BY 1, 2
