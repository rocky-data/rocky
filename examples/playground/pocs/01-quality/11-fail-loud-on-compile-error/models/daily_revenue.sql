-- A deliberately broken time_interval model.
--
-- The sidecar declares `time_column = "order_date"`, but this SELECT never
-- outputs an `order_date` column — it only emits `region` and `revenue`.
-- Rocky's compiler catches this as E020 (the time_column is absent from the
-- model's output schema) before any SQL is issued to the warehouse.
--
-- The fix is to add the partition column to the SELECT, exactly as
-- `daily_revenue.fixed.sql` does. run.sh applies that fix in its final step to
-- show the same pipeline then runs green.
SELECT
    region,
    SUM(amount) AS revenue
FROM raw__sales.orders
WHERE order_at >= @start_date
  AND order_at <  @end_date
GROUP BY region
