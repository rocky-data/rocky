SELECT
    TIMESTAMP_TRUNC(order_at, DAY) AS order_day,
    customer_id,
    COUNT(*)                       AS order_count,
    SUM(amount)                    AS revenue
FROM `__GCP_PROJECT__`.`poc_step1_live_ti`.`orders_src`
WHERE order_at >= @start_date
  AND order_at <  @end_date
GROUP BY 1, 2
