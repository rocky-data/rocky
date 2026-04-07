-- Daily revenue by customer.
--
-- The @start_date / @end_date placeholders are substituted by Rocky at
-- runtime per partition with quoted timestamp literals (e.g.
-- @start_date → '2026-04-07 00:00:00' for partition key 2026-04-07).
-- The window is half-open: rows at exactly @start_date are included,
-- rows at exactly @end_date are excluded.

SELECT
    CAST(order_at AS DATE) AS order_date,
    customer_id,
    COUNT(*)               AS order_count,
    SUM(amount)            AS revenue
FROM raw__orders.orders
WHERE order_at >= @start_date
  AND order_at <  @end_date
GROUP BY 1, 2
