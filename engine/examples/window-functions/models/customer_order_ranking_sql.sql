-- SQL equivalent of customer_order_ranking.rocky
-- Shows the same logic in standard SQL for comparison.
SELECT
    order_id,
    customer_id,
    region,
    amount,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn,
    RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS order_rank,
    DENSE_RANK() OVER (PARTITION BY region ORDER BY amount DESC) AS order_dense_rank
FROM source.raw.orders
