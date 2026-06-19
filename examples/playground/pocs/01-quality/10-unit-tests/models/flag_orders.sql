SELECT
    order_id,
    region,
    amount,
    amount >= 100 AS is_high_value
FROM raw_orders
ORDER BY amount DESC
