SELECT
    order_id,
    customer_id,
    COALESCE(CAST(amount_cents AS DECIMAL(12, 2)) / 100, 0) AS amount,
    LOWER(status) AS status,
    coupon
FROM raw.orders
