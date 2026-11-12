SELECT
    i::INTEGER AS order_id,
    'customer_' || (i % 7) AS customer,
    (i * 3.5)::DECIMAL(10, 2) AS amount,
    TIMESTAMP '2026-01-01 00:00:00' + i * INTERVAL 1 HOUR AS ordered_at
FROM range(1, 101) AS t(i)
