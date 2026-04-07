INSERT INTO raw__orders.orders
SELECT
    200 + i AS order_id,
    TIMESTAMP '2026-05-01' + INTERVAL (i * 60) SECOND AS _updated_at
FROM generate_series(1, 25) AS t(i);
