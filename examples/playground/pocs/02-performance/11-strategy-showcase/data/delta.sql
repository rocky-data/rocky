-- Mutation applied between runs to surface each strategy's differential
-- behavior:
--
--   * UPDATE on existing rows: order 5 status pending -> completed,
--     order 2 amount bumped. Existing rows, so they pre-date the
--     incremental watermark.
--   * INSERT 20 new orders (ids 101..120) with later `ordered_at`
--     timestamps so the incremental watermark catches them.

UPDATE raw__orders.orders SET status = 'completed' WHERE order_id = 5;
UPDATE raw__orders.orders SET amount = 999.99       WHERE order_id = 2;

INSERT INTO raw__orders.orders
SELECT
    CAST(100 + i AS BIGINT)                                  AS order_id,
    CAST(((100 + i - 1) % 20) + 1 AS BIGINT)                 AS customer_id,
    ROUND(10 + ((100 + i) * 1.7), 2)                         AS amount,
    'completed'                                              AS status,
    TIMESTAMP '2026-05-01 09:00:00'
        + (INTERVAL '13 minutes' * (i - 1))                  AS ordered_at
FROM generate_series(1, 20) AS t(i);
