-- Synthetic source row inline; transformation models only need the
-- shape, not the live raw__orders feed (that's the replication pipeline's
-- job). Keeps the breaking-change gate self-contained at compile-time.
SELECT * FROM (VALUES
    (1, 101, 'completed', 49.50),
    (2, 102, 'completed', 119.00),
    (3, 103, 'cancelled', 0.00)
) AS t(order_id, customer_id, status, amount)
