-- Engineered reconcile trap. Two columns carry surprises the schema hides:
--
--   status        Only ever 'COMPLETE' (uppercase). A schema-only guess of
--                 WHERE status = 'completed' compiles fine and returns ZERO
--                 rows. The other statuses are 'PENDING' and 'CANCELLED'.
--
--   amount_cents  Stored in CENTS as an integer. Summing it as-if-dollars
--                 overstates revenue by 100x.
--
-- Ground truth (used by the suite's optional reconcile assertions):
--   * completed-order revenue in USD:
--       (12500 + 7500 + 30000 + 5000 + 45000) cents = 100000 cents = $1000.00
--   * number of completed orders: 5
--   * total orders: 8
CREATE SCHEMA IF NOT EXISTS seeds;

CREATE OR REPLACE TABLE seeds.orders (
    order_id      BIGINT,
    customer_id   BIGINT,
    status        VARCHAR,
    amount_cents  BIGINT,
    ordered_at    TIMESTAMP
);

INSERT INTO seeds.orders VALUES
    -- Completed orders — these five make up the true revenue.
    (1, 101, 'COMPLETE',  12500, TIMESTAMP '2026-01-02 09:14:00'),
    (2, 102, 'COMPLETE',   7500, TIMESTAMP '2026-01-02 11:40:00'),
    (3, 103, 'COMPLETE',  30000, TIMESTAMP '2026-01-03 08:05:00'),
    (4, 104, 'COMPLETE',   5000, TIMESTAMP '2026-01-03 16:22:00'),
    (5, 105, 'COMPLETE',  45000, TIMESTAMP '2026-01-04 10:01:00'),
    -- Non-completed orders — must NOT count toward completed revenue.
    (6, 106, 'PENDING',   90000, TIMESTAMP '2026-01-04 12:30:00'),
    (7, 107, 'CANCELLED', 25000, TIMESTAMP '2026-01-05 14:45:00'),
    (8, 108, 'PENDING',   18000, TIMESTAMP '2026-01-05 17:10:00');
