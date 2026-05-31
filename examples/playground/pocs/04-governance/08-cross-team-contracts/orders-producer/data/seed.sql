-- Raw orders table the producer's `orders` model reads from.
-- The schema name (`raw__orders`) + table (`orders`) form the 2-part key
-- the --with-seed loader uses, which must match the producer SQL's FROM.
CREATE SCHEMA IF NOT EXISTS raw__orders;

CREATE TABLE raw__orders.orders (
    id          BIGINT,
    customer_id BIGINT,
    amount      DECIMAL(12, 2),
    shipped_at  TIMESTAMP
);

INSERT INTO raw__orders.orders VALUES
    (1, 100, 49.99,  TIMESTAMP '2026-01-01 09:00:00'),
    (2, 101, 19.50,  TIMESTAMP '2026-01-02 14:30:00'),
    (3, 100, 129.00, TIMESTAMP '2026-01-03 11:15:00');
