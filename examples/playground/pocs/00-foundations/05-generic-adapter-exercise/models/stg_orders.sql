-- Reads `raw__orders` directly so `rocky test` (in-memory DuckDB) finds the
-- table via `data/seed.sql`. The replication step in run.sh exercises the
-- separate raw__→staging__ flow but isn't a dependency of this model.
SELECT
    order_id,
    customer_id,
    product,
    amount,
    status,
    ordered_at
FROM raw__orders.orders
