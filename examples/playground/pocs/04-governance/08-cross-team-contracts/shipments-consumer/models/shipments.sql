-- Consumer model. It reads the producer's published `orders` table and
-- depends on the `shipped_at` column. The [imports.orders] block in
-- rocky.toml lets `rocky compile` verify that column still exists in the
-- producer's snapshot — if the producer drops it, this compile fails (E030).
SELECT
    id,
    customer_id,
    shipped_at
FROM shop.core.orders
