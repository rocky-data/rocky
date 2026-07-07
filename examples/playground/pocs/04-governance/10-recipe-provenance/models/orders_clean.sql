-- A plain deterministic SELECT over a bare source. Its canonical typed IR is
-- what the recipe_hash fingerprints: change this SQL and the recipe_hash
-- changes; leave it and every execution shares one recipe_hash, no matter how
-- many times or when it ran.
SELECT
    order_id,
    customer_id,
    UPPER(status) AS status,
    amount_cents,
    placed_at
FROM raw__orders.orders
