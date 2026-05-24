-- The plausible schema-only guess.
--
-- An agent that read the column names but never looked at the data writes
-- exactly this: filter on the lowercase status it expected, sum the amount
-- column as if it were dollars. It compiles. It is wrong twice over:
--   1. status = 'completed' never matches (the data holds 'COMPLETE') → 0 rows.
--   2. amount_cents is in cents, not dollars.
SELECT SUM(amount_cents) AS revenue_usd
FROM seeds.orders
WHERE status = 'completed'
