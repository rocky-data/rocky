-- The data-grounded model.
--
-- Written after sampling real rows (the MCP `sample_rows` + `profile_column`
-- tools, or a direct DuckDB query): status is uppercase 'COMPLETE', and
-- amount_cents is in cents. Filter on the literal that exists, divide by 100
-- to land in dollars.
SELECT SUM(amount_cents) / 100.0 AS revenue_usd
FROM seeds.orders
WHERE status = 'COMPLETE'
