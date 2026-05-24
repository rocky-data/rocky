-- A pure dimension: no temporal output column, so the W005 coverage check never
-- applies here regardless of whether a [freshness] block is declared.
SELECT
    customer_id,
    name
FROM seeds.customers
