WITH base AS (
  SELECT 1 AS customer_id, 'acme' AS name
)
SELECT customer_id, name FROM base
