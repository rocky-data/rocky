-- Singular test (custom SQL). The importer leaves `tests/` untouched —
-- Rocky equivalents are written as `[[tests]]` blocks of `type = "expression"`
-- on the relevant model sidecar, not auto-translated from dbt singular tests.
SELECT *
FROM {{ ref('stg_orders') }}
WHERE amount < 0
