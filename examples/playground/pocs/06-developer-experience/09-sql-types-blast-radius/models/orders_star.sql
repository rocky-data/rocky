-- Blast-radius lint: `SELECT *` from a table inside the project's semantic
-- graph fires a P002 `SelectStar` diagnostic. The rationale: downstream
-- models pin their column list; any column added upstream silently
-- propagates. The lint is semantic-graph aware — `SELECT *` from a lateral
-- subquery or CTE is fine because its shape is fully determined.

SELECT *
FROM raw__orders.orders
