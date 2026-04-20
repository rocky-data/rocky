-- NVL is Snowflake/Oracle-specific — BigQuery would reject this.
-- Compiling with `--target-dialect bq` fires P001: NonPortableConstruct(NVL).
-- Fix either by rewriting to COALESCE, or by adding `-- rocky-allow: NVL` if
-- you intentionally run only on Snowflake.

SELECT
    order_id,
    NVL(status, 'unknown') AS status
FROM raw__orders.orders
