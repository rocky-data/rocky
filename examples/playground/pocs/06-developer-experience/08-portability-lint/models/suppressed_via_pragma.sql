-- Same NVL usage as non_portable_nvl.sql, but suppressed via the pragma.
-- `rocky-allow` is per-file: if you know this model only ever targets
-- Snowflake, an inline pragma is better than a project-wide allowlist.

-- rocky-allow: NVL
SELECT
    order_id,
    NVL(status, 'unknown') AS status
FROM raw__orders.orders
