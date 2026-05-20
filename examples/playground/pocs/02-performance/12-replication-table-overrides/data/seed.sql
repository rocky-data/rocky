-- Seed data for 12-replication-table-overrides POC.
--
-- Two source schemas:
--   raw__orders   — matched by Rules 1–4 (connector-level and table-level overrides)
--   raw__events   — matched by Rules 5–6 (glob + incremental override)
--
-- Tables that Rule 4 / 5 will disable (enabled = false):
--   raw__orders._diagnostics_sync  ← matches glob _diagnostics_*
--   raw__events.user_01            ← matches glob user_?? in raw__events
--   raw__events.user_02            ← matches glob user_?? in raw__events

-- ── raw__orders ─────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS raw__orders;

-- orders: Rule 2 sets strategy = "merge", merge_keys = ["order_id"] (from Rule 1)
CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT
    CAST(i AS BIGINT)                                    AS order_id,
    CAST(((i - 1) % 10) + 1 AS BIGINT)                  AS customer_id,
    ROUND(10.0 + (i * 3.5), 2)                          AS amount,
    CASE WHEN i % 5 = 0 THEN 'cancelled'
         WHEN i % 3 = 0 THEN 'pending'
         ELSE 'completed' END                            AS status,
    TIMESTAMP '2026-05-01 08:00:00'
        + (INTERVAL '10 minutes' * (i - 1))             AS ordered_at
FROM generate_series(1, 50) AS t(i);

-- order_items: Rule 3 sets strategy = "incremental", timestamp_column = "created_at"
CREATE OR REPLACE TABLE raw__orders.order_items AS
SELECT
    CAST(i AS BIGINT)                                    AS item_id,
    CAST(((i - 1) % 50) + 1 AS BIGINT)                  AS order_id,
    'SKU-' || LPAD(CAST((i % 20) + 1 AS VARCHAR), 3, '0') AS sku,
    CAST((i % 5) + 1 AS BIGINT)                         AS quantity,
    ROUND(5.0 + (i * 1.2), 2)                           AS unit_price,
    TIMESTAMP '2026-05-01 08:00:00'
        + (INTERVAL '8 minutes' * (i - 1))              AS created_at
FROM generate_series(1, 100) AS t(i);

-- _diagnostics_sync: Rule 4 (glob `*`) disables this table — never replicated
CREATE OR REPLACE TABLE raw__orders._diagnostics_sync AS
SELECT
    CAST(i AS BIGINT)                              AS sync_id,
    'connector_abc' || CAST(i AS VARCHAR)          AS connector_id,
    TIMESTAMP '2026-05-20 00:00:00'               AS synced_at,
    'ok'                                           AS sync_status
FROM generate_series(1, 5) AS t(i);

-- ── raw__events ──────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS raw__events;

-- events: Rule 6 sets strategy = "incremental", timestamp_column = "occurred_at"
CREATE OR REPLACE TABLE raw__events.events AS
SELECT
    CAST(i AS BIGINT)                                    AS event_id,
    CASE WHEN i % 4 = 0 THEN 'checkout'
         WHEN i % 4 = 1 THEN 'page_view'
         WHEN i % 4 = 2 THEN 'add_to_cart'
         ELSE 'search' END                               AS event_type,
    CAST(((i - 1) % 20) + 1 AS BIGINT)                  AS user_id,
    TIMESTAMP '2026-05-01 09:00:00'
        + (INTERVAL '5 minutes' * (i - 1))              AS occurred_at
FROM generate_series(1, 80) AS t(i);

-- user_01, user_02: Rule 5 (glob `??`) disables both — never replicated
CREATE OR REPLACE TABLE raw__events.user_01 AS
SELECT 1 AS user_id, 'alice' AS username, TIMESTAMP '2026-05-01' AS created_at;

CREATE OR REPLACE TABLE raw__events.user_02 AS
SELECT 2 AS user_id, 'bob' AS username, TIMESTAMP '2026-05-01' AS created_at;
