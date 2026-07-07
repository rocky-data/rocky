-- Deterministic raw source — no random() / now(), so the recipe_hash is
-- reproducible across runs. run.sh appends rows between the two runs so both
-- executions BUILD (and both therefore record the same recipe_hash under a
-- different input).
CREATE SCHEMA IF NOT EXISTS raw__orders;

CREATE OR REPLACE TABLE raw__orders.orders AS
SELECT
    i                       AS order_id,
    1 + (i % 25)            AS customer_id,
    'complete'              AS status,
    (i * 137) % 90000 + 500 AS amount_cents,
    TIMESTAMP '2026-04-01' + INTERVAL (i * 3600) SECOND AS placed_at
FROM generate_series(1, 200) AS t(i);
