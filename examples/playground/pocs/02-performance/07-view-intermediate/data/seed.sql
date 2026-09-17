CREATE SCHEMA IF NOT EXISTS seeds;

CREATE OR REPLACE TABLE seeds.raw_events AS
SELECT
    i AS event_id,
    1 + (i % 50) AS user_id,
    CASE (i % 4)
        WHEN 0 THEN 'page_view'
        WHEN 1 THEN 'click'
        WHEN 2 THEN 'purchase'
        ELSE 'signup'
    END AS event_type,
    ROUND(RANDOM() * 100, 2) AS amount,
    TIMESTAMP '2026-04-01' + INTERVAL (i * 60) SECOND AS occurred_at
FROM generate_series(1, 500) AS t(i);
