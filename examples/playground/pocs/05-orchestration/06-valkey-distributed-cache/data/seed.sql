CREATE SCHEMA IF NOT EXISTS raw__events;

CREATE OR REPLACE TABLE raw__events.events AS
SELECT
    i AS event_id,
    1 + (i % 100) AS user_id,
    CASE (i % 3)
        WHEN 0 THEN 'page_view'
        WHEN 1 THEN 'click'
        ELSE 'purchase'
    END AS event_type,
    TIMESTAMP '2026-04-01' + INTERVAL (i * 60) SECOND AS occurred_at
FROM generate_series(1, 500) AS t(i);
