CREATE SCHEMA IF NOT EXISTS raw__events;

CREATE OR REPLACE TABLE raw__events.events AS
SELECT
    i AS event_id,
    1 + (i % 50) AS user_id,
    'click' AS event_type
FROM generate_series(1, 500) AS t(i);
