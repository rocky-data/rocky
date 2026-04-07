CREATE SCHEMA IF NOT EXISTS raw__events;

-- Simulated incident: only 5 rows instead of the usual ~500.
CREATE OR REPLACE TABLE raw__events.events AS
SELECT
    i AS event_id,
    1 + (i % 50) AS user_id,
    'click' AS event_type
FROM generate_series(1, 5) AS t(i);
