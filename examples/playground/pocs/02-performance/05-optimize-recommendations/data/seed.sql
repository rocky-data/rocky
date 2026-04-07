CREATE SCHEMA IF NOT EXISTS raw__events;

CREATE OR REPLACE TABLE raw__events.events AS
SELECT i AS id, 'click' AS event_type FROM generate_series(1, 200) AS t(i);
