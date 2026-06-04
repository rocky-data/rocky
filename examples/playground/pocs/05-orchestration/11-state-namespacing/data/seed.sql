-- Minimal raw source for the replication pipeline. Byte-stable.
CREATE SCHEMA IF NOT EXISTS raw__events;

CREATE OR REPLACE TABLE raw__events.events AS
SELECT
    i           AS event_id,
    'click'     AS event_type
FROM generate_series(1, 50) AS t(i);
