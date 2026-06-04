-- Raw source for the staging upstream. Byte-stable (no random()/now()), so the
-- "prod" and "dev" builds in run.sh are identical apart from where they land.
CREATE SCHEMA IF NOT EXISTS raw__events;

CREATE OR REPLACE TABLE raw__events.events AS
SELECT
    i                 AS event_id,
    1 + (i % 5)       AS user_id,
    'click'           AS event_type
FROM generate_series(1, 100) AS t(i);
