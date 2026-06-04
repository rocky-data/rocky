-- Staging upstream. Plain SELECT over the raw source — bare-name refs only,
-- no SELECT * EXCEPT / STRUCT literals / trailing commas (the --defer SQL
-- rewrite parses with the Databricks dialect and rejects those constructs).
SELECT
    event_id,
    user_id,
    event_type
FROM raw__events.events
