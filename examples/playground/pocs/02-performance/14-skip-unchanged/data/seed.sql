-- Byte-stable raw source. No random() / now() — runs #1 and #2 see IDENTICAL
-- bytes so the gate can prove the upstream is unchanged. run.sh mutates this
-- table between runs #2 and #3 to force a rebuild.
CREATE SCHEMA IF NOT EXISTS raw__events;

CREATE OR REPLACE TABLE raw__events.events AS
SELECT
    i                 AS event_id,
    1 + (i % 10)      AS user_id,
    'click'           AS event_type,
    TIMESTAMP '2026-04-01' + INTERVAL (i * 60) SECOND AS occurred_at
FROM generate_series(1, 100) AS t(i);
