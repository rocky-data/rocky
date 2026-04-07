-- 25 new events with timestamps after the original max.
INSERT INTO raw__events.events
SELECT
    600 + i AS event_id,
    1 + (i % 100) AS user_id,
    'click' AS event_type,
    TIMESTAMP '2026-05-01' + INTERVAL (i * 60) SECOND AS occurred_at
FROM generate_series(1, 25) AS t(i);
