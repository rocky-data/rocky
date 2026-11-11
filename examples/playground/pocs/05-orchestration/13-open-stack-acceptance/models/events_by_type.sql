SELECT
    event_type,
    COUNT(*) AS events,
    CAST(SUM(amount_cents) AS BIGINT) AS amount_cents
FROM poc.main.events
GROUP BY event_type
