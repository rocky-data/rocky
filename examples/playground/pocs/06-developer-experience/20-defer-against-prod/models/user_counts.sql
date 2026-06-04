-- The model under development. It refs stg_events by bare name; with --defer
-- that ref resolves to the prod schema instead of a local table.
SELECT
    user_id,
    COUNT(*) AS event_count
FROM stg_events
GROUP BY user_id
