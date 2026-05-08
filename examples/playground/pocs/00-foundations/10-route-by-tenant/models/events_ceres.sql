SELECT
    event_id,
    user_id,
    event_type,
    occurred_at
FROM main.events
WHERE account_id = 'ceres'
