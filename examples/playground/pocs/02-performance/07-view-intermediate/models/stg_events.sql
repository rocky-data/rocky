SELECT
    event_id,
    user_id,
    event_type,
    amount,
    occurred_at,
    DATE(occurred_at) AS event_date
FROM seeds.raw_events
WHERE event_type != 'page_view'
