SELECT
    user_id,
    COUNT(*)                                     AS event_count,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchase_count,
    COALESCE(SUM(amount), 0)                     AS total_spent,
    MIN(event_date)                              AS first_active,
    MAX(event_date)                              AS last_active
FROM stg_events
GROUP BY user_id
