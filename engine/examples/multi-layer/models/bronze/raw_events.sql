-- Bronze: raw event ingestion, no transformations
SELECT
    event_id,
    user_id,
    event_type,
    event_timestamp,
    page_url,
    session_id,
    device_type,
    ip_address,
    created_at
FROM source.raw.events
