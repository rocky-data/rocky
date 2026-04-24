-- Intentionally has no retention sidecar key. Demonstrates that the
-- `retention` field is optional and absent models surface as `null` /
-- `-` in `rocky retention-status`.
SELECT
    COUNT(*) AS total_events
FROM raw__events.page_views
