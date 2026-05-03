INSERT INTO tgtwarehouse.raw__demo.events
SELECT id, event_type, payload, _synced_at, CAST('rocky' AS STRING) AS _loaded_by
FROM srcwarehouse.src__demo.events
WHERE _synced_at > (
    SELECT COALESCE(MAX(_synced_at), TIMESTAMP '1970-01-01')
    FROM tgtwarehouse.raw__demo.events
)
