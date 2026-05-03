INSERT INTO "tgtwarehouse"."raw__demo"."events"
SELECT id, event_type, payload, _synced_at, CAST('rocky' AS STRING) AS _loaded_by
FROM "srcwarehouse"."src__demo"."events"
WHERE _synced_at > (
    SELECT COALESCE(MAX(_synced_at), '1970-01-01'::TIMESTAMP_NTZ)
    FROM "tgtwarehouse"."raw__demo"."events"
)
