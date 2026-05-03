CREATE OR REPLACE TABLE tgtwarehouse.marts__demo.fct_events_delta
USING DELTA
PARTITIONED BY (event_date)
AS
SELECT id, event_type, event_date, payload FROM tgtwarehouse.raw__demo.events
