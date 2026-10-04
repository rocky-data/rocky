CREATE TABLE `tgtwarehouse`.`marts__demo`.`events_daily` AS
SELECT * FROM (
SELECT event_date, COUNT(*) AS event_count FROM tgtwarehouse.raw__demo.events WHERE event_date >= '1900-01-01 00:00:00' AND event_date < '1900-01-01 00:00:00' GROUP BY 1
) AS __rocky_bootstrap WHERE 1 = 0
