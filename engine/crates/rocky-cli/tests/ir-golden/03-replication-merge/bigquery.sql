MERGE INTO `tgtwarehouse`.`raw__demo`.`customers` AS target
USING (SELECT *
FROM `srcwarehouse`.`src__demo`.`customers`) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET target.name = source.name, target.email = source.email, target.updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT ROW
