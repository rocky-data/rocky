CREATE OR REPLACE TABLE `tgtwarehouse`.`raw__demo`.`orders` AS
SELECT *
FROM `srcwarehouse`.`src__demo`.`orders`
