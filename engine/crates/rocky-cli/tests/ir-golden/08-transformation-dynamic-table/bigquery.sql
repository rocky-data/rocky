CREATE OR REPLACE DYNAMIC TABLE `tgtwarehouse`.`marts__demo`.`dt_orders_recent`
  TARGET_LAG = '1 hour'
  WAREHOUSE = compute_wh
AS
SELECT id, customer_id, total FROM tgtwarehouse.marts__demo.fct_orders WHERE created_at > CURRENT_DATE - INTERVAL '7' DAY
