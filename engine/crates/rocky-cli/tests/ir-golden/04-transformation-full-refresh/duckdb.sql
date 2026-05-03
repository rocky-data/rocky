CREATE OR REPLACE TABLE tgtwarehouse.marts__demo.fct_orders AS
SELECT id, customer_id, total FROM tgtwarehouse.raw__demo.orders
