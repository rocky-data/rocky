CREATE OR REPLACE MATERIALIZED VIEW tgtwarehouse.marts__demo.mv_orders_daily AS
SELECT DATE(created_at) AS day, COUNT(*) AS order_count, SUM(total) AS revenue FROM tgtwarehouse.marts__demo.fct_orders GROUP BY 1
