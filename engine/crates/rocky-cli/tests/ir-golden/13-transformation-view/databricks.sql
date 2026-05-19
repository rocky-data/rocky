CREATE OR REPLACE VIEW tgtwarehouse.marts__demo.v_active_customers AS
SELECT customer_id, name, email FROM tgtwarehouse.marts__demo.dim_customers WHERE status = 'active'
