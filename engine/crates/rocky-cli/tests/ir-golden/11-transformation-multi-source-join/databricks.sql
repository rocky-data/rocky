CREATE OR REPLACE TABLE tgtwarehouse.marts__demo.fct_order_lines AS
SELECT o.id AS order_id, c.name AS customer_name, p.sku, o.total FROM tgtwarehouse.raw__demo.orders o JOIN tgtwarehouse.raw__demo.customers c ON o.customer_id = c.id JOIN tgtwarehouse.raw__demo.products p ON o.product_id = p.id
