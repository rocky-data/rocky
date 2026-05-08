MERGE INTO tgtwarehouse.marts__demo.dim_customers AS t
USING (
SELECT customer_id, name, email, updated_at FROM tgtwarehouse.raw__demo.customers
) AS s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET name = s.name, email = s.email, updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT *
