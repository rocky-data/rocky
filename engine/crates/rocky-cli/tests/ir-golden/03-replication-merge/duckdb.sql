MERGE INTO tgtwarehouse.raw__demo.customers AS t
USING (
SELECT *
FROM srcwarehouse.src__demo.customers
) AS s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET name = s.name, email = s.email, updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT *
