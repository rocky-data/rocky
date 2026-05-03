MERGE INTO "tgtwarehouse"."marts__demo"."dim_customers" AS t
USING (
SELECT customer_id, name, email, updated_at FROM tgtwarehouse.raw__demo.customers
) AS s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET t.name = s.name, t.email = s.email, t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT *
