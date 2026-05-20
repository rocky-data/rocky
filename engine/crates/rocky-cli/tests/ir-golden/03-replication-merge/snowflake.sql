MERGE INTO "tgtwarehouse"."raw__demo"."customers" AS t
USING (
SELECT *
FROM "srcwarehouse"."src__demo"."customers"
) AS s
ON t."customer_id" = s."customer_id"
WHEN MATCHED THEN UPDATE SET t."name" = s."name", t."email" = s."email", t."updated_at" = s."updated_at"
WHEN NOT MATCHED THEN INSERT ("name", "email", "updated_at") VALUES (s."name", s."email", s."updated_at")
