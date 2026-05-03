CREATE TABLE IF NOT EXISTS "tgtwarehouse"."snapshots__demo"."dim_customers_history" AS SELECT *, CURRENT_TIMESTAMP AS valid_from, CAST(NULL AS TIMESTAMP) AS valid_to FROM "tgtwarehouse"."marts__demo"."dim_customers" WHERE 1=0
;

-- --- next statement ---

MERGE INTO "tgtwarehouse"."snapshots__demo"."dim_customers_history" AS target USING "tgtwarehouse"."marts__demo"."dim_customers" AS source ON target.customer_id = source.customer_id AND target.valid_to IS NULL WHEN MATCHED AND source.updated_at != target.updated_at THEN UPDATE SET valid_to = CURRENT_TIMESTAMP WHEN NOT MATCHED THEN INSERT (*) VALUES (source.*, CURRENT_TIMESTAMP, NULL)
;

-- --- next statement ---

INSERT INTO "tgtwarehouse"."snapshots__demo"."dim_customers_history" SELECT source.*, CURRENT_TIMESTAMP AS valid_from, CAST(NULL AS TIMESTAMP) AS valid_to FROM "tgtwarehouse"."marts__demo"."dim_customers" AS source INNER JOIN "tgtwarehouse"."snapshots__demo"."dim_customers_history" AS target ON target.customer_id = source.customer_id WHERE target.valid_to = (SELECT MAX(t2.valid_to) FROM "tgtwarehouse"."snapshots__demo"."dim_customers_history" t2 WHERE t2.customer_id = source.customer_id) AND NOT EXISTS (SELECT 1 FROM "tgtwarehouse"."snapshots__demo"."dim_customers_history" AS existing WHERE existing.customer_id = source.customer_id AND existing.valid_to IS NULL)
;

-- --- next statement ---

UPDATE "tgtwarehouse"."snapshots__demo"."dim_customers_history" SET valid_to = CURRENT_TIMESTAMP WHERE valid_to IS NULL AND NOT EXISTS (SELECT 1 FROM "tgtwarehouse"."marts__demo"."dim_customers" AS source WHERE target.customer_id = source.customer_id)
