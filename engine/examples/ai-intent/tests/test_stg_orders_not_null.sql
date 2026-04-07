-- AI-generated test assertion
-- Intent: order_id is the unique grain and must never be null
-- Returns 0 rows when assertion holds

SELECT *
FROM warehouse.staging.stg_orders
WHERE order_id IS NULL
