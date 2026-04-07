-- AI-generated test assertion
-- Intent: exclude cancelled orders from staging
-- Returns 0 rows when assertion holds

SELECT *
FROM warehouse.staging.stg_orders
WHERE status = 'cancelled'
