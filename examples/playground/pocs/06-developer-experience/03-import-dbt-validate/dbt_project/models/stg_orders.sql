{{ config(materialized='view') }}

SELECT
    order_id,
    customer_id,
    amount,
    LOWER(status) AS status
FROM {{ source('raw', 'orders') }}
WHERE status != 'cancelled'
