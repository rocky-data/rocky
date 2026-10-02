{{ config(materialized='table') }}

SELECT
    order_id,
    customer_id,
    amount,
    created_at
FROM {{ source('raw', 'orders') }}
