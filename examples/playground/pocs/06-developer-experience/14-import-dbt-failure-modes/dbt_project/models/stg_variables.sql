{{ config(materialized='table') }}

SELECT
    order_id,
    customer_id,
    amount
FROM {{ source('raw', 'orders') }}
WHERE customer_id > {{ var('cutoff') }}
