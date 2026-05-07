{{ config(materialized='incremental', unique_key='order_id') }}

SELECT
    order_id,
    customer_id,
    amount,
    created_at
FROM {{ source('raw', 'orders') }}
