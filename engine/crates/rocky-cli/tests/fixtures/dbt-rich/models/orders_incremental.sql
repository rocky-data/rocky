{{ config(materialized='incremental', unique_key='order_id') }}

SELECT
    order_id,
    created_at
FROM {{ source('raw', 'orders') }}
