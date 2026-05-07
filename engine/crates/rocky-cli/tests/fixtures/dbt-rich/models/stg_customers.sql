{{ config(materialized='view') }}

SELECT
    customer_id,
    name,
    email,
    created_at
FROM {{ source('raw', 'customers') }}
