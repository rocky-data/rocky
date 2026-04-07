{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT
    o.order_id,
    o.order_date,
    o.status,
    o.amount,
    c.customer_id,
    c.full_name AS customer_name,
    c.email AS customer_email
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_customers') }} c
    ON o.customer_id = c.customer_id
WHERE o.status != 'cancelled'
