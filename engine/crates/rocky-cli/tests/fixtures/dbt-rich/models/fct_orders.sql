{{ config(materialized='table') }}

SELECT
    o.order_id,
    o.customer_id,
    c.email,
    o.amount,
    o.created_at
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_customers') }} c
  ON o.customer_id = c.customer_id
