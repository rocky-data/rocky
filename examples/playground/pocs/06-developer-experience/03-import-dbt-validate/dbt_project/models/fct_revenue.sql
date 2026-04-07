{{ config(materialized='table') }}

SELECT
    customer_id,
    SUM(amount) AS total_revenue,
    COUNT(*) AS order_count
FROM {{ ref('stg_orders') }}
GROUP BY customer_id
