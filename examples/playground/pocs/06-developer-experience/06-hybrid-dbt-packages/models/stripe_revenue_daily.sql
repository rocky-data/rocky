-- Revenue from Stripe charges — sourced from dbt_stripe package output.
-- Demonstrates Rocky consuming a second dbt package alongside Fivetran/Facebook.

SELECT
    created_date,
    COUNT(*)                                            AS total_charges,
    COUNT(*) FILTER (WHERE status = 'succeeded')        AS successful_charges,
    COUNT(*) FILTER (WHERE status = 'failed')           AS failed_charges,
    ROUND(SUM(CASE WHEN status = 'succeeded' THEN amount_cents ELSE 0 END) / 100.0, 2) AS revenue_usd,
    ROUND(AVG(CASE WHEN status = 'succeeded' THEN amount_cents ELSE NULL END) / 100.0, 2) AS avg_charge_usd,
    COUNT(DISTINCT customer_id)                         AS unique_customers
FROM dbt_stripe.stg_stripe__charges
GROUP BY created_date
