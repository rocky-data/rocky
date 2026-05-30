SELECT
    c.customer_id,
    c.full_name,
    c.email,
    c.tax_id,
    c.country,
    o.total_orders,
    o.lifetime_value_cents,
    o.last_order_at,
    p.total_paid_cents
FROM demo.dim_customers c
LEFT JOIN (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(amount_cents) AS lifetime_value_cents,
        MAX(ordered_at) AS last_order_at
    FROM demo.stg_orders
    GROUP BY customer_id
) o USING (customer_id)
LEFT JOIN (
    SELECT
        customer_id,
        SUM(amount_cents) AS total_paid_cents
    FROM demo.stg_payments
    GROUP BY customer_id
) p USING (customer_id)
