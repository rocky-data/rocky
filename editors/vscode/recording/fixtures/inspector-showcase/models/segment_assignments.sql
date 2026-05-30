SELECT
    customer_id,
    CASE
        WHEN lifetime_value_cents >= 42000 THEN 'high'
        WHEN lifetime_value_cents > 0 THEN 'mid'
        ELSE 'new'
    END AS segment
FROM demo.customer_360
