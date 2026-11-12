SELECT
    p.payment_id,
    p.order_id,
    o.customer_id,
    p.method,
    p.paid_at
FROM raw.payments AS p
INNER JOIN stg_orders AS o ON o.order_id = p.order_id
