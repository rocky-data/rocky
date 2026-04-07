SELECT
    order_id,
    customer_id,
    order_date,
    status,
    amount,
    region
FROM source.raw.orders
WHERE amount > 0
