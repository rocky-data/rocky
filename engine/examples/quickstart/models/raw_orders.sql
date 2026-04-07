SELECT
    order_id,
    customer_id,
    order_date,
    status,
    amount,
    currency,
    shipping_address_id,
    created_at,
    updated_at
FROM source.raw.orders
