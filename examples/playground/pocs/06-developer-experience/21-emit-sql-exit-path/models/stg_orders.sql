SELECT
    tenant,
    order_id,
    customer_id,
    region,
    amount,
    status,
    order_date
FROM raw__sales.orders
