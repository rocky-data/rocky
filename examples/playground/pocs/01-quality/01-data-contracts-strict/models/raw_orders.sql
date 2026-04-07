SELECT
    CAST(order_id AS BIGINT) AS order_id,
    CAST(customer_id AS BIGINT) AS customer_id,
    CAST(amount AS DECIMAL(10,2)) AS amount
FROM seeds.orders
