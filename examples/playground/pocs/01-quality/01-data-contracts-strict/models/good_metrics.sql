-- Output schema matches the contract: order_id (BIGINT), customer_id (BIGINT), amount (DECIMAL).
SELECT order_id, customer_id, amount FROM raw_orders
