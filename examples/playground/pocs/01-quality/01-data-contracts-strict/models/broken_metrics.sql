-- Deliberately violates the contract:
-- 1. Drops the `customer_id` column (which is in [rules].required).
-- 2. Returns `amount` cast to VARCHAR (wrong type — contract says DECIMAL).
SELECT order_id, CAST(amount AS VARCHAR) AS amount FROM raw_orders
