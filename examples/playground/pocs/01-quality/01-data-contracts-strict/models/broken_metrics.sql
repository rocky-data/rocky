-- Deliberately violates the contract three ways:
--   1. Drops `customer_id` (in [rules].required)          -> E010 required-column-missing
--   2. `order_id` stays nullable, contract wants non-null  -> E012 nullability
--   3. `customer_id` is [rules].protected but removed      -> E013 protected-column-removed
SELECT order_id, amount FROM raw_orders
