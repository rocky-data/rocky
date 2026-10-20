-- Plain SELECT over a bare source table, so it is skip-eligible.
-- run.sh edits this file to simulate a change.
SELECT order_id, customer_id, amount, order_date
FROM raw__shop.orders
