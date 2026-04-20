-- Existing model so `rocky ai` can ground the prompt in a real schema.
-- The ValidationContext passed to the LLM will contain this model's
-- typed columns as source material.

SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    status,
    region
FROM raw__orders.orders
