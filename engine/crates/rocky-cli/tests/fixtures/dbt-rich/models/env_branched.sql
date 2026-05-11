SELECT
    order_id,
    customer_id,
    amount
FROM raw.orders
{% if target.name == 'prod' %}
WHERE updated_at >= '2026-01-01'
{% endif %}
