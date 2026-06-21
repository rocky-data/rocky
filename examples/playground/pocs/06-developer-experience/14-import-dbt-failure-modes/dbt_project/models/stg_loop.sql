{{ config(materialized='view') }}
-- A {% for %} loop the no-manifest importer cannot faithfully render: the
-- regex conversion strips only the {% %} delimiters, so the loop body would
-- survive exactly once. The importer refuses this model rather than emit
-- broken SQL.
SELECT
{% for col in ['order_id', 'customer_id', 'amount'] %}
    {{ col }},
{% endfor %}
    1 AS sentinel
FROM raw.orders
