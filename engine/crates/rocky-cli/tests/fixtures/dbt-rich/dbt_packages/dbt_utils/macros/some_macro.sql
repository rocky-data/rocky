{% macro some_macro(col) %}
    coalesce({{ col }}, '')
{% endmacro %}
