{% macro star(from, relation_alias=False, except=[]) %}
    {%- set cols = adapter.get_columns_in_relation(from) -%}
    {%- for col in cols if col.column not in except -%}
        {% if relation_alias %}{{ relation_alias }}.{% endif %}{{ col.column }}{% if not loop.last %}, {% endif %}
    {%- endfor -%}
{% endmacro %}
