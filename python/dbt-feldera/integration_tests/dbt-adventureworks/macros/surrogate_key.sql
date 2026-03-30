{#
    Feldera-compatible surrogate key macro.

    Replaces dbt_utils.generate_surrogate_key() with a simple CONCAT + COALESCE
    approach, since Feldera Calcite SQL does not support MD5.
    Handles single-field case where CONCAT requires at least two operands.
#}
{% macro generate_surrogate_key(field_list) %}
    {%- if field_list | length == 1 -%}
        COALESCE(CAST({{ field_list[0] }} AS VARCHAR), '_null_')
    {%- else -%}
        CONCAT(
            {%- for field in field_list %}
            COALESCE(CAST({{ field }} AS VARCHAR), '_null_')
            {%- if not loop.last %}, '|', {% endif %}
            {%- endfor %}
        )
    {%- endif -%}
{% endmacro %}
