{#
    Feldera-compatible surrogate key macro.

    Replaces dbt_utils.generate_surrogate_key() using Feldera's native MD5()
    function, which returns a 32-character hex VARCHAR.
    Fields are CAST to VARCHAR, NULL-coalesced, and concatenated with a pipe
    delimiter before hashing — matching standard dbt_utils behaviour.
#}
{% macro generate_surrogate_key(field_list) %}
    {%- if field_list | length == 1 -%}
        MD5(COALESCE(CAST({{ field_list[0] }} AS VARCHAR), '_null_'))
    {%- else -%}
        MD5(CONCAT(
            {%- for field in field_list %}
            COALESCE(CAST({{ field }} AS VARCHAR), '_null_')
            {%- if not loop.last %}, '|', {% endif %}
            {%- endfor %}
        ))
    {%- endif -%}
{% endmacro %}
