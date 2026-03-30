{# Schema (pipeline) DDL macros for Feldera adapter #}

{% macro feldera__create_schema(relation) %}
    {# Create pipeline is deferred until deployment #}
    {{ adapter.create_schema(relation) }}
{% endmacro %}

{% macro feldera__drop_schema(relation) %}
    {# Drop (delete) a pipeline #}
    {{ adapter.drop_schema(relation) }}
{% endmacro %}
