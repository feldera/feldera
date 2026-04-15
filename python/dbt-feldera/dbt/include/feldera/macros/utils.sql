{# Utility macros for Feldera adapter #}

{% macro feldera__current_timestamp() %}
    NOW()
{% endmacro %}

{% macro feldera__create_table_as(temporary, relation, compiled_code, language='sql') %}
    {# Feldera does not support CREATE TABLE AS SELECT. #}
    {# Tables are input schemas; views are transforms. #}
    {%- set pipeline_name = config.get('pipeline_name', model.schema) -%}
    {{ adapter.register_view(pipeline_name, relation.identifier, 'CREATE MATERIALIZED VIEW ' ~ relation.identifier ~ ' AS ' ~ compiled_code) }}
{% endmacro %}

{% macro feldera__get_create_view_as_sql(relation, sql) %}
    CREATE VIEW {{ relation.identifier }} AS
    {{ sql }}
{% endmacro %}

{% macro feldera__type_string() %}
    VARCHAR
{% endmacro %}

{% macro feldera__type_timestamp() %}
    TIMESTAMP
{% endmacro %}

{% macro feldera__type_float() %}
    {# Required dbt dispatch hook. Without this override, dbt's default
       returns FLOAT — which Feldera does not support. We return DOUBLE
       (64-bit IEEE 754). #}
    DOUBLE
{% endmacro %}

{% macro feldera__type_numeric() %}
    DECIMAL
{% endmacro %}

{% macro feldera__type_bigint() %}
    BIGINT
{% endmacro %}

{% macro feldera__type_int() %}
    INTEGER
{% endmacro %}

{% macro feldera__type_boolean() %}
    BOOLEAN
{% endmacro %}
