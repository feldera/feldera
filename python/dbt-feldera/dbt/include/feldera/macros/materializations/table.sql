{#
    Table materialization for Feldera.

    Creates a CREATE TABLE statement in the pipeline SQL. Tables are input
    sources in Feldera — they define the schema for data ingress (HTTP, Kafka,
    S3, etc.). The model SQL should define the column list, not a SELECT.
#}
{% materialization table, adapter='feldera' %}
    {%- set pipeline_name = config.get('pipeline_name', model.schema) -%}
    {%- set table_name = model.name -%}
    {%- set connectors = config.get('connectors', []) -%}

    {%- set table_sql -%}
        CREATE TABLE {{ table_name }} (
            {{ sql }}
        )
        {%- if connectors %}
        WITH ('connectors' = '{{ connectors | tojson }}')
        {%- endif %}
    {%- endset -%}

    {{ adapter.register_table(pipeline_name, table_name, table_sql) }}

    {{ return({'relations': [this]}) }}
{% endmaterialization %}
