{#
    Feldera seed macro overrides.

    Feldera seeds work differently from traditional databases:
    - A CREATE TABLE DDL is registered with the pipeline state manager
    - Deployment (compilation + start) is deferred until the on-run-end hook
    - After the pipeline is running, seed data is pushed via HTTP ingress

    These macros override dbt's default seed helpers via adapter dispatch.
#}

{% macro feldera__create_csv_table(model, agate_table) %}
    {%- set pipeline_name = model.schema -%}
    {%- set table_name = model.name -%}
    {%- set column_override = model['config'].get('column_types', {}) -%}
    {%- set quote_seed_column = model['config'].get('quote_columns', none) -%}

    {# Build column definitions from agate table schema #}
    {%- set column_defs = [] -%}
    {%- for col_name in agate_table.column_names -%}
        {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
        {%- set type = column_override.get(col_name, inferred_type) -%}
        {%- set column_name = (col_name | string) -%}
        {%- set col_def = adapter.quote_seed_column(column_name, quote_seed_column) ~ ' ' ~ type -%}
        {%- do column_defs.append(col_def) -%}
    {%- endfor -%}

    {# Quote the table name to handle reserved words like 'date' #}
    {%- set table_sql -%}
        CREATE TABLE "{{ table_name }}" (
            {{ column_defs | join(', ') }}
        )
    {%- endset -%}

    {# Register the table DDL — deployment is deferred to on-run-end #}
    {{ adapter.register_table(pipeline_name, table_name, table_sql) }}

    {{ return(table_sql) }}
{% endmacro %}


{% macro feldera__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {%- set pipeline_name = model.schema -%}

    {% if full_refresh %}
        {{ adapter.stop_pipeline(pipeline_name) }}
    {% endif %}

    {% set sql = feldera__create_csv_table(model, agate_table) %}
    {{ return(sql) }}
{% endmacro %}


{% macro feldera__load_csv_rows(model, agate_table) %}
    {%- set pipeline_name = model.schema -%}
    {%- set table_name = model.name -%}
    {%- set column_types = model['config'].get('column_types', {}) -%}

    {# Stash data for deferred push after pipeline deployment #}
    {{ adapter.stash_seed(pipeline_name, table_name, agate_table, column_types) }}

    {{ return("-- seed data stashed for deferred push to " ~ pipeline_name ~ "." ~ table_name) }}
{% endmacro %}


{% macro feldera__get_csv_sql(create_or_truncate_sql, insert_sql) %}
    {{ create_or_truncate_sql }}
{% endmacro %}
