{#
    View materialization for Feldera.

    Creates a non-materialized view in the pipeline. The view exists as part
    of the DBSP dataflow but does not persist its output for ad-hoc queries.
    Suitable for intermediate transformations.
#}
{% materialization view, adapter='feldera' %}
    {%- set pipeline_name = config.get('pipeline_name', model.schema) -%}
    {%- set view_name = model.name -%}

    {%- set view_sql -%}
        CREATE VIEW {{ view_name }} AS
        {{ sql }}
    {%- endset -%}

    {{ adapter.register_view(pipeline_name, view_name, view_sql) }}

    {# dbt requires a 'main' statement to be called during model execution #}
    {% call statement('main') %}
        -- Feldera: view '{{ view_name }}' registered (deployed on-run-end)
    {% endcall %}

    {{ return({'relations': [this]}) }}
{% endmaterialization %}
