{#
    View materialization for Feldera.

    Creates a view in the pipeline. By default, a non-materialized view is
    created (intermediate transform). When ``connectors`` are configured or
    ``materialized_view`` is set to ``true``, a ``CREATE MATERIALIZED VIEW``
    is emitted instead, enabling ad-hoc queries and output connectors
    (e.g., Delta Lake, Kafka).

    Configuration:
        materialized: 'view'
        pipeline_name: Pipeline name (defaults to schema)
        materialized_view: true/false (default false, auto-promoted when connectors are set)
        connectors: Optional output connector config (list of connector dicts)
#}
{% materialization view, adapter='feldera' %}
    {%- set pipeline_name = config.get('pipeline_name', model.schema) -%}
    {%- set view_name = model.name -%}
    {%- set connectors = config.get('connectors', []) -%}
    {%- set materialized = config.get('materialized_view', false) or connectors -%}

    {%- set view_sql -%}
        CREATE {{ 'MATERIALIZED ' if materialized else '' }}VIEW {{ view_name }}
        {%- if connectors %}
        WITH ('connectors' = '{{ connectors | tojson }}')
        {%- endif %}
        AS
        {{ sql }}
    {%- endset -%}

    {{ adapter.register_view(pipeline_name, view_name, view_sql) }}

    {# dbt requires a 'main' statement to be called during model execution #}
    {% call statement('main') %}
        -- Feldera: view '{{ view_name }}' registered (deployed on-run-end)
    {% endcall %}

    {{ return({'relations': [this]}) }}
{% endmaterialization %}
