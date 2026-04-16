{#
    View materialization for Feldera.

    Creates a view in the pipeline. By default, a non-materialized view is
    created (intermediate transform). When ``stored`` is set to ``true``,
    a ``CREATE MATERIALIZED VIEW`` is emitted instead, enabling ad-hoc
    queries against the view's state. Output connectors work on both plain
    and materialized views.

    On ``--full-refresh``, the pipeline is stopped and all stored state
    (including connector offsets) is cleared before redeployment.

    Configuration:
        materialized: 'view'
        pipeline_name: Pipeline name (defaults to schema)
        stored: true/false (default false)
        connectors: Optional output connector config (list of connector dicts)
#}
{% materialization view, adapter='feldera' %}
    {%- set pipeline_name = config.get('pipeline_name', model.schema) -%}
    {%- set view_name = model.name -%}
    {%- set connectors = config.get('connectors', []) -%}
    {%- set stored = config.get('stored', false) -%}
    {%- set full_refresh = flags.FULL_REFRESH -%}

    {# On full refresh, stop and clear the pipeline #}
    {%- if full_refresh -%}
        {{ adapter.stop_pipeline(pipeline_name) }}
    {%- endif -%}

    {%- set view_sql -%}
        CREATE {{ 'MATERIALIZED ' if stored else '' }}VIEW {{ view_name }}
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

    {# Deployment is deferred to the on-run-end hook (finalize_seeds),
       which merges these views with existing table DDLs from prior
       dbt seed runs via update_with_views(). #}

    {{ return({'relations': [this]}) }}
{% endmaterialization %}
