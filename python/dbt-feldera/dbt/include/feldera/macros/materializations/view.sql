{#
    View materialization for Feldera.

    Creates a view in the pipeline. By default, a non-materialized view is
    created (intermediate transform). When ``connectors`` are configured or
    ``stored`` is set to ``true``, a ``CREATE MATERIALIZED VIEW``
    is emitted instead, enabling ad-hoc queries and output connectors
    (e.g., Delta Lake, Kafka).

    On ``--full-refresh``, the pipeline is stopped and all stored state
    (including connector offsets) is cleared before redeployment.

    Configuration:
        materialized: 'view'
        pipeline_name: Pipeline name (defaults to schema)
        stored: true/false (default false, auto-promoted when connectors are set)
        connectors: Optional output connector config (list of connector dicts)
#}
{% materialization view, adapter='feldera' %}
    {%- set pipeline_name = config.get('pipeline_name', model.schema) -%}
    {%- set view_name = model.name -%}
    {%- set connectors = config.get('connectors', []) -%}
    {%- set stored = config.get('stored', false) or connectors -%}
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

    {# Deploy the pipeline (compile + start) #}
    {{ adapter.deploy_pipeline(pipeline_name) }}

    {{ return({'relations': [this]}) }}
{% endmaterialization %}
