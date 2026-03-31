{#
    Incremental IVM materialization for Feldera.

    Instead of dbt's watermark-based incremental strategy, this materialization 
    leverages Feldera's DBSP engine for automatic incremental view maintenance (IVM).

    The model SQL is installed as a standing query. DBSP automatically
    incrementalizes it. When input data changes, only affected output rows
    are recomputed.

    Configuration:
        materialized: 'incremental'
        pipeline_name: Pipeline name (defaults to schema)
        materialized_view: true/false (default true, enables ad-hoc queries)
        connectors: Optional output connector config

    On full refresh: pipeline is stopped, storage cleared, and redeployed.
#}
{% materialization incremental, adapter='feldera' %}
    {%- set pipeline_name = config.get('pipeline_name', model.schema) -%}
    {%- set view_name = model.name -%}
    {%- set materialized = config.get('materialized_view', true) -%}
    {%- set connectors = config.get('connectors', []) -%}
    {%- set full_refresh = flags.FULL_REFRESH -%}

    {# On full refresh, stop and clear the pipeline #}
    {%- if full_refresh -%}
        {{ adapter.stop_pipeline(pipeline_name) }}
    {%- endif -%}

    {# Build the CREATE [MATERIALIZED] VIEW statement #}
    {%- set view_sql -%}
        CREATE {{ 'MATERIALIZED ' if materialized else '' }}VIEW {{ view_name }}
        {%- if connectors %}
        WITH ('connectors' = '{{ connectors | tojson }}')
        {%- endif %}
        AS
        {{ sql }}
    {%- endset -%}

    {# Register the view with the pipeline state manager #}
    {{ adapter.register_view(pipeline_name, view_name, view_sql) }}

    {# Deploy the pipeline (compile + start) #}
    {{ adapter.deploy_pipeline(pipeline_name) }}

    {{ return({'relations': [this]}) }}
{% endmaterialization %}
