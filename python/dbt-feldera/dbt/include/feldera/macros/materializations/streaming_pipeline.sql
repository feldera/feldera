{#
    Streaming Pipeline materialization for Feldera.

    Deploys an entire Feldera pipeline as a single dbt model. The model SQL
    IS the complete pipeline program — containing CREATE TABLE and CREATE VIEW
    statements. This is useful for deploying complex multi-table, multi-view
    pipelines as a single unit.

    Configuration:
        materialized: 'streaming_pipeline'
        pipeline_name: Pipeline name (defaults to model name)
        workers: Number of worker threads (default: from profile)
        compilation_profile: Compilation profile (default: from profile)
#}
{% materialization streaming_pipeline, adapter='feldera' %}
    {%- set pipeline_name = config.get('pipeline_name', model.name) -%}
    {%- set full_refresh = flags.FULL_REFRESH -%}

    {# On full refresh, stop and clear #}
    {%- if full_refresh -%}
        {{ adapter.stop_pipeline(pipeline_name) }}
    {%- endif -%}

    {#
        Parse the model SQL to extract individual CREATE TABLE and CREATE VIEW
        statements and register them with the pipeline state manager.
        The entire SQL body is treated as the pipeline program.
    #}
    {{ adapter.register_table(pipeline_name, model.name ~ '__program', sql) }}

    {# Deploy the full pipeline #}
    {{ adapter.deploy_pipeline(pipeline_name) }}

    {{ return({'relations': [this]}) }}
{% endmaterialization %}
