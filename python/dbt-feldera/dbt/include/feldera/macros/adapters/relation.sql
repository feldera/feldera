{# Relation DDL macros for Feldera adapter #}

{% macro feldera__drop_relation(relation) %}
    {# Remove a table or view from the pipeline state #}
    {{ adapter.drop_relation(relation) }}
{% endmacro %}

{% macro feldera__truncate_relation(relation) %}
    {# Stop pipeline and clear storage #}
    {{ adapter.truncate_relation(relation) }}
{% endmacro %}

{% macro feldera__rename_relation(from_relation, to_relation) %}
    {# Rename within pipeline SQL #}
    {{ adapter.rename_relation(from_relation, to_relation) }}
{% endmacro %}
