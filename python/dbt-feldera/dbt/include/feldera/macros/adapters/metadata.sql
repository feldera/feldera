{# Metadata macros for Feldera adapter #}

{% macro feldera__list_schemas(database) %}
    {# List all pipelines as schemas #}
    {% set schemas = adapter.list_schemas(database) %}
    {% set result = [] %}
    {% for s in schemas %}
        {% do result.append(s) %}
    {% endfor %}
    {{ return(result) }}
{% endmacro %}

{% macro feldera__check_schema_exists(information_schema, schema) %}
    {# Check if a pipeline exists #}
    {% set exists = adapter.check_schema_exists(information_schema.database, schema) %}
    {{ return(exists) }}
{% endmacro %}

{% macro feldera__list_relations_without_caching(schema_relation) %}
    {# List all tables and views in a pipeline #}
    {% set relations = adapter.list_relations_without_caching(schema_relation) %}
    {{ return(relations) }}
{% endmacro %}

{% macro feldera__get_columns_in_relation(relation) %}
    {# Get column metadata from pipeline schema #}
    {% set columns = adapter.get_columns_in_relation(relation) %}
    {{ return(columns) }}
{% endmacro %}
