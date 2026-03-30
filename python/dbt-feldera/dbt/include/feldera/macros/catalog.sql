{# Catalog macro for Feldera adapter #}

{% macro feldera__get_catalog(information_schema, schemas) %}
    {% set catalog = [] %}
    {% for schema in schemas %}
        {% set schema_relation = api.Relation.create(
            database=information_schema.database,
            schema=schema,
        ) %}
        {% set relations = adapter.list_relations_without_caching(schema_relation) %}
        {% for relation in relations %}
            {% set columns = adapter.get_columns_in_relation(relation) %}
            {% for col in columns %}
                {% do catalog.append({
                    'table_database': relation.database,
                    'table_schema': relation.schema,
                    'table_name': relation.identifier,
                    'table_type': relation.type,
                    'column_name': col.name,
                    'column_index': loop.index,
                    'column_type': col.dtype,
                }) %}
            {% endfor %}
        {% endfor %}
    {% endfor %}
    {{ return(catalog) }}
{% endmacro %}
