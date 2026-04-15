{#
    Incremental materialization — unsupported.

    Feldera does not support the ``incremental`` materialization because all
    views in Feldera are natively maintained incrementally by the DBSP engine.

    Use ``materialized='view'`` with ``materialized_view=true`` instead.
#}
{% materialization incremental, adapter='feldera' %}
    {# Feldera does not need a separate incremental materialization because
       the DBSP engine incrementally maintains every view automatically.
       When input data changes, only affected output rows are recomputed —
       no watermarks, merge logic, or special configuration required. #}
    {{ exceptions.CompilationError(
        """
        dbt-feldera does not support the 'incremental' materialization,
        because all views in Feldera are natively maintained incrementally
        by the DBSP engine.

        Use materialized='view' with materialized_view=true instead:

            {{ config(materialized='view', materialized_view=true) }}

        See: https://docs.feldera.com/sql/materialized
        """
    )}}
{% endmaterialization %}
