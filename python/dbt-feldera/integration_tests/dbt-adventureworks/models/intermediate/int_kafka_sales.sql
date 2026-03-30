{#
    Intermediate: Kafka sales persisted to Delta Lake.

    A straight pass-through of the kafka_sales input table, materialized
    as a Feldera view with a Delta Lake output connector.  This gives us
    a queryable Delta table of raw Kafka events.
#}
select * from {{ ref('kafka_sales') }}
