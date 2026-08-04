# Time-Series Extensions

This section lists SQL extensions supported by Feldera for computing over
time-series data.  A time series is a sequence of events, such as IoT sensor
readings or financial transactions, where each event is associated with one or
more timestamps.

Refer to the
[guide on time series analysis with Feldera](/tutorials/time-series)
for a detailed description of these constructs and their usage.

## `LATENESS` expressions

Lateness is a constant bound associated with a timestamp column in a
table or view, such that updates to the table are not allowed to arrive more than
lateness time units out of order.

See the [Time Series Analysis Guide](/tutorials/time-series#timestamp-columns-and-lateness)
for details.

<!-- ## `WATERMARK` expressions

:::warning

The `WATERMARK` feature is still experimental, and it may be removed
or substantially modified in the future.

:::

`WATERMARK` is an annotation on a column of a table that delays the processing
of the input rows by a constant amount of time.

See the [Time Series Analysis Guide](/tutorials/time-series#delaying-inputs-with-watermark)
for details. -->

## `append_only` tables

The `append_only` annotation on a table instructs Feldera that the table will
only receive `INSERT` updates.

See the [Time Series Analysis Guide](/tutorials/time-series#append-only-tables)
for details.

## `emit_final` views

:::warning

The `emit_final` feature is still experimental, and it may be removed
or substantially modified in the future.

:::

The `emit_final` annotation on a view instructs Feldera to only output its final rows,
i.e., rows that are guaranteed to never get deleted or updated.

See the [Time Series Analysis Guide](/tutorials/time-series#emitting-final-values-of-a-view-with-emit_final)
for details.

## Soft deletes with temporal filters

An input connector can be configured with the [`soft_delete`](/connectors#soft_delete)
property to transform deletions into insertions; in this case the `is_delete` metadata
attribute records the kind of change (insert/delete), essentially converting a table
into a log.  Note that the table does *not* declare a `PRIMARY KEY` column, although
the data may contain one.  Since the connector transforms every change into an
insertion, the table only receives insertions, so it can be declared
[`append_only`](#append_only-tables), which enables additional optimizations.

The [Soft deletes](/connectors#soft-deletes) section shows how one can write a query
to recover the current contents of the table from this log: group the changes
on the columns forming the primary key, rank them by time, keep the latest one,
and return it only when it is an insertion.  That query returns one row for each
primary key, but it must remember every change in the log, so its state grows
without bound.

However, in some cases only a bounded window of the table is necessary for
computing the desired results.  When a
[temporal filter](/tutorials/time-series#now-and-temporal-filters) can be used
to describe the window, the entire computation can be performed using finite
state, by sequencing the computation as follows:

```
[connector with soft deletes] -> [temporal filter] -> [reconstruct table] -> [views]
```

The following program reconstructs only the recent contents
of a change stream while never storing more than the last seven
days of changes:

```sql
-- The 'soft_delete' connector property converts this
-- table into a log of changes to the table.
CREATE TABLE input_log (
    id BIGINT, -- not declared as primary key
    s VARCHAR,
    ts TIMESTAMP,
    -- Is the change a deletion?  Produced by the connector
    is_delete BOOLEAN DEFAULT CAST(CONNECTOR_METADATA()['is_delete'] AS BOOLEAN)
) WITH (
    -- A soft-delete table only receives insertions
    'append_only' = 'true',
    'connectors' = '[{
        "name": "changes",
        "soft_delete": true,
        "transport": {
            "name": "kafka_input",
            "config": {
                "topic": "changes",
                "start_from": "earliest",
                "bootstrap.servers": "example.com:9092",
                "include_timestamp": true
            }
        },
        "format": {
            "name": "json",
            "config": { "update_format": "insert_delete" }
        }
    }]'
);

-- Contains only changes to 'input_log' from the last 7 days
CREATE LOCAL VIEW recent AS
SELECT * FROM input_log
WHERE ts >= NOW() - INTERVAL 7 DAYS AND ts <= NOW();

-- The contents of the 'input' table limited to the last 7 days
CREATE LOCAL VIEW input AS
SELECT id, s, ts
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY id ORDER BY ts DESC, is_delete NULLS FIRST
    ) AS rn
    FROM recent
)
WHERE rn = 1 AND is_delete IS NOT TRUE;

-- Rolling aggregates over the reconstructed table: for each record,
-- the number of records with a timestamp in the preceding minute, hour, and day.
CREATE VIEW input_stats AS
SELECT
    id, s, ts,
    COUNT(*) OVER minute_window AS rows_last_minute,
    COUNT(*) OVER hour_window AS rows_last_hour,
    COUNT(*) OVER day_window AS rows_last_day
FROM input
WINDOW
    minute_window AS (ORDER BY ts RANGE BETWEEN INTERVAL 1 MINUTE PRECEDING AND CURRENT ROW),
    hour_window AS (ORDER BY ts RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW),
    day_window AS (ORDER BY ts RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW);
```

The view `input_stats` consumes the reconstructed table using
[rolling aggregates](/tutorials/time-series#rolling-aggregates) over three
shorter intervals.  The aggregates see the reconstructed table rather than the
log, so a deleted record stops contributing to the counts as soon as its
deletion arrives.

Two details of the query matter for correctness:

* The `is_delete` term in the `ORDER BY` clause ranks an insertion ahead of a
  deletion that carries the same timestamp, which keeps the new value of a
  record that a CDC stream updates with a single delete-insert message pair.
  See [Soft deletes](/connectors#soft-deletes) for details.

* The polarity filter `is_delete IS NOT TRUE` must be outside the subquery
  that ranks the changes. Filtering out the deletions before ranking would
  "resurrect" the previous insertion of a deleted key.
