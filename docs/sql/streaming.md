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

## `WATERMARK` expressions

:::warning

The `WATERMARK` feature is still experimental, and it may be removed
or substantially modified in the future.

:::

`WATERMARK` is an annotation on a column of a table that delays the processing
of the input rows by a constant amount of time.

See the [Time Series Analysis Guide](/tutorials/time-series#delaying-inputs-with-watermark)
for details.

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