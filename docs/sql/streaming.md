# Streaming SQL extensions

In order to implement features that are supported by streaming
engines, we offer a few extensions to standard SQL.

## Append-only tables

By specifying the property `'append_only' = 'true'` on a table, the
user instructs Feldera that the table will only receive `INSERT`
updates (no deletions or updates).  This type of table is frequently
used in streaming programs.  This usage pattern enables the compiler
to apply additional optimizations.

Example:

```sql
CREATE TABLE T (
   ...
) WITH (
   'append_only' = 'true'
);
```

::: warning

Currently the runtime does not enforce the fact that such tables are
append-only.  In the presence of updates or deletions in an
append-only table, the behavior of the program is unspecified.  In
particular, insertions in a table with a primary key may be
implemented as a pair insert/delete if an item with the same key
already exists.  Such tables are not append-only.

:::

## Waterlines

Waterlines are a mechanism introduced by Feldera for reducing the
memory use of streaming SQL programs.  Waterlines are an instance of a
database statistic.  Waterlines are implemented in collaboration
between the SQL compiler and the SQL runtime.

Each intermediate relation in a SQL plan may have an attached
waterline.  For a relation with schema S, a waterline is a single
tuple T having the same schema S.  Consider the following example:

```sql
CREATE TABLE T(ID INT, TS TIMESTAMP);
```

A waterline for table `T` is represented by a tuple `T_waterline(ID
INT, TS TIMESTAMP)`.

If the field `T_waterline.TS` has a value X at runtime, it indicates
that *all future updates* to table `T` will involve rows that have a
value *larger* than X in column `TS`.

The waterline of each relation is updated after each program execution
step.

The `LATENESS` annotation, described below can be used to help the
compiler infer waterlines.  The `LATENESS` directly specifies how the
waterline is computed for tables or views; starting from this
information the SQL compiler may be able to infer waterlines for other
collections.

## `LATENESS` expressions

`LATENESS` is a property of the data in a column of a table or a view
that is used for computing the table's (view's) waterline.  `LATENESS`
is described by an expression that evaluates to a constant value.  The
expression must have a type that can be subtracted from the column
type.  For example, a column of type `TIMESTAMP` may have a lateness
specified as an `INTERVAL` type.

To specify `LATENESS` for a table column, the column declaration can
be annotated in the `CREATE TABLE` statement, as in this example:

```sql
CREATE TABLE order_pickup (
   when TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES,
   location VARCHAR
);
```

To specify `LATENESS` for a view, the SQL statement `LATENESS` must be
used.  The statement specifies a view, a column of the view, and an
expression for the latness value.  For example:

```sql
LATENESS V.COL1 INTERVAL '1' HOUR;
CREATE VIEW V AS SELECT T.COL1, T.COL2 FROM T;
```

The `LATENESS` property of a column allows values that are too much
"out of order" to be ignored.  The runtime will compute the
[*waterline*](streaming.md#waterlines) of the view as the maximum of
all values ever encountered minus the specified lateness.  A value X
is "out of order" if it is below the computed waterline.  Such values
will be logged and discarded.

For example, consider the table above, and a sequence of insertions,
each as a separate transaction:

```sql
-- initially T_waterline = (-infinity, NULL)
INSERT INTO T VALUES('2020-01-01 00:00:00', 'home');
-- after this insertion T_waterline = (2019-12-12 23:00:00, NULL)

INSERT INTO T VALUES('2020-01-01 01:00:00', 'office');
-- T_waterline = (2020-01-01 00:00:00, NULL)

-- next row is late (before the previous row), but not too late (it is after the waterline)
INSERT INTO T VALUES('2020-01-01 00:10:00', 'shop');
-- T_waterline = (2020-01-01 00:00:00, NULL), unchanged

INSERT INTO T VALUES('2020-01-01 02:00:00', 'home');
-- T_waterline = (2020-01-01 01:00:00, NULL)

-- next row is too late (before the waterline), and it will be ignored
INSERT INTO T VALUES('2020-01-01 00:20:00', 'friend');
-- T_waterline = (2020-01-01 01:00:00, NULL) is unchanged
```

The second insertion is not out of order, since its timestamp is
larger than the timestamp of the first insertion.  The first insertion
sets the waterline of the table to the inserted timestamp minus one hour.

The third insertion is out of order, since its timestamp value is
smaller than the second insertion.  But the third insertion is *not*
too late, since it is only 50 minutes late, whereas the specified
column lateness is 1 hour, and thus is is before the

waterline.  The fifth row is too late, though, since it is 100 minutes
late with respect to the fourth row, and it is before the waterline
computed after the fourth row.

Lateness is used to instruct the data processing system to ignore the
rows that are too much out of order; the system behaves as if such
insertions never took place.  The computed waterline used to decide if
a value is late, is updated only *after* the current program step.  So
if all 5 insertions above are executed within one step, all five will
take place.  If they are executed as 5 steps, only the first 4 will
take place.

The `LATENESS` annotation ensures that some computed results that
reflect past data may not be updated due to very late coming data.
This also enables the runtime system to avoid maintaining very old
state, which may never impact future results.

A table or view can have any number of columns annotated with
lateness.  An inserted row is considered "too late" if any of its
annotated columns is too late.

## `emit_final` views

:::warning

The `emit_final` feature is still experimental, and it may be removed
or substantially modified in the future.

:::

By default, Feldera updates SQL views incrementally whenever new
inputs arrive. Such incremental updates include deleting previously
added rows and inserting new rows into the view.  By monitoring these
updates, the user can observe the most up-to-date results of the
computation.  However, some applications only need to observe the
final outputs of the computation, i.e., rows that are guaranteed to
never be deleted or updated.  For instance, the application may need
to know the value of an aggregate over a [tumbling
window](table.md#tumble) only after the content of the window has been
finalized.  Furthermore, some applications may not be able to handle
intermediate updates.  Typical reasons for this are:

* **The application is unable to process row deletions.** Some
    databases and database connectors cannot ingest deletions
    efficiently or at all.

* **The application cannot handle a large volume of incremental
    updates.** Frequent small input changes can generate many
    incremental output updates. These updates often cancel out, as
    rows are inserted and later deleted and replaced with new
    values. Hence the volume of intermediate updates can be much
    larger than the final output.

* **The application is only allowed to act on the final
    output**. Consider an application that raises an alarm, sends an
    email, or performs some other irreversible action based on the
    output or the view. Such actions should only be taken when the
    output is final. Having to handle intermediate outputs can
    complicate the logic of such applications, as the application may
    not be able to reliably identify and discard intermediate values.

The `emit_final` attribute instructs Feldera to only output final rows
of a view, as in the following example:

```sql
CREATE VIEW V WITH (
  'emit_final' = 'ts'
) AS SELECT ... as ts, .... FROM ...
```

This property is modeled after the Kafka [emit on
close](https://kafka.apache.org/33/javadoc/org/apache/kafka/streams/kstream/EmitStrategy.html#onWindowClose())
property.

The `emit_final` property must specify either a column name that
exists in the view, or a column number, where 0 is the leftmost view
column.

The `emit_final` annotation causes the view to emit only the rows that
have a value in the specified column that is before the view's current
[waterline](streaming.md#waterline), i.e., these rows will never be
updated.  If the compiler cannot infer a waterline for the specified
column of the view, it will emit an error at compilation time.

Currently this annotation is only allowed on views that are not
`LOCAL`.  This annotation takes effect only for the view used as
output.  If the view is used in defining other views, these derived
views will receive the non-delayed data.

Be careful when using this annotation: some kinds of computations may
require delaying outputs for very long periods of time.  Consider an
example like:

```sql
CREATE VIEW W AS
SELECT YEAR(timestamp), ... FROM T
GROUP BY YEAR(timestamp)
```

This program will need to buffer outputs for a whole year before
emitting the results.

## `WATERMARK` expressions

:::warning

The `WATERMARK` feature is still experimental, and it may be removed
or substantially modified in the future.

:::

`WATERMARK` is an annotation on the data in a column of a table that
is relevant for the case of stream processing.  `WATERMARK` is
described by an expression that evaluates to a constant value.  The
expression must have a type that can be subtracted from the column
type.  For example, a column of type `TIMESTAMP` may have a watermark
specified as an `INTERVAL` type.

To specify `WATERMARK` for a table column, the column declaration can
be annotated in the `CREATE TABLE` statement.  For example:

```sql
CREATE TABLE order_pickup (
   pickup_time TIMESTAMP NOT NULL WATERMARK INTERVAL '1:00' HOURS TO MINUTES,
   location VARCHAR
);
```

The effect of the `WATERMARK` is to delay the processing of the input
rows until they are less likely to arrive out of order with respect to
other rows.  More precisely, the system maintains the largest value
encountered so far in any input row for the columns that have a
watermark.  Given a `WATERMARK` annotation with value W, an input row
with a value X for a watermarked column will be "held up" until an
input row with a value X + W has been received.  The program will
behave as if the row with value X has only just been received.
Similar to `LATENESS`, the effect of the `WATERMARK` is visible in the
*next* circuit step; an event that changes the watermark in step 2 of
the circuit will only make "visible" the eligible events inserted up
to step 1.

If a table has multiple columns annotated with `WATERMARK` values, a
row is released only when *all* the delays have been exceeded.

:::warning

The current version of the compiler does not support multiple
`WATERMARK` columns in a single table.

:::