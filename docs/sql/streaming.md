# Streaming SQL extensions

In order to implement features that are supported by streaming
engines, we offer a few extensions to standard SQL.

### `LATENESS` expressions

`LATENESS` is a property of the data in a column of a table or a view
that is relevant for the case of stream processing.  `LATENESS` is
described by an expression that evaluates to a constant value.  The
expression must have a type that can be subtracted from the column
type.  For example, a column of type `TIMESTAMP` may have a lateness
specified as an `INTERVAL` type.

To specify `LATENESS` for a table column, the column declaration can
be annotated in the `CREATE TABLE` statement.  For example:

```sql
CREATE TABLE order_pickup (
   when TIMESTAMP NOT NULL LATENESS INTERVAL '1:00' HOURS TO MINUTES,
   location VARCHAR
);
```

To specify `LATENESS` for a view, our custom SQL statement `LATENESS`
must be used.  The statement specifies a view, a column of the view,
and an expression for the latness value.  The statement may appear
before or after the view declaration.  For example:

```sql
LATENESS V.COL1 INTERVAL '1' HOUR;
CREATE VIEW V AS SELECT T.COL1, T.COL2 FROM T;
```

The `LATENESS` property of a column allows values that are too much
"out of order" to be ignored.  A value X is "out of order" if the
table or view has previously has contained a row with a value Y for
the respective column, such that Y > X.  A value X is *too much out of
order* if Y - lateness > X.

For example, consider the table above, and a sequence of insertions,
each as a separate transaction:

```sql
INSERT INTO T VALUES('2020-01-01 00:00:00', 'home');
INSERT INTO T VALUES('2020-01-01 01:00:00', 'office');
-- next row is late, but not too late
INSERT INTO T VALUES('2020-01-01 00:10:00', 'shop');
INSERT INTO T VALUES('2020-01-01 02:00:00', 'home');
-- next row is too late, and it will be ignored
INSERT INTO T VALUES('2020-01-01 00:20:00', 'friend');
```

The second insertion is not out of order, since its timestamp is
larger than the timestamp of the first insertion.  The third insertion
is out of order, since its timestamp value is smaller than the second
insertion.  Buy the third insertion is *not* too late, since it is
only 50 minutes late, whereas the specified column lateness is 1 hour.
The fifth row is too late, though, since it is 100 minutes late with
respect to the fourth row.

Lateness is used to instruct the data processing system to ignore the
rows that are too much out of order; the system behaves as if such
insertions never took place.  The "current timestamp", used to decide
if a value is late, is updated only after the current program step.
So if all 5 insertions above are executed within one step, all five
will take place.  If they are executed as 5 steps, only the first 4
will take place.

The `LATENESS` annotation ensures that some computed results that
reflect past data may not be updated due to very late coming data.
This also enables the runtime system to avoid maintaining very old
state, which may never impact future results.

A table or view can have any number of columns annotated with
lateness.  An inserted row is considered "too late" if any of its
annotated columns is too late.

### `WATERMARK` expressions

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
   when TIMESTAMP NOT NULL WATERMARK INTERVAL '1:00' HOURS TO MINUTES,
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

If a table has multiple columns annotated with `WATERMARK` values, a
row is released only when *all* the delays have been exceeded.