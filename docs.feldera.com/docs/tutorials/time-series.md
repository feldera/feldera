# Time Series Analysis with Feldera

This guide covers concepts required to effectively process **time series**
data with Feldera.  A time series is a sequence of events, such as IoT sensor
readings or financial transactions, where each event is associated with one or
more timestamps.  Time series data is inherently dynamic, changing continuously
as new events arrive in real-time.  Feldera is designed to
compute over changing data, making it well-suited for time series
analytics.

:::tip

Most examples in this guide are available as a pre-packaged demo "Time Series Analysis with Feldera"
that ships with the
[Feldera Docker container](https://docs.feldera.com/get-started/docker) and the
[online sandbox](https://try.feldera.com).

:::

At a high-level, Feldera allows users to efficiently compute on time series data by
providing [`LATENESS`](#timestamp-columns-and-lateness) annotations on tables and views. These
annotations describe the datasource and tell Feldera the maximum out-of-orderness in the data.
For example, it allows users to convey to Feldera a hint of the form "I know that data from my
IoT sensor will never get delayed by more than a day". Feldera takes these hints and automatically
decides when it is safe to drop state that won't affect the output of any of the views. **This allows
Feldera to evaluate various classes of queries on infinite streams of data (like telemetry data)
with finite memory, making it extremely resource efficient.**

:::warning

Feldera does not automatically garbage collect [materialized tables and views](/sql/materialized).
If such a table stores an unbounded time series, it will continue to consume storage without limit.
In addition, if the table has a primary key (PK), Feldera will maintain a PK index for this table,
which will only be garbage collected if at least one of the PK columns has a `LATENESS` annotation.

:::

:::warning

Feldera implements an `INSERT` or `UPDATE` in a table with a primary
key as a pair of `INSERT` and `DELETE` operations: a new record is
inserted, and the old record with the same primary key (if present) is
deleted.  For tables with `LATENESS` this may cause surprising
effects: the `DELETE` needs to delete the previous version of the
record, with the old timestamp; if this timestamp is behind the
`LATENESS` threshold, the entire INSERT or UPDATE is considered late
and is thus ignored.  This effectively means that "old" records in
such a table can never be updated or deleted.

:::

Users can further take advantage of `LATENESS` annotations to control
**when** the output of a query is produced by Feldera using
[`emit_final`](#emitting-final-values-of-a-view-with-emit_final)
annotations.

In the following sections, we will explain lateness and other key concepts with
which users can efficiently compute over timeseries data.

The last part of this guide lists [SQL patterns](#sql-for-time-series-analytics)
frequently used in time series analysis. Feldera supports garbage collection for
most of these patterns, meaning that they can be evaluated efficiently using
bounded memory.

## Timestamp columns and lateness

A timestamp column is a column in a SQL table or view that indicates a specific
point in time associated with each row.

  * There can be any number of timestamp columns in a table. For instance, a
    table that describes taxi rides can store the time when the ride order was
    received, pickup time, dropoff time, and the time when the payment was
    processed.

  * Timestamp columns can have any temporal (`TIMESTAMP`, `DATE`, `TIME`) or
    numeric type.

**A timestamp column can grow monotonically or almost monotonically**. Time
series data often arrives ordered by timestamp, i.e., every event has the same
or larger timestamp than the previous event.  In some cases, events can get
reordered and delayed, but this delay is bounded, e.g., it may not exceed 1
hour.  We refer to this bound as **lateness**.

**Lateness** is a constant bound `L` associated with a timestamp column in a
table or view, such that if the table contains a record with timestamp equal to
`TS`, then any future records inserted to or removed from the table will have
timestamps greater than or equal to `TS - L`.  In other words, updates to the
table cannot arrive more than `L` time units out of order.
A lateness value of zero indicates that updates to the table arrive strictly in
order, with each new update having a timestamp equal to or later than the previous one.

The Feldera query engine uses lateness information to optimize the execution of
queries.

### Specifying lateness for tables and views

To specify lateness for a table column, the column declaration is annotated with
the `LATENESS` attribute:

```sql
CREATE TABLE purchase (
   customer_id INT,
   ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOURS,
   amount BIGINT
);
```

The value of the `LATENESS` attribute is a constant expression that must have a
type that can be subtracted from the column type.  In the above example,
lateness for a column of type `TIMESTAMP` is specified as an `INTERVAL` type.
`LATENESS` for an integer column is specified as an integer constant.

**Views can have lateness too**. Lateness is a property of input data; however
it is not always possible to associate a lateness annotation with an input table
column.  For instance, input data can arrive as JSON strings, which must be
[parsed](/sql/json/#parse_json) to extract the actual payload fields, including
timestamps.  In such situations, columns with lateness appear in intermediate
views.  To specify lateness for a view, the `LATENESS` statement must be used.
The statement specifies a view, a column of the view, and an expression for the
lateness value.  The statement may appear before or after the view declaration:

```sql
CREATE VIEW v AS SELECT t.col1, t.col2 FROM t;
LATENESS v.col1 INTERVAL 1 HOUR;
```

`LATENESS` can only be specified for toplevel view columns; i.e., the following
is illegal: `LATENESS v.str.col INTERVAL 1 HOUR`.  `LATENESS` cannot be specified
for a [recursive view](/sql/recursion).

When the compiler infers [waterlines](#the-data-retention-algorithm)
for the input collection that feeds a VIEW, it combines them with the
specified `LATENESS`, taking the larger waterline for each column.

### Guidelines for writing lateness annotations

Keep in mind the following guidelines when choosing lateness annotations for
tables and views:

* **Lateness is NOT time-to-live**.  Lateness should not be confused with
  time-to-live annotations used by some stream processing systems.  Declaring
  a column with `LATENESS` 1 hour does not mean that the data will be discarded
  after an hour. The compiler decides what data to store and for how long by
  analyzing the entire SQL program.  `LATENESS` annotations simply inform this
  analysis.

* **LATENESS does not affect the output of the program**.  Assuming lateness
  annotations are accurate, i.e., input records do not arrive more than lateness
  time units out of order, the output of the program with lateness annotations
  is guaranteed to be identical to outputs of the same program without annotations.

* **LATENESS does NOT delay computation**.  Feldera does not delay computation
  until all out-of-order records have been received.  It always computes the
  output of the queries given all inputs received so far and incrementally updates
  these outputs as new out-of-order inputs arrive.

* **Inputs that violate lateness are discarded.**  When a program receives a record
  that is more than lateness time units behind the most recent timestamp value
  previously observed in the same column, the pipeline will drop such a record.

We recommend choosing conservative `LATENESS` values, leaving sufficient time for any
delayed inputs to arrive.  In practice, this may require accounting for potential
upstream failures, such as an IoT device losing cloud connectivity.

### Lateness example

Consider the `purchase` table declared above with 1-hour lateness on the `ts` column
and the following sequence of inserts into this table:

```sql
INSERT INTO purchase VALUES(1, '2020-01-01 00:00:00', 10);

INSERT INTO purchase VALUES(1, '2020-01-01 01:00:00', 20);

-- Late row, but within the lateness bound.
INSERT INTO purchase VALUES(1,'2020-01-01 00:10:00', 15);

INSERT INTO purchase VALUES(1, '2020-01-01 02:00:00', 12);

-- Late row that violates lateness.
INSERT INTO purchase VALUES(1, '2020-01-01 00:20:00', 65);
```

The second insertion arrives in order, since its timestamp is
larger than the timestamp of the first insertion.  The third insertion
is out of order, since its timestamp is smaller than the second
insertion.  But it does not violate lateness, since it is
only 50 minutes late, whereas the specified lateness is 1 hour.
The fifth row violates lateness, since it is 100 minutes late with
respect to the fourth row, and will be discarded.

Note that lateness is a soft bound.  A pipeline is guaranteed to
accept any records that arrive within the bound; but can also accept
late records for a short period of time.  The reason for this is that
Feldera ingests input records in chunks and advances the cutoff timestamp,
below which inputs are discarded, after processing the whole chunk.

## How Feldera garbage collects old state

Consider the `purchase` table from before.  As new purchase records are added
over time, the table will keep growing.  How much of its history do we need to
store?  A conventional database, serving as a system of record, must store every
record added to it, until it is deleted by the user, so that it can answer
queries over this data on-demand.  In contrast, the Feldera query engine
incrementally maintains a set of views defined ahead of time and
only keeps state needed by those views.

:::note

Feldera also supports on-demand queries (also known as **ad hoc queries**) for
materialized tables and views.  See [documentation](/sql/materialized#inspecting-materialized-tables-and-views) for details.

:::

Let’s assume the `purchase` table is used to compute a single view that tracks
the daily maximum purchase amount:

```sql
CREATE VIEW daily_max AS
SELECT
    TIMESTAMP_TRUNC(ts, DAY) AS d,
    MAX(amount) AS max_amount
FROM
    purchase
GROUP BY
    TIMESTAMP_TRUNC(ts, DAY);
```

If the `purchase` table did not have the `LATENESS` annotation,
the query engine would assume that records can be inserted and deleted
with arbitrary timestamps, e.g., it would be possible to delete a year-old
record.  Deleting an old record requires re-computing the daily maximum
for the corresponding date.  This in turn requires storing the entire
purchase history permanently.

**We can do better with `LATENESS`**.  The `LATENESS` annotation guarantees
that changes to the table cannot get delayed by more than one hour relative
to each other.  Hence, daily maxima can only change for the current and, possibly,
previous day.  Since we will never need to reevaluate the query for earlier dates,
we do not need to store older data.

### The data retention algorithm

To explain how Feldera discards unused state, we need to introduce a new concept:

**Waterline** of a relation with respect to a timestamp column is a timestamp value
`W`, such that all future additions or deletions in the relation will have timestamps
greater than or equal to `W`.

Feldera uses the following procedure to identify and discard unused data:

1. **Compute waterlines of all program relations**. Starting from user-supplied
   `LATENESS` annotations, Feldera identifies all tables and views in the program
   that produce outputs monotonic in the input timestamp and computes
   waterlines for them.

2. **Compute state retention bounds**. For each view in the program, the query engine
   determines how much state it needs to maintain in order to evaluate the view incrementally.
   Some queries, such as simple projections and filters, do not require keeping any state.
   Others, such as joins and aggregates, may require storing the state of their input
   relations.  In this case, the query engine determines **how long the state
   should be retained** based on the semantics of the query and the waterlines
   computed at the previous step.

3. **Discard old records**. Feldera continuously runs background jobs that garbage
   collect old records that fall below retention bounds.

**Not all queries support discarding old inputs**. We list the operators for which
Feldera implements garbage collection [below](#sql-for-time-series-analytics).

**Relations can have multiple waterlines**. The query engine can derive waterlines
for multiple columns in a table or view. All these waterlines can be utilized for
garbage collection. For example, if the `purchase` table included an additional
timestamp column that recorded the time when payment was received, and this column
had its own `LATENESS` attribute, we could create a view computing daily totals
based on this column, and garbage collection would work for this view as well.

**Discarding old records is not equivalent to deleting them**. These are
fundamentally different operations.  Deleting a record means that any outputs
derived from it should be updated.  For example, deleting a year-old purchase
requires recomputing the `daily_max` query for the corresponding date.
In contrast, a discarded record is conceptually still part of the relation;
however it will not participate in computing any future incremental updates
and therefore does not need to be stored.

## Emitting final values of a view with `emit_final`

:::warning

The `emit_final` feature is still experimental, and it may be removed
or substantially modified in the future.

:::

By default, Feldera updates SQL views incrementally whenever new
inputs arrive. Such incremental updates include deleting existing
rows and inserting new rows into the view.  By monitoring these
updates, the user can observe the most up-to-date results of the
computation.  However, some applications only need to observe their
final outputs, i.e., rows that are guaranteed to
never get deleted or updated.

Consider the following query that computes the sum of all purchases
for each day.

```sql
CREATE VIEW daily_total AS
SELECT
    TIMESTAMP_TRUNC(ts, DAY) AS d,
    SUM(amount) AS total
FROM
    purchase
GROUP BY
    TIMESTAMP_TRUNC(ts, DAY);
```

Suppose that a new `purchase` record arrives every second.  In response,
Feldera updates the `daily_total` value for the current date,
deleting the old value and inserting a new one.  The user may observe
86400 such updates per day.  In some cases this is precisely what the user
wants--to keep track of the latest value of the aggregate with low latency.
But if the application only requires the final value of the aggregate at
the end of the day, handling all the intermediate updates may introduce
unnecessary overheads. Some applications may not be able to handle
intermediate updates at all.  Typical reasons for this are:

* **The application is unable to process row deletions**. Some
    databases and database connectors cannot ingest deletions
    efficiently or at all.

* **The application cannot handle a large volume of incremental
    updates.**  As we see in this example, the volume of intermediate
    updates can be much larger than the final output, potentially
    overwhelming the application.

* **The application is only allowed to act on the final
    output**. Consider an application that sends an
    email, approves a stock trade or performs some other irreversible
    action based on the output or the view. Such actions should only
    be taken when the output is final. Handling intermediate outputs can
    complicate the logic of the application, as the application may
    not be able to reliably identify and discard intermediate values.

The `emit_final` attribute instructs Feldera to only output final rows
of a view, as in the following example:

```sql
CREATE VIEW daily_total_final
WITH ('emit_final' = 'd')
AS
SELECT
    TIMESTAMP_TRUNC(ts, DAY) AS d,
    SUM(amount) AS total
FROM
    purchase
GROUP BY
    TIMESTAMP_TRUNC(ts, DAY);
```

The `emit_final` annotation causes the view to emit only the rows that
have a value in the specified column that is before the view's current
waterline.  Rows whose value in this column is `NULL` are never emitted:
a `NULL` value is never late, so such a row can still be deleted.

Let us insert some records in the `purchase` table and
observe how this affects the waterlines and the output of the view
with and without `emit_final` annotations.

| `INSERT INTO purchase`                 | Output without `emit_final`                     | Output with `emit_final`    |
|----------------------------------------|-------------------------------------------------|-----------------------------|
| `VALUES(1, '2020-01-01 01:00:00', 10)` | `insert: {"d":"2020-01-01 00:00:00","total":10}`|                             |
| `VALUES(1, '2020-01-01 02:00:00', 10)` | `delete: {"d":"2020-01-01 00:00:00","total":10}`|                             |
|                                        | `insert: {"d":"2020-01-01 00:00:00","total":20}`|                             |
| `VALUES(1, '2020-01-02 00:00:00', 10)` | `insert: {"d":"2020-01-02 00:00:00","total":10}`|                             |
| `VALUES(1, '2020-01-02 01:00:00', 10)` | `delete: {"d":"2020-01-02 00:00:00","total":10}`| `insert: {"d":"2020-01-01 00:00:00","total":20}`|
|                                        | `insert: {"d":"2020-01-02 00:00:00","total":20}`|                             |

* Without `emit_final`, every input update produces an output update,
  deleting any outdated records and inserting new records instead.

* With `emit_final` only outputs below the current waterline are
  produced. In this example, the waterline of the `daily_total.d`
  timestamp column remains at `2020-01-01 00:00:00` until the last
  input, which adds a record with timestamp `2020-01-02 01:00:00`.
  Since `purchase.ts` has `LATENESS` of 1 hour, no new updates for
  the previous date can be received after this.  The waterline
  moves forward by one day and the final value for `2020-01-01` is output.

The `emit_final` property must specify either a column name that
exists in the view, or a column number, where 0 is the leftmost view
column.

Note that using `emit_final` can significantly delay the output of a view.
In the example above, the aggregation is performed over a 1-day window,
resulting in a 1-day delay before the output is produced. If the aggregation
window were extended to 1 year, the view would not produce any outputs for
an entire year.

The use of `emit_final` is subject to the following restrictions:

* If the query engine cannot infer a waterline for the specified
  column of the view, it will emit an error at compilation time.

* Currently this annotation is only allowed on views that are not
  [`LOCAL`](/sql/grammar/#creating-views).  It takes effect only
  for the view used as output.  If the view is used in defining
  other views, these derived views will receive the non-delayed data.

## Append-only tables

Time series tables are often append-only: once an event has been received, it
cannot be modified or deleted.  Feldera is able to leverage this property to
optimize certain types of queries. Consider the `daily_max` view from above:

```sql
CREATE VIEW daily_max AS
SELECT
    TIMESTAMP_TRUNC(ts, DAY) AS d,
    MAX(amount) AS max_amount
FROM
    purchase
GROUP BY
    TIMESTAMP_TRUNC(ts, DAY);
```

Maintaining the `MAX` aggregate incrementally requires permanently storing the
entire contents of the `purchase` table (ignoring the `LATENESS` annotation on
`purchase.ts`).  However, if the table is append-only, then the `MAX` aggregate
can only increase monotonically as new records are added to the table, and we
only need to store the current largest value for each 1-day window.

The `append_only` annotation on a table instructs Feldera that the table will
only receive `INSERT` updates, enabling this and other optimizations:

```sql
CREATE TABLE purchase (
   customer_id INT,
   ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOURS,
   amount BIGINT
) WITH (
    'append_only' = 'true'
);
```

Note that `append_only` annotations are only supported for tables; however
the Feldera query engine automatically derives it for views that depend on
append-only tables.

## SQL for time series analytics

In this section, we present a catalog of SQL operators and patterns frequently used in
time series analysis. Although these operators are applicable to both time-series and
non-time-series data, they are particularly beneficial when working with time series.

Feldera offers efficient incremental implementations of these constructs and
supports garbage collection for most of them, meaning that they can be evaluated
efficiently in both time and space.

### Aggregating Time Series Data Over Fixed-Size Time Intervals

A common practice in time series analysis is to segment the timeline into fixed-size
intervals—such as minutes, hours, days, or years—and aggregate the data within each
segment. Earlier, we encountered two examples of such queries: `daily_max` and
`daily_sum`, which apply the `MAX` and `SUM` aggregation functions over 1-day
intervals.

#### Garbage collection

The incremental implementation of aggregation operators stores both
the input and output of the operator.  Feldera applies various optimizations to
optimize storage, for example linear aggregation functions, such as
`SUM`, `COUNT`, `AVG`, `STDDEV`, do not require storing the input relation;
`MIN` and `MAX` aggregates over [append-only](#append-only-tables) collections
only store one value per group, etc.  On top of these optimizations, the GC
mechanism can discard old records under the following conditions:

* Old input records get discarded if at least one of the expressions in the
  `GROUP BY` clause is a monotonic function of a timestamp column, for which the query
  engine can establish a waterline.  For instance, in the `daily_max` view,
  `TIMESTAMP_TRUNC` is a monotonic function over the `purchase.ts` column.

* Old output records get discarded if at least one of the output columns
  has a waterline.  In the `daily_max` view, the `TIMESTAMP_TRUNC(ts, DAY) as d`
  column has a waterline.

* The `MIN` of a column with a waterline has the same waterline.  This holds
  only when every aggregate in the same `GROUP BY` is a `MIN` over that same
  column; adding a `MAX`, a `MIN` over a different column, or a `FILTER`
  clause loses the waterline.

### Tumbling and hopping windows

[Tumbling](/sql/table/#tumble) and [hopping](/sql/table/#tumble) window operators
provide a more idiomatic way to slice the timeline into non-overlapping or
overlapping windows.  These operators do not maintain any state and therefore do
not require GC.  The `daily_max` view can be expressed using the `TUMBLE` operator
as follows:

```sql
CREATE VIEW daily_max_tumbling AS
SELECT
    window_start,
    MAX(amount)
FROM TABLE(
  TUMBLE(
    "DATA" => TABLE purchase,
    "TIMECOL" => descriptor(ts),
    "SIZE" => INTERVAL 1 DAY))
GROUP BY
    window_start;
```

### Rolling aggregates

[Rolling aggregates](https://www.feldera.com/blog/rolling-aggregates) offer a
different way to define time windows. For each data point in a time series, they
compute an aggregate over a fixed time frame (such as a day, an hour, or a
month) preceding this data point.  Rolling aggregates can be expressed in SQL using
[window aggregate functions](/sql/aggregates#window-aggregate-functions).
Here is a version of `daily_max` that computes `MAX` over the 1-day window
preceding each event:

```sql
CREATE VIEW daily_max_rolling AS
SELECT
    ts,
    amount,
    MAX(amount) OVER window_1_day
FROM purchase
WINDOW window_1_day AS (ORDER BY ts RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW);
```

Rolling aggregates provide an accurate summary of recent event history for each data
point in the dataset. They are more expensive than
[tumbling and hopping windows](#tumbling-and-hopping-windows), since they instantiate as many
individual windows as there are data points in the table.
Feldera features an efficient incremental implementation of the rolling aggregate operator,
that is capable of evaluating millions of individual windows per second on
a laptop.

#### Garbage collection

GC for rolling aggregates works similarly to regular aggregates (see [above](#aggregating-time-series-data-over-fixed-size-time-intervals)):

* Old input records are discarded if the `ORDER BY` expression is a monotonic function
  of a timestamp column, for which the query engine can establish a waterline.  For
  instance, in the `daily_max_rolling` view above, the `purchase.ts` column, used in the
  `ORDER BY` clause, has a waterline.

* Old output records get discarded if at least one of the output columns
  has a waterline.  In the `daily_max_rolling` view, the `ts` column copied
  from the input table has a waterline.

### Join over timestamp columns

The following query computes daily totals over the `purchase` and `returns`
tables and joins the results by date in order to construct a daily summary
combining both totals for each given day in a single view:

```sql
CREATE VIEW daily_totals AS
WITH
    purchase_totals AS (
        SELECT
            TIMESTAMP_TRUNC(purchase.ts, DAY) AS purchase_date,
            SUM(purchase.amount) AS total_purchase_amount
        FROM purchase
        GROUP BY
            TIMESTAMP_TRUNC(purchase.ts, DAY)
    ),
    return_totals AS (
        SELECT
            TIMESTAMP_TRUNC(returns.ts, DAY) AS return_date,
            SUM(returns.amount) AS total_return_amount
        FROM returns
        GROUP BY
            TIMESTAMP_TRUNC(returns.ts, DAY)
    )
SELECT
    purchase_totals.purchase_date AS d,
    purchase_totals.total_purchase_amount,
    return_totals.total_return_amount
FROM
    purchase_totals
    FULL OUTER JOIN
    return_totals
ON
    purchase_totals.purchase_date = return_totals.return_date;
```

#### Garbage collection

The join operator stores both of its input relations.  If at least one pair of
columns being joined on has waterlines (in this case, both `purchase_totals.purchase_date`
and `return_totals.return_date` have waterlines), the operator discards old records
below the smaller of the two waterlines.

### As-of joins

The [`ASOF JOIN`](https://www.feldera.com/blog/asof-join) operator is a specialized
type of JOIN used between two relations with comparable timestamp columns. It is
used to answer questions such as "What was the stock price at
the exact moment of the transaction?" or "What was the account balance just before
the money transfer?"

For each row in the left input relation, the as-of join operator identifies at most
one row in the right input with the closest timestamp that is no greater than the
timestamp in the left row. The example below demonstrates how an as-of join can be
used to extract the customer’s address at the time of purchase from the `customer`
table.

```sql
CREATE VIEW purchase_with_address AS
SELECT
    purchase.ts,
    purchase.customer_id,
    customer.address
FROM purchase
LEFT asof JOIN customer match_condition(purchase.ts >= customer.ts)
ON purchase.customer_id = customer.customer_id;
```

#### Garbage collection

The incremental as-of join operator stores both of its input relations.  If both
timestamp columns in the `MATCH_CONDITION` have waterlines, the operator will

* discard records below the smaller of the two waterlines from the left collection
* discard all except the last record below the smaller of the two waterlines for each join key
  from the right collection.

### `LAG` and `LEAD`

[`LAG` and `LEAD`](/sql/aggregates#window-aggregate-functions) SQL window functions allow access
to a previous or following row in a relation.  When used with a window ordered by a timestamp
column, they refer to earlier or later data points in a time series. The following view computes
previous and next purchase amounts for each record in the `purchase` table:

```sql
CREATE VIEW purchase_with_prev_next AS
SELECT
    ts,
    customer_id,
    amount,
    LAG(amount) OVER(PARTITION BY customer_id ORDER BY ts) AS previous_amount,
    LEAD(amount) OVER(PARTITION BY customer_id ORDER BY ts) AS next_amount
FROM
    purchase;
```

#### Garbage collection

GC for `LAG` and `LEAD` functions has not been implemented yet (see our
GC feature [roadmap](https://github.com/feldera/feldera/issues/1850)).
[Let us know](https://github.com/feldera/feldera/issues/new/) if you are interested in this feature.

### Deduplicating a CDC stream

A change data capture connector can deliver the same change more than once: it
replays part of a partition after a restart, or a producer retries.  The task in
this section is to implement a SQL program which preserves the first version of
each change, and ignores the copies that follow it.  Moreover, we want to
minimize memory use: do not hold all the information in a change, but only
enough information to uniquely identify the change, and discard this information
when we know more copies of the change cannot arrive.

The changes come from this table, which lives in the database and is not part of
the Feldera program:

```sql
CREATE TABLE orders (
    order_id VARCHAR NOT NULL PRIMARY KEY,
    amount DECIMAL(12, 2),
    status VARCHAR
    -- additional columns omitted
);
```

The program assumes that the source database records the time of every change to
a row, and the position of the change in its log.  Each change is uniquely
identified by the primary key, a record time and a log position.  The CDC stream
from the database exposes this information.

#### Feldera's table: the CDC stream from the database

The program assumes the following about the CDC stream of changes:

* The original table has inserts, updates and deletes.  Each of those changes
  appends one message to the CDC stream, so the stream itself is append-only.
* A change is uniquely identified by the key, the record time and the log
  position.  A change can appear multiple times in the CDC log.
* Any two different copies of a change arrive within a bounded time of each
  other.
* The first copy of a change is the copy that entered Kafka first.
* A change that deletes a row carries its values in `before`, every other change
  in `after`.

The stream `orders_cdc` simulates the way a CDC stream would look for an
`orders` table.  Its shape follows the change event that
[Debezium](https://debezium.io/documentation/reference/) produces, e.g., for
[PostgreSQL](https://debezium.io/documentation/reference/stable/connectors/postgresql.html):

```sql
CREATE TABLE orders_cdc (
    -- The row before the change, NULL for an insert.  Same columns as after
    before ROW(
        order_id VARCHAR NULL,
        amount DECIMAL(12, 2) NULL,
        status VARCHAR NULL
        -- additional columns of the orders table omitted
    ) NULL,
    -- The row after the change, NULL for a delete
    after ROW(
        -- The primary key of the orders table (but not a PK of the CDC stream)
        order_id VARCHAR NULL,
        amount DECIMAL(12, 2) NULL,
        status VARCHAR NULL
        -- additional columns of the orders table omitted
    ) NULL,
    source ROW(
        -- When the database recorded the change
        ts_ms TIMESTAMP NULL,
        -- Position of the change in the write-ahead log of the database
        lsn BIGINT NULL
        -- additional fields of the Debezium source block omitted
    ) NULL,
    -- 'c' for an insert, 'u' for an update, 'd' for a delete, 'r' for a snapshot read
    op VARCHAR NULL,
    kafka_timestamp TIMESTAMP DEFAULT CAST(CONNECTOR_METADATA()['kafka_timestamp'] AS TIMESTAMP),
    kafka_offset BIGINT DEFAULT CAST(CONNECTOR_METADATA()['kafka_offset'] AS BIGINT)
) WITH (
    'append_only' = 'true',
    'connectors' = '[{
        "name": "orders",
        "transport": {
            "name": "kafka_input",
            "config": {
                "topic": "orders",
                "bootstrap.servers": "broker:9092",
                "include_timestamp": true,
                "include_offset": true,
                "synchronize_partitions": true
            }
        },
        "format": {
            "name": "json",
            "config": { "update_format": "raw" }
        }
    }]'
);
```

Four options of the connector matter to this program:

* `include_timestamp` and `include_offset` make the Kafka timestamp and the
  Kafka offset of a record available to
  [`CONNECTOR_METADATA()`](/connectors/sources/kafka#metadata).  Without them
  the two corresponding columns are always NULL.
* [`synchronize_partitions`](/connectors/sources/kafka#synchronize_partitions)
  makes the connector ingest records in the order of their Kafka timestamps
  across the partitions of the topic.  Without this option the connector reads
  the partitions in parallel and may reorder rows.
* The format is `json` with `update_format` set to
  [`raw`](/formats/json#the-raw-format), so each message becomes one row of
  `orders_cdc`.

#### Flattening the stream

The following local view simplifies the structure of the stream and discards
malformed records (without a timestamp or primary key):

```sql
CREATE LOCAL VIEW changes AS
SELECT
    -- Flatten the messages into top-level columns
    source.ts_ms AS recorded_at,
    kafka_timestamp,
    -- A delete describes the row removed in 'before', other changes are in 'after'
    (CASE WHEN op = 'd' THEN before ELSE after END).order_id AS order_id,
    (CASE WHEN op = 'd' THEN before ELSE after END).amount AS amount,
    (CASE WHEN op = 'd' THEN before ELSE after END).status AS status,
    op,
    -- A snapshot read carries no log position
    COALESCE(source.lsn, 0) AS lsn,
    kafka_offset
FROM orders_cdc
WHERE
  -- drop rows without a key
  (CASE WHEN op = 'd' THEN before ELSE after END).order_id IS NOT NULL
  -- drop rows without timestamps
  AND source.ts_ms IS NOT NULL
  AND kafka_timestamp IS NOT NULL;
```

#### Bounding the state

The following `LATENESS` declarations are applied to this view and are used to
bound the state kept by the pipeline.

```sql
LATENESS changes.recorded_at INTERVAL 1 DAY;
LATENESS changes.kafka_timestamp INTERVAL 0 SECONDS;
```

`recorded_at` is the timestamp the database wrote in its log for each change.
Its values may be out of order for several reasons: - the partitions of
the stream advance at different rates, - a replay after any system (debezium,
Kafka, Feldera) restart re-reads positions the connector already delivered.
`recorded_at` records the same timestamp for all changes within a transaction,
so it could have the same value for multiple rows.  The LATENESS value should be
chosen as the largest of those. In this program the LATENESS value will be used
to remember how long after the original a copy is recognized.

`kafka_timestamp` is a timestamp set when the record enters Kafka.  A replayed
write from the database gets a new, higher timestamp.  How the timestamp is
created is decided by the Kakfa setting
[`message.timestamp.type`](https://kafka.apache.org/documentation/#topicconfigs_message.timestamp.type)
of the topic.  Use `LogAppendTime`, to make the broker stamp the record as it appends it
to the partition; timestamps within a partition will have increasing timestamps.(The default value for this setting, `CreateTime` may produce timestamps out of order).

#### Keeping the first copy

The following view keeps just enough information to detect the first copy of
each arriving row:

```sql
-- The processing time of the first copy of each change
CREATE LOCAL VIEW first_arrival AS
SELECT order_id, recorded_at, lsn, MIN(kafka_timestamp) AS kafka_timestamp
FROM changes
GROUP BY order_id, recorded_at, lsn;
```

The `GROUP BY` includes `recorded_at` as well as the original `PRIMARY KEY` from
table `orders`.  Among duplicates of a row, the one with the smallest kafka
timestamp is preserved.  Warning: two copies that enter Kafka with the same timestamp
are indistinguishable here and both reach the output.  To breaking such ties you needs to compare on a column that differs between the copies, e.g., `kafka_offset`.

The following `LOCAL VIEW` uses a join to extract from the original row the
remaining fields:

```sql
-- The first copy of each change, with its values
CREATE LOCAL VIEW deduplicated AS
SELECT changes.order_id, changes.amount, changes.status, changes.op,
    changes.recorded_at, changes.kafka_timestamp, changes.kafka_offset
FROM first_arrival JOIN changes
  ON changes.order_id = first_arrival.order_id
 AND changes.recorded_at = first_arrival.recorded_at
 AND changes.lsn = first_arrival.lsn
 AND changes.kafka_timestamp = first_arrival.kafka_timestamp;
```

#### Garbage collection

The pipeline stores three collections, and discards old records from all of
them:

* `first_arrival` stores one timestamp for each group.  Feldera discards a group
  when its `recorded_at` falls below the waterline, one day after the database
  recorded the change.
* The join stores both of its inputs, keyed by
  `(order_id, recorded_at, lsn, kafka_timestamp)`.  `MIN(kafka_timestamp)` has
  the waterline of `kafka_timestamp`.  Each side discards a record when either
  timestamp in its key falls below its waterline.  With a lateness of 0 the
  waterline of `kafka_timestamp` is the newest timestamp seen, so each side keeps
  only a small set of the most recent records.

At this point `deduplicated` is itself a log of deduplicated changes: one
row for each change the database recorded, with `op` marking a deletion.  The
section
[Soft deletes with temporal filters](/sql/streaming#soft-deletes-with-temporal-filters)
shows how one can reconstruct the content of table `orders` from such a log, by
ranking the changes of each key and keeping the latest one: you can order by
`recorded_at` and `lsn`.

(If the primary key of the collection is large, a hash of the primary key --
e.g., MD5 -- can be kept instead and used to deduplicate keys.)

## `NOW()` and temporal filters

All features discussed so far evaluate SQL queries over time series data without
referring to the current physical time.  Feldera allows using the current
physical time in queries via the [`NOW()`](/sql/datetime/#now) function.  The primary use of this
function is in implementing **temporal filters**, i.e., queries that filter
records based on the current time values, e.g.:

```sql
CREATE VIEW recent_purchases AS
SELECT * FROM purchase
WHERE
    ts >= NOW() - INTERVAL 7 DAYS;
```

`NOW()` is the only construct in Feldera that can cause the pipeline to produce
outputs without receiving any new input.  In the above example, the pipeline will
output deletions for records that fall outside the 7-day window as the physical
clock ticks forward.

See [`NOW()`](/sql/datetime/#now) documentation for more details.

#### Garbage collection

Feldera implements garbage collection for temporal filters by discarding records older
than the left window bound (e.g., records more than 7 days old in the above example).
This optimization is sound because `NOW()` grows monotonically, and does _not_ require
the timestamp column to have a waterline.


## Further reading

* [Blog post: LATENESS in streaming programs](https://www.feldera.com/blog/lateness-in-streaming-programs)
