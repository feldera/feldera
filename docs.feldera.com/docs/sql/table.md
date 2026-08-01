# Table Functions

A table function is a function that returns data of a table type.  The
table-valued function can be used wherever a relation can be used.

## Descriptors

The `DESCRIPTOR` operator is used with table functions.  The syntax
is:

```
DESCRIPTOR(name [, name ]*)
```

`DESCRIPTOR` appears as an argument in a function to indicate a list
of names.  The interpretation of names is left to the function.

## Table functions

Table functions occur in the FROM clause.

The following table functions are predefined:

### `TUMBLE`

`TUMBLE` assigns a window for each row of a relation based on a
timestamp column. An assigned time window is specified by its
beginning and ending.  All time windows have the same length (in
absolute time), and that’s why tumbling sometimes is named as “fixed
windowing”. The first parameter of the `TUMBLE` table function is a
table parameter.

The `timecol` must have a `TIMESTAMP` type.  The `size` must be a
"short" SQL interval type (e.g., `DAYS` or shorter), because "long"
SQL interval values are not constant values (e.g., the duration of a
month is not a constant).

#### Syntax:

```
TUMBLE(data, DESCRIPTOR(timecol), size [, offset ])
```

Indicates a tumbling window of `size` interval for `timecol`,
optionally aligned at `offset`.

Here is an example:

```sql
SELECT * FROM TABLE(
  TUMBLE(
    TABLE orders,
    descriptor(rowtime),
    INTERVAL '1' MINUTE));

-- or with the named params
-- note: the DATA param must be the first
SELECT * FROM TABLE(
  TUMBLE(
    data => TABLE orders,
    timecol => descriptor(rowtime),
    size => INTERVAL '1' MINUTE));
```

The result is a table that has all the columns of the `orders` table,
and in addition the following columns, defined by the `TUMBLE`
function:
- `window_start`, of the same type as the column `orders.rowtime`
- `window_end`, of the same type as the column `orders.rowtime`

### `HOP`

`HOP` assigns windows that cover rows within the interval of size and
shifting every slide based on a timestamp column.  Windows assigned
could overlap, so hopping sometimes is also named “sliding window”.

#### Syntax:

```
HOP(data, DESCRIPTOR(timecol), slide, size [, offset ])
```

Indicates a hopping window for `timecol`, covering rows within the
interval of `size`, shifting every `slide` and optionally aligned at
`offset`.  The type of the `timecol` has to be `TIMESTAMP`.  The
intervals must be compile-time constants, and be expressed as a
"short" interval (i.e., days or smaller time units), because "long"
SQL interval values are not constant values (e.g., the duration of a
month is not a constant).

Here is an example:

```sql
SELECT * FROM TABLE(
  HOP(
    TABLE orders,
    descriptor(rowtime),
    INTERVAL '2' MINUTE,
    INTERVAL '5' MINUTE));

-- or with the named params
-- note: the DATA param must be the first
SELECT * FROM TABLE(
  HOP(
    data => TABLE orders,
    timecol => descriptor(rowtime),
    slide => INTERVAL '2' MINUTE,
    size => INTERVAL '5' MINUTE));
```

applies hopping with 5-minute interval size on rows from table
`orders` and shifting every 2 minutes.

The result is a table that has all the columns of the `orders` table,
and in addition the following columns, defined by the `HOP`
function:
- `window_start`, of the same type as the column `orders.rowtime`
- `window_end`, of the same type as the column `orders.rowtime`

A `NULL` timestamp produces no rows in the result.

### `SESSION`

`SESSION` groups rows into sessions based on a timestamp column.  Two
rows belong to the same session when their timestamps are less than
`size` (the inactivity gap) apart.  Unlike `TUMBLE` and `HOP` windows,
session windows are not fixed in absolute time: each session starts at
the timestamp of its first row and ends `size` after the timestamp of
its last row.  The optional `key` descriptor partitions the rows;
sessions are formed separately within each key.

Here is an example showing session windows defined by intervals longer
than 10 minutes (we only show the timestamps of the rows involved,
sorted increasingly).

```
10:00 -- session starts
10:04 |
10:13 |
10:20 -- session ends
10:32 -- session starts
10:36 |
10:40 -- session ends
10:51 -- session starts and ends
```

#### Syntax:

```
SESSION(data, DESCRIPTOR(timecol) [, DESCRIPTOR(key) ], size)
```

The type of the `timecol` has to be `TIMESTAMP`.

Here is an example:

```sql
SELECT * FROM TABLE(
  SESSION(
    TABLE orders,
    DESCRIPTOR(rowtime),
    DESCRIPTOR(product),
    INTERVAL '20' MINUTE));

-- or with the named params
-- note: the DATA param must be the first
SELECT * FROM TABLE(
  SESSION(
    DATA => TABLE orders,
    TIMECOL => DESCRIPTOR(rowtime),
    KEY => DESCRIPTOR(product),
    SIZE => INTERVAL '20' MINUTE));
```

groups the rows of `orders` into sessions per `product`; a session
ends when a product receives no orders for 20 minutes.

The result is a table that has all the columns of the `orders` table,
and in addition the following columns, defined by the `SESSION`
function:
- `window_start`, of the same type as the column `orders.rowtime`;
  the timestamp of the session's first row
- `window_end`, of the same type as the column `orders.rowtime`;
  the timestamp of the session's last row plus `size`

A `NULL` timestamp produces no rows in the result.  A `NULL` key
groups rows like any other key value.
