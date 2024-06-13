# Table functions

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
    DESCRIPTOR(rowtime),
    INTERVAL '1' MINUTE));

-- or with the named params
-- note: the DATA param must be the first
SELECT * FROM TABLE(
  TUMBLE(
    DATA => TABLE orders,
    TIMECOL => DESCRIPTOR(rowtime),
    SIZE => INTERVAL '1' MINUTE));
```

### `HOP`

`HOP` assigns windows that cover rows within the interval of size and
shifting every slide based on a timestamp column.  Windows assigned
could overlap, so hopping sometime is also named “sliding window”.

#### Syntax:

```
HOP(data, DESCRIPTOR(timecol), slide, size [, offset ])
```

Indicates a hopping window for `timecol`, covering rows within the
interval of `size`, shifting every `slide` and optionally aligned at
`offset`.

Here is an example:

```sql
SELECT * FROM TABLE(
  HOP(
    TABLE orders,
    DESCRIPTOR(rowtime),
    INTERVAL '2' MINUTE,
    INTERVAL '5' MINUTE));

-- or with the named params
-- note: the DATA param must be the first
SELECT * FROM TABLE(
  HOP(
    DATA => TABLE orders,
    TIMECOL => DESCRIPTOR(rowtime),
    SLIDE => INTERVAL '2' MINUTE,
    SIZE => INTERVAL '5' MINUTE));
```

applies hopping with 5-minute interval size on rows from table orders
and shifting every 2 minutes. rowtime is the column of table orders
that tells data completeness.