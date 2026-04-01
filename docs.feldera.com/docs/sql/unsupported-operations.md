# Unsupported and Limited SQL Operations

This page documents SQL operations and functions that are not yet
supported, only partially supported, or should be used with caution in
Feldera.  These limitations are tracked in our [GitHub issue
tracker](https://github.com/feldera/feldera/issues) and may be
resolved in future releases.

## Window functions (`OVER` clause)

### `ROW_NUMBER`, `RANK`, and `DENSE_RANK` require TopK pattern

`ROW_NUMBER()`, `RANK()`, and `DENSE_RANK()` are **only supported when
the compiler detects a TopK pattern**.  Feldera supports **one** TopK pattern per OVER query. A TopK pattern requires that
the window function result is filtered using a `WHERE` clause:

```sql
-- Supported: TopK pattern
SELECT * FROM (
   SELECT empno,
          ROW_NUMBER() OVER (ORDER BY empno) AS rn
   FROM empsalary
) WHERE rn < 3;

-- NOT supported: standalone usage without TopK filter
SELECT empno,
       ROW_NUMBER() OVER (ORDER BY empno)
FROM empsalary;
```

General-purpose `RANK`, `DENSE_RANK`, and `ROW_NUMBER` outside of TopK
detection are not yet implemented.
See [#3934](https://github.com/feldera/feldera/issues/3934).

### `NTILE` and `NTH_VALUE` are not supported

The `NTILE()` and `NTH_VALUE()` window functions are not yet implemented.

### `FIRST_VALUE` and `LAST_VALUE` limited to unbounded range

`FIRST_VALUE()` and `LAST_VALUE()` are only supported for windows with
an unbounded range (e.g., `RANGE BETWEEN UNBOUNDED PRECEDING AND
CURRENT ROW`).  Custom `RANGE` bounds or `ROWS` frames are not yet
supported.
See [#3918](https://github.com/feldera/feldera/issues/3918).

### No `STRING` or `DOUBLE` types in `OVER` ordering

Window functions using `ORDER BY` on `VARCHAR`/`STRING` or
`DOUBLE`/`FLOAT` columns are not yet supported.  This limitation
affects TPC-DS queries q35, q47, q49, q51, q57, q70, and q86.
See [#457](https://github.com/feldera/feldera/issues/457).

### `ROWS` frame type not supported

The `ROWS` frame specification in window functions is not yet
supported.  Only `RANGE` frames are currently accepted.
See [#457](https://github.com/feldera/feldera/issues/457).

### `EXCLUDE` clause not supported

The `EXCLUDE` clause in window frame specifications is not supported.
See [#457](https://github.com/feldera/feldera/issues/457).

### Multi-column `ORDER BY` in windows not supported

Window functions with `ORDER BY` on multiple columns are not yet
supported.
See [#457](https://github.com/feldera/feldera/issues/457).

### Constant Window Boundaries
Window boundaries must be constant expressions. For example, `RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW` is valid. But `RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND CURRENT ROW` is not, because a month is a not a constant time interval.

## Correlated subqueries

Some forms of correlated subqueries are not yet implemented and will
fail with a "Not yet implemented" error.  For example, using `UNNEST`
on a column from an outer query inside an `ARRAY` subquery is not
supported:

```sql
-- NOT supported: correlated UNNEST inside ARRAY subquery
SELECT s.id, ARRAY(
    SELECT sp.cell
    FROM UNNEST(s.mentions) AS mention_id
    JOIN spreadsheet sp ON sp.id = mention_id
) AS mentioned_cells
FROM spreadsheet s;
```

In some instances, Feldera cannot decorrelate complex nested subqueries. In these cases, we recommend users refactor the query.

See [#2555](https://github.com/feldera/feldera/issues/2555).

## Map functions

Several `MAP` functions are not yet implemented:

| Function | Status |
|----------|--------|
| `MAP_CONCAT` | Not supported |
| `MAP_ENTRIES` | Not supported |
| `MAP_FROM_ARRAYS` | Not supported |
| `MAP_FROM_ENTRIES` | Not supported |
| `STR_TO_MAP` | Not supported |
| Building a `MAP` from a subquery returning a pair of columns | Not supported |

A list of supported MAP operations is available [here](./map.md).
See [#1907](https://github.com/feldera/feldera/issues/1907).

## `MATCH_RECOGNIZE`

The `MATCH_RECOGNIZE` clause for pattern matching over rows is not yet
supported.

## `PIVOT` & `UNPIVOT`
`PIVOT` is supported if the user provides a fixed set of columns. Refer to [PIVOT documentation](./aggregates.md#pivots) for example usage. Dynamic `PIVOT` is not yet supported. `UNPIVOT` is not yet supported.


## `INTERSECT ALL` and `EXCEPT ALL`
`INTERSECT ALL` and `EXCEPT ALL` operations are not yet supported.

## `MULTISET` Data Type
The `MULTISET` data type is not currently supported.

## Session windows

Session windows (grouping events into sessions based on a gap in
activity) are not yet supported.

## Timezone support

`TIME`, and `TIMESTAMP` types have no time zone.  There is no
`TIMESTAMP WITH TIME ZONE` type, and timezone conversion functions are
not available.  See the [datetime documentation](datetime.md#timezones)
for details.

## Performance caveats

### `ARRAY_AGG` is expensive

`ARRAY_AGG` has O(N) space cost and O(M) work per change, where N is
the collection size and M is the total number of elements in modified
groups.  Consider whether your use case truly requires collecting all
values into an array.  See the [aggregate efficiency
documentation](aggregates.md#expensive-aggregation-functions) for
details.

### Use `NOW()` with caution

The `NOW()` function returns the current timestamp and is updated at
every processing step (every 100ms by default).

- **In filters**: `NOW()` in `WHERE` clauses for temporal filtering
  (e.g., `WHERE ts >= NOW() - INTERVAL 1 DAY`) is efficient and
  recommended.
- **In `SELECT` or `JOIN` expressions**: Using `NOW()` in projections
  (e.g., `SELECT col + NOW() FROM T`) forces a full table scan at
  every step, which can produce large deltas and degrade performance
  significantly.

See the [datetime documentation](datetime.md#now) for more details.
