# Unsupported and Limited SQL Operations

This page documents SQL operations and functions that are not yet
supported, only partially supported, or should be used with caution in
Feldera.  These limitations are tracked in our [GitHub issue
tracker](https://github.com/feldera/feldera/issues) and may be
resolved in future releases.

## Aggregate functions

The following aggregate functions are not supported:
`PERCENTILE_DISC`, `PERCENTILE_CONT`, `MODE`, `CORR`, `COVAR_POP`,
`COVAR_SAMP`, `REGR_SLOPE`, `REGR_INTERCEPT`, `REGR_R2`, `JSON_AGG`,
`JSON_OBJECT_AGG`, `LISTAGG`.

## Window functions (`OVER` clause)

### Statistics window functions

`NTILE`, `NTH_VALUE`, `PERCENT_RANK`, and `CUME_DIST` window
functions are not yet implemented.

### `FIRST_VALUE` and `LAST_VALUE` limited to unbounded range

`FIRST_VALUE()` and `LAST_VALUE()` are only supported for frames whose
bounds are `UNBOUNDED PRECEDING`, `CURRENT ROW`, or `UNBOUNDED
FOLLOWING` (with `RANGE` or `ROWS`): `FIRST_VALUE` requires the frame
to start at `UNBOUNDED PRECEDING`, and `LAST_VALUE` requires the frame
to end at `UNBOUNDED FOLLOWING`.  Numeric bounds are not yet
supported.
See [#3918](https://github.com/feldera/feldera/issues/3918).

### No `STRING` or `DOUBLE` types in `OVER` ordering

Windowed aggregate functions with frames (e.g., `SUM(x) OVER (...
RANGE BETWEEN ...)`) do not yet support `ORDER BY` on
`VARCHAR`/`STRING`, `DOUBLE`/`FLOAT`, or `VARBINARY` columns.  Plain
window functions such as `ROW_NUMBER`, `RANK`, and `DENSE_RANK`
support these types.
See [#457](https://github.com/feldera/feldera/issues/457).


### `EXCLUDE` clause not supported

The `EXCLUDE` clause in window frame specifications is not supported.
See [#457](https://github.com/feldera/feldera/issues/457).

### Multi-column `ORDER BY` in windows not supported

Windowed aggregate functions with frames require `ORDER BY` on
exactly one column.  Plain window functions such as `RANK` and
`DENSE_RANK` support `ORDER BY` on multiple columns.
See [#457](https://github.com/feldera/feldera/issues/457).

### Constant Window Boundaries

Window boundaries must be constant expressions. For example, `RANGE
BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW` is valid. But `RANGE
BETWEEN INTERVAL 1 MONTH PRECEDING AND CURRENT ROW` is not, because a
month is not a constant time interval.

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

In some instances, Feldera cannot decorrelate complex nested
subqueries. In these cases, we recommend users refactor the query.

See [#2555](https://github.com/feldera/feldera/issues/2555).

## `LEFT JOIN UNNEST`

`LEFT JOIN UNNEST(...)` and `OUTER APPLY (...)`, are not yet supported:

```sql
-- NOT supported: LEFT JOIN UNNEST
SELECT s.id, mention_id
FROM spreadsheet s
LEFT JOIN UNNEST(s.mentions) AS m(mention_id) ON TRUE;
```

## Map functions

Several `MAP` functions are not yet implemented:

| Function | Status |
|----------|--------|
| `MAP_ENTRIES` | Not supported |
| `MAP_FROM_ARRAYS` | Not supported |
| `MAP_FROM_ENTRIES` | Not supported |
| `STR_TO_MAP` | Not supported |

A list of supported MAP operations is available [here](./map.md).
See [#1907](https://github.com/feldera/feldera/issues/1907).

## `MATCH_RECOGNIZE`

The `MATCH_RECOGNIZE` clause for pattern matching over rows is not yet
supported.

## `PIVOT`

`PIVOT` is supported if the user provides a fixed set of
columns. Refer to [PIVOT documentation](./aggregates.md#pivots) for
example usage.  Dynamic `PIVOT` is not yet supported.

## `MULTISET` Data Type
The `MULTISET` data type is not currently supported.

## Session windows

Session windows (grouping events into sessions based on a gap in
activity) are not yet supported.

## `TIME` with timezone

The type `TIME WITH TIME ZONE` is not supported.

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
every processing step (every 1 second by default, configurable via
`clock_resolution_usecs`).

- **In filters**: `NOW()` in `WHERE` clauses for temporal filtering
  (e.g., `WHERE ts >= NOW() - INTERVAL 1 DAY`) is efficient and
  recommended.
- **In `SELECT` or `JOIN` expressions**: Using `NOW()` in projections
  (e.g., `SELECT col + NOW() FROM T`) forces a full table scan at
  every step, which can produce large deltas and degrade performance
  significantly.

See the [datetime documentation](datetime.md#now) for more details.
