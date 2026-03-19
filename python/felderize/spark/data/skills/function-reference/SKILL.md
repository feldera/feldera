---
name: function-reference
description: Spark-to-Feldera function mapping reference. The single source of truth for which Spark functions have Feldera equivalents, which need rewriting, and which are unsupported. Consult this before marking anything as unsupported.
---

# Function Reference

## Purpose

Consult this reference BEFORE translating any Spark function. It tells you whether a function is directly available, needs rewriting, or is unsupported in Feldera.

## Direct equivalents (same or near-identical syntax)

These Spark functions exist in Feldera — translate directly:

### Aggregate functions

| Spark | Feldera | Notes |
|-------|---------|-------|
| `COUNT(*)`, `COUNT(col)` | Same | |
| `SUM(col)` | Same | |
| `AVG(col)` | Same | |
| `MIN(col)`, `MAX(col)` | Same | |
| `COUNT(DISTINCT col)` | Same | |
| `STDDEV_SAMP(col)` | Same | |
| `STDDEV_POP(col)` | Same | |

### String functions

| Spark | Feldera | Notes |
|-------|---------|-------|
| `UPPER(s)` | Same | |
| `LOWER(s)` | Same | |
| `TRIM(s)` | Same | |
| `LTRIM(s)`, `RTRIM(s)` | Same | |
| `LENGTH(s)` | Same | |
| `SUBSTRING(s, pos, len)` | Same | |
| `CONCAT(a, b)` | Same | |
| `CONCAT_WS(sep, ...)` | Same | |
| `REPLACE(s, from, to)` | Same | |
| `REGEXP_REPLACE(s, pat, rep)` | Same | |
| `RLIKE(s, pattern)` | Same | |
| `INITCAP(s)` | Same | |
| `REVERSE(s)` | Same | |
| `REPEAT(s, n)` | Same | |
| `ASCII(s)` | Same | Returns numeric code of first character |
| `CHR(n)` | Same | Returns character for numeric code |
| `LEFT(s, n)` | Same | |
| `RIGHT(s, n)` | Same | |
| `MD5(s)` | Same | |

### Array functions

| Spark | Feldera | Notes |
|-------|---------|-------|
| `array_contains(arr, val)` | `ARRAY_CONTAINS(arr, val)` | |
| `sort_array(arr)` | `SORT_ARRAY(arr)` | |
| `sort_array(arr, false)` | `SORT_ARRAY(arr, false)` | Descending |
| `array_distinct(arr)` | `ARRAY_DISTINCT(arr)` | |
| `array_position(arr, val)` | `ARRAY_POSITION(arr, val)` | 1-based |
| `array_remove(arr, val)` | `ARRAY_REMOVE(arr, val)` | |
| `arrays_overlap(a, b)` | `ARRAYS_OVERLAP(a, b)` | |
| `array_repeat(val, n)` | `ARRAY_REPEAT(val, n)` | |
| `array_union(a, b)` | `ARRAY_UNION(a, b)` | |
| `array_intersect(a, b)` | `ARRAY_INTERSECT(a, b)` | |
| `array_except(a, b)` | `ARRAY_EXCEPT(a, b)` | |
| `array_join(arr, sep)` | `ARRAY_JOIN(arr, sep)` | Alias for ARRAY_TO_STRING |
| `size(arr)` | `CARDINALITY(arr)` | Different name |
| `array(v1, v2)` | `ARRAY(v1, v2)`|
| `array[v1, v2]` | `ARRAY[v1, v2]`|

### Higher-order array functions

| Spark | Feldera | Notes |
|-------|---------|-------|
| `transform(arr, x -> expr)` | `TRANSFORM(arr, x -> expr)` | Same syntax |
| `exists(arr, x -> expr)` | `ARRAY_EXISTS(arr, x -> expr)` | Different name |

### Map functions

| Spark | Feldera | Notes |
|-------|---------|-------|
| `map_keys(m)` | `MAP_KEYS(m)` | |
| `map_values(m)` | `MAP_VALUES(m)` | |

### Date/Time functions

| Spark | Feldera | Notes |
|-------|---------|-------|
| `YEAR(d)` | Same | |
| `MONTH(d)` | Same | |
| `DAY(d)` | `DAYOFMONTH(d)` | Use EXTRACT for timestamps |
| `HOUR(ts)` | Same | |
| `MINUTE(ts)` | Same | |
| `SECOND(ts)` | Same | |
| `CURRENT_TIMESTAMP` | `NOW()` | |

### JSON functions

| Spark | Feldera | Notes |
|-------|---------|-------|
| `parse_json(s)` | `PARSE_JSON(s)` | Returns VARIANT |
| `to_json(v)` | `TO_JSON(v)` | |

### Math functions

| Spark | Feldera | Notes |
|-------|---------|-------|
| `ABS(x)` | Same | |
| `CEIL(x)` | Same | For float/double input: Spark returns BIGINT, Feldera returns DOUBLE |
| `FLOOR(x)` | Same | For float/double input: Spark returns BIGINT, Feldera returns DOUBLE |
| `ROUND(x, d)` | Same | Rounding differs: Spark rounds half-up (0.5 → 1), Feldera rounds half-to-even (0.5 → 0, 1.5 → 2) |
| `BROUND(x, d)` | Same | Banker's rounding (half-to-even); Feldera supports decimal only |
| `MOD(a, b)` | Same | Feldera supports integer only; Spark also supports DECIMAL |
| `POWER(x, y)` | Same | |
| `SQRT(x)` | Same | |
| `LN(x)` | Same | Spark returns NULL for negative input; Feldera returns -inf for 0 and runtime error for negative |
| `LOG10(x)` | Same | Spark returns NULL for negative input; Feldera returns -inf for 0 and runtime error for negative |

### Null handling

| Spark | Feldera | Notes |
|-------|---------|-------|
| `COALESCE(a, b)` | Same | |
| `NVL(a, b)` | `COALESCE(a, b)` | NVL not supported in Feldera |
| `NULLIF(a, b)` | Same | |
| `IFNULL(a, b)` | Same | Equivalent to COALESCE(left, right) |
| `a <=> b` | Same | Null-safe equality — returns true when both sides are NULL |

### Window functions (unrestricted)

| Spark | Feldera | Notes |
|-------|---------|-------|
| `LAG(expr, offset, default)` | Same | |
| `LEAD(expr, offset, default)` | Same | |
| `SUM(expr) OVER (...)` | Same | |
| `AVG(expr) OVER (...)` | Same | |
| `COUNT(expr) OVER (...)` | Same | |
| `MIN(expr) OVER (...)` | Same | |
| `MAX(expr) OVER (...)` | Same | |
| `FIRST_VALUE(expr) OVER (...)` | Same | IGNORE NULLS not supported; no custom RANGE/ROWS — only whole-partition windows |
| `LAST_VALUE(expr) OVER (...)` | Same | IGNORE NULLS not supported; no custom RANGE/ROWS — only whole-partition windows |

These can be freely combined in the same query.

### Window functions (TopK pattern only)

| Spark | Feldera | Notes |
|-------|---------|-------|
| `ROW_NUMBER()` | Same | Must be in subquery with WHERE filter on result |
| `RANK()` | Same | Must be in subquery with WHERE filter on result |
| `DENSE_RANK()` | Same | Must be in subquery with WHERE filter on result |

Valid TopK example:
```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY col1 ORDER BY col2) AS rn
  FROM t
) sub
WHERE rn <= 10
```

Without the outer WHERE filter, these are UNSUPPORTED.

## Rewritable patterns (Spark syntax → Feldera syntax)

These require translation but ARE supported:

| Spark | Feldera | See skill |
|-------|---------|-----------|
| `CURRENT_DATE` | `CAST(NOW() AS DATE)` | time-converter |
| `EXPLODE` / `LATERAL VIEW explode(arr)` | `UNNEST(arr) AS t(col)` | unnest |
| `LATERAL VIEW explode(map)` | `CROSS JOIN UNNEST(map) AS t(k, v)` | unnest |
| `posexplode(arr)` | `SELECT pos, val FROM UNNEST(arr) WITH ORDINALITY AS t(val, pos)` | unnest; Feldera returns (val, pos), reorder in SELECT to match Spark |
| `inline(arr_of_structs)` | `UNNEST(arr) AS t(f1, f2, ...)` | unnest |
| `get_json_object(s, '$.a.b')` | `PARSE_JSON(s)['a']['b']` | json-operations |
| `from_json(s, schema)` | `PARSE_JSON(s)` + CAST or bracket access | json-operations |
| `json_tuple(s, k1, k2)` | Multiple `PARSE_JSON(s)['k']` | json-operations |
| `named_struct('a', v1, 'b', v2)` | `CAST(ROW(v1, v2) AS ROW(a T, b S))` | query-rewrite; use CAST to preserve field names |
| `nvl2(x, a, b)` | `CASE WHEN x IS NOT NULL THEN a ELSE b END` | query-rewrite |
| `pmod(a, b)` | `MOD(MOD(a, b) + b, b)` | query-rewrite |
| `PIVOT(...)` | Same or `CASE WHEN` aggregation | Feldera supports PIVOT with static columns only; use CASE WHEN for dynamic pivots |
| `GROUPING SETS` | Same | |
| `ROLLUP(a, b)` | Same | |
| `CUBE(a, b)` | Same | |
| `date_add(d, n)` | `d + INTERVAL 'n' DAY` | time-converter |
| `date_sub(d, n)` | `d - INTERVAL 'n' DAY` | time-converter |
| `datediff(end, start)` | `TIMESTAMPDIFF(DAY, start, end)` | time-converter |
| `date_trunc('MONTH', d)` | `DATE_TRUNC(d, MONTH)` | time-converter |
| `date_trunc('MONTH', ts)` | `TIMESTAMP_TRUNC(ts, MONTH)` | time-converter |
| `LPAD(s, n, pad)` | `CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(REPEAT(pad, n-LENGTH(s)), s) END` | query-rewrite |
| `RPAD(s, n, pad)` | `CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(s, REPEAT(pad, n-LENGTH(s))) END` | query-rewrite |
| `LOCATE(substr, str)` | `POSITION(substr IN str)` | |
| `LOCATE(substr, str, pos)` | `POSITION(substr IN SUBSTRING(str, pos)) + pos - 1` | |
| `startswith(s, prefix)` | `LEFT(s, LENGTH(prefix)) = prefix` | |
| `endswith(s, suffix)` | `RIGHT(s, LENGTH(suffix)) = suffix` | |
| `isnan(x)` | `IS_NAN(x)` | |
| `LEFT ANTI JOIN ... ON cond` | `WHERE NOT EXISTS (SELECT 1 FROM ... WHERE cond)` | query-rewrite |
| `weekofyear(d)` | `EXTRACT(WEEK FROM d)` | query-rewrite |
| `add_months(d, n)` | `d + INTERVAL 'n' MONTH` | |
| `last_day(d)` | `DATE_TRUNC(d, MONTH) + INTERVAL '1' MONTH - INTERVAL '1' DAY` | |
| `unix_timestamp(ts)` | `EXTRACT(EPOCH FROM ts)` | |
| `unix_millis(ts)` | `CAST(EXTRACT(EPOCH FROM ts) * 1000 AS BIGINT)` | |
| `unix_micros(ts)` | `CAST(EXTRACT(EPOCH FROM ts) * 1000000 AS BIGINT)` | |
| `from_unixtime(n)` | `TIMESTAMPADD(SECOND, n, DATE '1970-01-01')` | Returns TIMESTAMP; Spark returns STRING in yyyy-MM-dd HH:mm:ss — Feldera has no FORMAT_TIMESTAMP, use CONCAT with YEAR/MONTH/DAY/HOUR/MINUTE/SECOND to match |
| `to_timestamp(n)` (numeric) | `TIMESTAMPADD(SECOND, n, DATE '1970-01-01')` | |
| `to_timestamp(s[, fmt])` | `PARSE_TIMESTAMP(fmt, s)` | Argument order reversed; default fmt is `%Y-%m-%d %H:%M:%S`; translate Java fmt to strftime |
| `map_entries(m)` | `CROSS JOIN UNNEST(m) AS t(k, v)` | Flatten map to rows |
| `translate(s, from, to)` | Chain of `REGEXP_REPLACE` per character | |
| `to_date(ts)` / `to_date(ts, fmt)` | `CAST(ts AS DATE)` | Format param ignored |
| `try_divide(a, b)` | `CASE WHEN b = 0 OR b IS NULL THEN NULL ELSE a / b END` | |
| `date_format(d, fmt)` | `FORMAT_DATE(fmt, d)` | DATE only; argument order reversed; Spark uses Java patterns (yyyy-MM-dd), Feldera uses strftime (%Y-%m-%d); no TIMESTAMP equivalent |

## Unsupported (no Feldera equivalent)

Do NOT attempt to translate these. Return as unsupported immediately.

### Functions
| Function | Category |
|----------|----------|
| `substring_index` | String |
| `REGEXP_EXTRACT` | Regex |
| `SOUNDEX` | String phonetic |
| `find_in_set` | String search |
| `parse_url` | URL parsing |
| `SHA`, `SHA2`, `SHA256` | Hashing |
| `next_day` | Date |
| `MAKE_DATE` | Date constructor |
| `months_between` | Date diff |
| `sequence()` for date ranges | Date generation |
| `CORR` | Statistical aggregate |
| `approx_count_distinct`, `APPROX_DISTINCT` | Approximate aggregate |
| `percentile_approx`, `approx_percentile` | Approximate aggregate |
| `PERCENT_RANK`, `CUME_DIST` | Window (not implemented) |
| `NTILE` | Window (not implemented) |
| `NTH_VALUE` | Window (not implemented) |
| `filter(arr, lambda)` | Higher-order (compiler rejects) |
| `aggregate(arr, init, lambda)` | Higher-order |
| `forall(arr, lambda)` | Higher-order |
| `zip_with(a, b, lambda)` | Higher-order |
| `map_filter(m, lambda)` | Higher-order |
| `transform_keys(m, lambda)` | Higher-order |
| `transform_values(m, lambda)` | Higher-order |
| `flatten(nested_arr)` | Array |
| `arrays_zip(a, b)` | Array |
| `slice(arr, start, len)` | Array |
| `from_csv`, `to_csv` | CSV |
| `schema_of_json`, `schema_of_csv` | Schema inference |
| `stack()` | Unpivot |
| `INSERT OVERWRITE` | DDL |
| `grouping_id()` | Grouping |
| `str_to_map` | String/Map |
| `map_concat` | Map |

### Patterns
| Pattern | Reason |
|---------|--------|
| `ROW_NUMBER()` / `RANK()` / `DENSE_RANK()` without TopK | Must be in subquery with WHERE filter |
| `ORDER BY` / `LIMIT` in views | Supported — keep as-is, do NOT remove or mark unsupported |

## Important rules

- Do NOT hallucinate restrictions that don't exist (e.g., "Multiple RANK aggregates per window" is NOT an error).
- You CAN combine LAG, LEAD, SUM OVER, etc. in the same query — no restriction.
- If the compiler reports "No match found for function signature X", check this reference FIRST. If listed as unsupported, return immediately — do NOT retry.
