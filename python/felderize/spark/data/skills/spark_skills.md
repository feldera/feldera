---
name: spark-skills
description: Master Spark-to-Feldera translation reference. The single skill file covering all function mappings, type syntax, DDL rewrites, and unsupported features. Consult this before marking anything as unsupported.
---

# Spark-to-Feldera Translation Reference

## Purpose

Consult this reference BEFORE translating any Spark SQL. It covers DDL rewrites, type mappings, function mappings, and unsupported features.

## Type Mappings

| Spark | Feldera | Notes |
|-------|---------|-------|
| `STRING` | `STRING` | Same |
| `TEXT` | `TEXT` | Same |
| `INT` / `INTEGER` | `INT` | Same |
| `BIGINT` | `BIGINT` | Same |
| `BOOLEAN` | `BOOLEAN` | Same |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | Same |
| `FLOAT` | `REAL` | Feldera uses REAL instead of FLOAT |
| `DOUBLE` | `DOUBLE` | Same |
| `DATE` | `DATE` | Same |
| `TIMESTAMP` | `TIMESTAMP` | Same |
| `MAP<K, V>` | `MAP<K, V>` | Translate inner types |
| `ARRAY<T>` | `T ARRAY` | Suffix syntax; see rules below |
| `STRUCT<a: T, b: U>` | `ROW(a T, b U)` | |

### Array type syntax rules

Feldera uses suffix syntax — fix DDL before addressing query-level issues:

```text
ARRAY<T>           →  T ARRAY
ARRAY<ARRAY<T>>    →  T ARRAY ARRAY
ARRAY<ROW(...)>    →  ROW(...) ARRAY
MAP<K, ARRAY<V>>   →  MAP<K, V ARRAY>
```

- Array literals use `ARRAY[...]` or `ARRAY(...)`.
- Array indexes are 1-based.
- Never emit `ARRAY<...>` in Feldera DDL.

If the compiler reports `Encountered "<" ... ARRAY<VARCHAR>`: rewrite ALL `ARRAY<...>` to suffix form first, then re-validate.

## DDL Rewrites

| Spark syntax | Action |
|-------------|--------|
| `CREATE OR REPLACE TEMP VIEW` | → `CREATE VIEW` |
| `CREATE TEMPORARY VIEW` | → `CREATE VIEW` |
| `USING parquet` / `delta` / `csv` | Remove clause |
| `PARTITIONED BY (...)` | Remove clause |
| `CONSTRAINT name PRIMARY KEY (cols)` | → `PRIMARY KEY (cols)` — drop the `CONSTRAINT name` wrapper; Feldera rejects the named constraint syntax |
| PK column without `NOT NULL` | Add `NOT NULL` — Feldera requires all PRIMARY KEY columns to be NOT NULL |

### PRIMARY KEY rules

Two constraints Feldera enforces that Spark does not:

1. **No `CONSTRAINT name` wrapper** — Feldera rejects `CONSTRAINT pk PRIMARY KEY (col)`. Use bare `PRIMARY KEY (col)`.
2. **All PK columns must be `NOT NULL`** — Feldera rejects nullable PK columns:

```
error: PRIMARY KEY cannot be nullable: PRIMARY KEY column 'borrowerid' has type VARCHAR, which is nullable
```

```sql
-- Spark (both issues)
CREATE TABLE orders (
  order_id STRING,
  item_id STRING,
  CONSTRAINT orders_pk PRIMARY KEY (order_id, item_id)
);

-- Feldera (fixed)
CREATE TABLE orders (
  order_id VARCHAR NOT NULL,
  item_id VARCHAR NOT NULL,
  PRIMARY KEY (order_id, item_id)
);
```

### Reserved words as column names must be quoted

**Only quote column names that are SQL reserved words** — do not quote ordinary identifiers. Quoting non reserved words is wrong: it makes identifiers case-sensitive and adds unnecessary noise.

Quote a column name only when the compiler rejects it unquoted:
```
error: Error parsing SQL: Encountered ", TimeStamp" at line 33, column 25.
```

When quoting is needed, apply it consistently in both `CREATE TABLE` and every query reference:

```sql
-- Schema: only "TimeStamp" is quoted — it clashes with the TIMESTAMP type keyword
CREATE TABLE events (
  id BIGINT NOT NULL,
  source VARCHAR,
  "TimeStamp" TIMESTAMP,
  PRIMARY KEY (id)
);

-- Query: quote "TimeStamp" everywhere it appears, leave other columns unquoted
SELECT e.source, e."TimeStamp" as ts,
       MAX(e."TimeStamp") OVER (PARTITION BY e.id) as max_ts
FROM events e
WHERE e."TimeStamp" >= TIMESTAMP '2024-01-01 00:00:00'
```

Known column names that clash with SQL keywords: `TimeStamp`, `Date`, `Time`, `Value`, `Type`, `Name`, `Language`.

### DDL Examples

```sql
-- Spark
CREATE TABLE t (id BIGINT, name STRING, tags ARRAY<STRING>) USING parquet;
-- Feldera
CREATE TABLE t (id BIGINT, name VARCHAR, tags VARCHAR ARRAY);
```

```sql
-- Spark
CREATE OR REPLACE TEMP VIEW v AS SELECT * FROM t ORDER BY id LIMIT 10;
-- Feldera
CREATE VIEW v AS SELECT * FROM t ORDER BY id LIMIT 10;
```

## Function Reference

### Direct equivalents (same or near-identical syntax)

These Spark functions exist in Feldera — translate directly:

#### Aggregate functions

| Spark | Feldera | Notes |
|-------|---------|-------|
| `COUNT(*)`, `COUNT(col)` | Same | |
| `SUM(col)` | Same | |
| `AVG(col)` | Same | |
| `MIN(col)`, `MAX(col)` | Same | |
| `COUNT(DISTINCT col)` | Same | |
| `STDDEV_SAMP(col)` | Same | |
| `STDDEV_POP(col)` | Same | |
| `bool_or(col)` | Same | |
| `bool_and(col)` | Same | |
| `bit_and(col)` — aggregate | `BIT_AND(col)` | Aggregate: bitwise AND over all rows in group |
| `bit_or(col)` — aggregate | `BIT_OR(col)` | Aggregate: bitwise OR over all rows in group |
| `bit_xor(col)` — aggregate | `BIT_XOR(col)` | Aggregate: bitwise XOR over all rows in group |

#### String functions

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
| `RLIKE(s, pattern)` | Same | Infix `s RLIKE pattern` also works |
| `INITCAP(s)` | Same | |
| `REVERSE(s)` | Same | |
| `REPEAT(s, n)` | Same | |
| `ASCII(s)` | Same | Returns numeric code of first character |
| `CHR(n)` | Same | Returns character for numeric code |
| `LEFT(s, n)` | Same | |
| `RIGHT(s, n)` | Same | |
| `MD5(s)` | Same | |
| `split_part(str, delim, n)` | `SPLIT_PART(str, delim, n)` | 1-based; negative n counts from end |
| `overlay(str placing repl from pos for len)` | `OVERLAY(str PLACING repl FROM pos FOR len)` | Same syntax |

#### Array functions

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
| `array(v1, v2)` | `ARRAY(v1, v2)` | |

#### Higher-order array functions

| Spark | Feldera | Notes |
|-------|---------|-------|
| `transform(arr, x -> expr)` | `TRANSFORM(arr, x -> expr)` | Same syntax |
| `exists(arr, x -> expr)` | `ARRAY_EXISTS(arr, x -> expr)` | Different name |

#### Map functions

| Spark | Feldera | Notes |
|-------|---------|-------|
| `map_keys(m)` | `MAP_KEYS(m)` | |
| `map_values(m)` | `MAP_VALUES(m)` | |

#### Date/Time functions

| Spark | Feldera | Notes |
|-------|---------|-------|
| `YEAR(d)` | Same | |
| `MONTH(d)` | Same | |
| `DAY(d)` | `DAYOFMONTH(d)` | |
| `DAYOFMONTH(d)` | Same | Alias for EXTRACT(DAY FROM d) |
| `DAYOFWEEK(d)` | Same | Alias for EXTRACT(DOW FROM d); 1=Sunday…7=Saturday |
| `HOUR(ts)` | Same | |
| `MINUTE(ts)` | Same | |
| `SECOND(ts)` | Same | |
| `CURRENT_TIMESTAMP` | `NOW()` | |

#### JSON functions

Feldera uses a `VARIANT` type for JSON. Core functions:

| Function | Purpose |
|----------|---------|
| `PARSE_JSON(string)` | JSON string → `VARIANT` |
| `TO_JSON(variant)` | `VARIANT` → JSON string |
| `CAST(variant AS type)` | Extract typed value from VARIANT |

VARIANT access patterns:

| Pattern | Syntax | Example |
|---------|--------|---------|
| Object field | `variant['key']` | `v['name']` |
| Nested field | Chain brackets | `v['user']['id']` |
| Array element | `variant[index]` | `v[0]` |
| Dot syntax | `variant.field` | `v.name` |
| Case-sensitive | Quote field name | `v."lastName"` |

Results are always `VARIANT` — cast to get a concrete type: `CAST(v['age'] AS INT)`.

| Spark | Feldera | Notes |
|-------|---------|-------|
| `parse_json(s)` | `PARSE_JSON(s)` | Returns VARIANT |
| `to_json(v)` | `TO_JSON(v)` | |
| `get_json_object(s, '$.a.b')` | `CAST(PARSE_JSON(s)['a']['b'] AS VARCHAR)` | `$.items[0]` → `['items'][0]`; skip PARSE_JSON if col is already VARIANT |
| `from_json(s, schema)` | `PARSE_JSON(s)['field']` + CAST | Use bracket access per field |
| `json_tuple(s, k1, k2)` | `PARSE_JSON(s) AS v, CAST(v['k1'] AS VARCHAR)` | Parse once, reuse alias |
| `json_array_length(s)` | `CARDINALITY(CAST(PARSE_JSON(s) AS VARIANT ARRAY))` | |

Notes:
- Parse once, reuse: `SELECT PARSE_JSON(col) AS v, CAST(v['key'] AS VARCHAR) AS k` — lateral column aliases supported.
- For performance-critical paths, `CREATE TYPE` + `jsonstring_as_<type>` is more efficient than `PARSE_JSON` + bracket access.

#### Math functions

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
| `EXP(x)` | Same | Input/output: DOUBLE |
| `SIGN(x)` | Same | Input/output: DECIMAL; returns -1, 0, or 1 |

#### Null handling

| Spark | Feldera | Notes |
|-------|---------|-------|
| `COALESCE(a, b)` | Same | |
| `NVL(a, b)` | `COALESCE(a, b)` | NVL not supported in Feldera |
| `NULLIF(a, b)` | Same | |
| `IFNULL(a, b)` | Same | Equivalent to COALESCE(left, right) |
| `a <=> b` | Same | Null-safe equality — returns true when both sides are NULL |

#### Window functions (general)

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

The `QUALIFY` clause filters rows based on a window function result — supported directly in Feldera:
```sql
SELECT *, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
FROM employees
QUALIFY rnk = 1
```

#### Window functions (TopK pattern only)

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

These require translation but ARE supported.

### Array / UNNEST

| Spark | Feldera | Notes |
|-------|---------|-------|
| `EXPLODE` / `LATERAL VIEW explode(arr)` | `UNNEST(arr) AS t(col)` | |
| `LATERAL VIEW OUTER explode(arr)` | `UNNEST(arr) AS t(col)` | OUTER semantics not replicated — NULL/empty array rows are dropped; add a warning |
| `LATERAL VIEW explode(map)` | `CROSS JOIN UNNEST(map) AS t(k, v)` | |
| `posexplode(arr)` | `SELECT pos, val FROM UNNEST(arr) WITH ORDINALITY AS t(val, pos)` | Feldera returns (val, pos); reorder in SELECT to match Spark |
| `inline(arr_of_structs)` | `UNNEST(arr) AS t(f1, f2, ...)` | |
| `map_entries(m)` | `CROSS JOIN UNNEST(m) AS t(k, v)` | Flatten map to rows |

### Structs

| Spark | Feldera | Notes |
|-------|---------|-------|
| `named_struct('a', v1, 'b', v2)` | `CAST(ROW(v1, v2) AS ROW(a T, b S))` | Use CAST to preserve field names |

### Aggregation

| Spark | Feldera | Notes |
|-------|---------|-------|
| `GROUPING SETS` | Same | |
| `ROLLUP(a, b)` | Same | |
| `CUBE(a, b)` | Same | |
| `grouping_id(col, ...)` | Same | Returns integer bitmask identifying which columns are aggregated |
| `PIVOT(...)` | Same or `CASE WHEN` aggregation | Feldera supports PIVOT with static columns only; use CASE WHEN for dynamic pivots |

### Date / Time

| Spark | Feldera | Notes |
|-------|---------|-------|
| `CURRENT_DATE` | `CAST(NOW() AS DATE)` | |
| `date_add(d, n)` | `d + INTERVAL 'n' DAY` | For literal n; for column n use `n * INTERVAL '1' DAY` |
| `date_sub(d, n)` | `d - INTERVAL 'n' DAY` | For literal n; for column n use `n * INTERVAL '1' DAY` |
| `datediff(end, start)` | `DATEDIFF(DAY, start, end)` | Feldera takes 3 args (unit, start, end); Spark takes 2 — argument order is also reversed |
| `months_between(end, start)` | `DATEDIFF(MONTH, start, end)` | Spark returns fractional months; Feldera returns integer months |
| `date_trunc('MONTH', d)` | `DATE_TRUNC(d, MONTH)` | |
| `date_trunc('MONTH', ts)` | `TIMESTAMP_TRUNC(ts, MONTH)` | |
| `trunc(d, 'YYYY')` | `DATE_TRUNC(d, YEAR)` | String arg → keyword unit |
| `trunc(d, 'MM')` | `DATE_TRUNC(d, MONTH)` | |
| `trunc(d, 'Q')` | `DATE_TRUNC(d, QUARTER)` | |
| `trunc(d, 'DD')` | `DATE_TRUNC(d, DAY)` | |
| `weekofyear(d)` | `EXTRACT(WEEK FROM d)` | |
| `add_months(d, n)` | `d + INTERVAL 'n' MONTH` | For literal n; for column n use `n * INTERVAL '1' MONTH` |
| `last_day(d)` | `DATE_TRUNC(d, MONTH) + INTERVAL '1' MONTH - INTERVAL '1' DAY` | |
| `MAKE_DATE(y, m, d)` | `PARSE_DATE('%Y-%m-%d', CONCAT(CAST(y AS VARCHAR), '-', RIGHT(CONCAT('0', CAST(m AS VARCHAR)), 2), '-', RIGHT(CONCAT('0', CAST(d AS VARCHAR)), 2)))` | Zero-pads month/day; years < 1000 may produce wrong results |
| `unix_timestamp(ts)` | `EXTRACT(EPOCH FROM ts)` | |
| `unix_millis(ts)` | `CAST(EXTRACT(EPOCH FROM ts) * 1000 AS BIGINT)` | |
| `unix_micros(ts)` | `CAST(EXTRACT(EPOCH FROM ts) * 1000000 AS BIGINT)` | |
| `from_unixtime(n)` | `MAKE_TIMESTAMP(n)` | Define UDF first: `CREATE FUNCTION MAKE_TIMESTAMP(SECONDS BIGINT) RETURNS TIMESTAMP AS TIMESTAMPADD(SECOND, SECONDS, DATE '1970-01-01')` |
| `to_timestamp(n)` (numeric) | `TIMESTAMPADD(SECOND, n, DATE '1970-01-01')` | |
| `to_timestamp(s[, fmt])` | `PARSE_TIMESTAMP(fmt, s)` | Argument order reversed; default fmt is `%Y-%m-%d %H:%M:%S`; translate Java fmt to strftime |
| `to_date(ts)` / `to_date(ts, fmt)` | `CAST(ts AS DATE)` | Format param ignored |
| `date_format(d, fmt)` | `FORMAT_DATE(fmt, d)` | DATE only; argument order reversed; Spark uses Java patterns (yyyy-MM-dd), Feldera uses strftime (%Y-%m-%d); no TIMESTAMP equivalent |

### String

| Spark | Feldera | Notes |
|-------|---------|-------|
| `LPAD(s, n, pad)` | `CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(REPEAT(pad, n-LENGTH(s)), s) END` | |
| `RPAD(s, n, pad)` | `CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(s, REPEAT(pad, n-LENGTH(s))) END` | |
| `LOCATE(substr, str)` | `POSITION(substr IN str)` | |
| `LOCATE(substr, str, pos)` | `CASE WHEN POSITION(substr IN SUBSTRING(str, pos)) = 0 THEN 0 ELSE POSITION(substr IN SUBSTRING(str, pos)) + pos - 1 END` | |
| `startswith(s, prefix)` | `LEFT(s, LENGTH(prefix)) = prefix` | |
| `endswith(s, suffix)` | `RIGHT(s, LENGTH(suffix)) = suffix` | |
| `translate(s, from, to)` | Chain of `REGEXP_REPLACE` per character | e.g. `translate(s, 'aei', '123')` → `REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(s, 'a', '1'), 'e', '2'), 'i', '3')` |

### Math

| Spark | Feldera | Notes |
|-------|---------|-------|
| `pmod(a, b)` | `MOD(MOD(a, b) + b, b)` | Always non-negative result |
| `isnan(x)` | `IS_NAN(x)` | |
| `log2(x)` | `LOG(x, 2)` | |
| `log(base, x)` | `LOG(x, base)` | **Arg order reversed**: Spark `log(base, value)` → Feldera `LOG(value, base)` |
| `try_divide(a, b)` | `CASE WHEN b = 0 OR b IS NULL THEN NULL ELSE a / b END` | |

### Null handling

| Spark | Feldera | Notes |
|-------|---------|-------|
| `nvl2(x, a, b)` | `CASE WHEN x IS NOT NULL THEN a ELSE b END` | |

### Joins

| Spark | Feldera | Notes |
|-------|---------|-------|
| `JOIN ... USING (col)` | `JOIN ... ON left.col = right.col` | Explicit `ON` is safer and avoids ambiguity |
| `NATURAL JOIN` | `JOIN ... ON (explicit matching columns)` | Always rewrite to explicit `ON` conditions |
| `LEFT ANTI JOIN ... ON cond` | `WHERE NOT EXISTS (SELECT 1 FROM ... WHERE cond)` | |

### UNNEST details

Feldera `UNNEST` expands an array or map into rows:

```sql
-- Array
FROM table, UNNEST(array_col) AS alias(col_name)
-- Map (two columns)
FROM table CROSS JOIN UNNEST(map_col) AS alias(key_col, val_col)
-- With position index
FROM table, UNNEST(array_col) WITH ORDINALITY AS alias(val_col, pos_col)
```

Key rules:
- `UNNEST` on NULL returns no rows (empty).
- For arrays of ROW values, UNNEST expands struct fields as output columns automatically.
- Always provide explicit column aliases.
- `WITH ORDINALITY`: ordinal column comes **after** the value column — opposite of Spark's `posexplode` which puts position first; reorder in SELECT.

`LATERAL VIEW OUTER explode` — Feldera has no OUTER equivalent; rows where the array is NULL/empty will be dropped. Add a warning when translating OUTER.

Multi-level unnest example:
```sql
-- Spark
SELECT o.id, item.product_id, item.qty
FROM orders o LATERAL VIEW inline(o.line_items) item AS product_id, qty

-- Feldera
SELECT o.id, item.product_id, item.qty
FROM orders o, UNNEST(o.line_items) AS item(product_id, qty)
```

When NOT to use UNNEST:
- `EXPLODE(sequence(...))` for date range generation — `sequence()` is unsupported, mark entire pattern unsupported.
- `LATERAL VIEW OUTER` when preserving NULL rows is required — add a warning diagnostic.

### DATE_ADD / DATE_SUB — wrong forms to avoid

`date_add` and `date_sub` take exactly 2 args: `(date_expr, integer_days)` → `expr ± INTERVAL 'n' DAY`. Do NOT emit these:

```sql
DATE_ADD(d, 30, 'DAY')                  -- 3-arg hybrid: wrong
DATE_ADD(d, INTERVAL '30' DAY, 'DAY')   -- 3-arg hybrid: wrong
DATE_ADD(DAY, 30, d)                    -- TIMESTAMPADD signature: wrong
```

For unit-first 3-arg form use `TIMESTAMPADD(DAY, 30, d)` — it is a different function.

### named_struct field access

`named_struct('a', v1, 'b', v2)` → `CAST(ROW(v1, v2) AS ROW(a T, b S))` when field names matter, or plain `ROW(v1, v2)` when they don't.

- Named ROW field access: `row_val.field_name`
- Anonymous ROW field access (no CAST): `row_val[1]` (1-based index)

```sql
-- Spark
SELECT source, COUNT(DISTINCT named_struct('l', left_id, 'r', right_id)) AS unique_pair_count
FROM pair_events GROUP BY source

-- Feldera (anonymous ROW)
SELECT source, COUNT(DISTINCT ROW(left_id, right_id)) AS unique_pair_count
FROM pair_events GROUP BY source

-- Feldera (named fields via CAST)
SELECT source, COUNT(DISTINCT CAST(ROW(left_id, right_id) AS ROW(l BIGINT, r BIGINT))) AS unique_pair_count
FROM pair_events GROUP BY source
```

## Unsupported (no Feldera equivalent)

### Rules for unsupported constructs

When an unsupported construct is found:

1. Translate the rest of the query if possible (schema, other supported expressions).
2. List each unsupported construct in the `unsupported` array with a brief explanation.
3. If the entire query depends on the unsupported construct, set `feldera_query` to an empty string.
4. Do NOT enter the repair loop for known-unsupported functions.

Do NOT:
- Approximate with a different function that changes semantics.
- Keep retrying after a compiler "No match found" error for a known-unsupported function.
- Silently change semantics to make the SQL compile.

### Partial translation

If a query has both supported and unsupported parts, translate what you can:

- Schema (`CREATE TABLE`) can almost always be translated.
- If only one expression in a SELECT is unsupported, note it but translate the rest.
- If the unsupported construct is central (e.g., the main aggregation), return the schema only.

```sql
-- Only UPPER is translatable; SOUNDEX is unsupported
SELECT id, UPPER(name) AS name, SOUNDEX(name) AS sound FROM users
-- → translate UPPER(name), mark SOUNDEX as unsupported
```

### Unsupported function list

#### String

| Function | Notes |
|----------|-------|
| `substring_index` | No equivalent |
| `REGEXP_EXTRACT` | Do NOT approximate with REGEXP_REPLACE |
| `SOUNDEX` | Phonetic function — not supported |
| `find_in_set` | No equivalent |
| `parse_url` | No equivalent |
| `levenshtein(s1, s2)` | Edit distance — not supported |
| `format_string(fmt, args)` / `printf(fmt, args)` | Printf-style formatting — not supported |
| `str_to_map` | No equivalent |

#### Date / Time

| Function | Notes |
|----------|-------|
| `next_day(d, dayOfWeek)` | No equivalent |
| `sequence(start, stop)` | Date/array range generation — not supported |
| `make_timestamp(y,mo,d,h,mi,s)` | No built-in; rewrite as `CAST(CONCAT(y,'-',mo,'-',d,' ',h,':',mi,':',s) AS TIMESTAMP)` or `PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CONCAT(...))` |

#### Aggregate

| Function | Notes |
|----------|-------|
| `approx_count_distinct`, `APPROX_DISTINCT` | Approximate aggregates not supported |
| `percentile_approx`, `approx_percentile` | Approximate aggregates not supported |
| `CORR` | Statistical aggregate — not supported |

#### Window

| Function | Notes |
|----------|-------|
| `PERCENT_RANK`, `CUME_DIST` | Not implemented |
| `NTILE` | Not implemented |
| `NTH_VALUE` | Not implemented |
| `ROW_NUMBER()` / `RANK()` / `DENSE_RANK()` without TopK | Must be in subquery with WHERE filter on result |
| `RANGE BETWEEN INTERVAL ... PRECEDING` | Frame bounds must be non-negative constant integers; INTERVAL not allowed — use `ROWS BETWEEN N PRECEDING AND CURRENT ROW` |

#### Higher-order functions (lambda)

| Function | Notes |
|----------|-------|
| `filter(arr, lambda)` | Compiler rejects — no equivalent |
| `aggregate(arr, init, lambda)` | No equivalent |
| `forall(arr, lambda)` | No equivalent |
| `zip_with(a, b, lambda)` | No equivalent |
| `map_filter(m, lambda)` | No equivalent |
| `transform_keys(m, lambda)` | No equivalent |
| `transform_values(m, lambda)` | No equivalent |

#### Array

| Function | Notes |
|----------|-------|
| `flatten(nested_arr)` | No equivalent |
| `arrays_zip(a, b)` | No equivalent |
| `slice(arr, start, len)` | No equivalent |

#### Map

| Function | Notes |
|----------|-------|
| `map_concat(m1, m2)` | No equivalent |

#### Bitwise (scalar)

| Function | Notes |
|----------|-------|
| `bit_and(a, b)`, `bit_or(a, b)`, `bit_xor(a, b)` | Scalar 2-arg form not supported; only aggregate `BIT_AND/OR/XOR(col)` exists |
| `shiftleft`, `shiftright`, `shiftrightunsigned` | No bitwise shift syntax in Feldera |

#### Math

| Function | Notes |
|----------|-------|
| `width_bucket(v, min, max, n)` | No equivalent |
| `try_add(a, b)`, `try_multiply(a, b)` | No equivalent; use `CASE WHEN` for overflow-safe patterns |

#### Hashing / Encoding

| Function | Notes |
|----------|-------|
| `SHA`, `SHA2`, `SHA256` | Not supported; MD5 is the only supported hash function |
| `base64`, `unbase64` | Not built-in; can be implemented as a Rust UDF |

#### JSON / CSV

| Function | Notes |
|----------|-------|
| `json_object_keys` | No equivalent |
| `schema_of_json`, `schema_of_csv` | Schema inference — not supported |
| `from_csv`, `to_csv` | CSV serialization — not supported |

#### DDL / Structural

| Function | Notes |
|----------|-------|
| `stack()` | Unpivot via function — not supported |
| `UNPIVOT` | SQL syntax not documented; rewrite as `UNION ALL` of individual column projections |
| `INSERT OVERWRITE` | Not supported |

## Compiler error fixes

When the Feldera compiler rejects translated SQL, check these common causes first:

| Error message | Cause | Fix |
|--------------|-------|-----|
| `Invalid number of arguments to function 'DATE_ADD'` | Emitted 3-arg form | Use 2-arg: `col + INTERVAL '30' DAY` |
| `No match found for function signature DATEDIFF` | Kept Spark's 2-arg form | Use 3-arg: `DATEDIFF(DAY, start_expr, end_expr)` |
| `No match found for function signature day(<TIMESTAMP>)` | Used `DAY(ts)` on a TIMESTAMP | Use `DAYOFMONTH(ts)` or `EXTRACT(DAY FROM ts)` |
| `No match found for function signature X` | Function is unsupported | Check this reference; if listed as unsupported, return immediately — do NOT retry |
| `Encountered "<" ... ARRAY<VARCHAR>` | Used Spark array syntax | Rewrite ALL `ARRAY<T>` to `T ARRAY` suffix form |
| `Error parsing SQL: Encountered ", ColumnName"` | Column name is a SQL reserved word | Double-quote the column name in schema and all query references, e.g. `"TimeStamp"` |
| `PRIMARY KEY cannot be nullable: column 'x' has type T, which is nullable` | PK column missing `NOT NULL` | Add `NOT NULL` to every column listed in the PRIMARY KEY |

## Important rules

- Do NOT hallucinate restrictions that don't exist (e.g., "Multiple RANK aggregates per window" is NOT an error).
- You CAN combine LAG, LEAD, SUM OVER, etc. in the same query — no restriction.
