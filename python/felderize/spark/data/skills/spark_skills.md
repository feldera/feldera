---
name: spark-skills
description: Master Spark-to-Feldera translation reference. The single skill file covering all function mappings, type syntax, DDL rewrites, and unsupported features. Consult this before marking anything as unsupported.
---

# Spark-to-Feldera Translation Reference

## Purpose

Consult this reference BEFORE translating any Spark SQL. It covers DDL rewrites, type mappings, function mappings, and unsupported features.

All Spark function signatures in this file are from the **Apache Spark SQL reference** (`spark.apache.org/docs/latest/api/sql/index.html`). When in doubt about a function signature or argument order, the Apache SQL reference is the authority.

- Do NOT hallucinate restrictions that don't exist (e.g., "Multiple RANK aggregates per window" is NOT an error).
- You CAN combine LAG, LEAD, SUM OVER, etc. in the same query — no restriction.

## General Behavioral Differences

These are systematic differences between Spark and Feldera to be aware of during translation. **If any of these apply to the query being translated, add a `-- NOTE:` comment in the output SQL explaining the difference to the user.** This is important so the user understands where results may differ between Spark and Feldera.

- **[GBD-WHITESPACE] Whitespace definition:** Spark treats `' '` (space), `\t` (tab), `\n` (newline), `\r` (carriage return), and other Unicode whitespace as "whitespace" in any operation that involves trimming or whitespace-awareness. Feldera follows the SQL standard and only considers ASCII space (0x20) as whitespace. This affects `TRIM`, `LTRIM`, `RTRIM`, `CAST(str AS BOOLEAN)`, and any other function that implicitly strips whitespace. If the input may contain `\t` or `\n` at the edges, the results will differ.

- **[GBD-INT-DIV] Integer division:** When both operands are integers, Spark returns DOUBLE (e.g. `95/100 = 0.95`); Feldera performs integer division (e.g. `95/100 = 0`). Cast at least one operand to DOUBLE when fractional results are needed.

- **[GBD-AGG-TYPE] Aggregate return types on numeric inputs:** Spark often widens numeric aggregates to DOUBLE regardless of input type; Feldera follows the SQL standard and preserves the input type. Key cases:
  - `AVG(integer_col)` — Spark returns DOUBLE (`AVG(1,2)` = `1.5`); Feldera returns INT (`AVG(1,2)` = `1`). No rewrite possible.
  - `STDDEV_SAMP/STDDEV_POP(decimal_col)` — Spark widens to DOUBLE; Feldera preserves DECIMAL scale. No rewrite possible.
  - `AVG(decimal_col)` — Spark returns `DECIMAL(p+4, s+4)`; Feldera returns `DECIMAL(p,s)` (same scale). No rewrite possible.

- **[GBD-DIV-ZERO] Division by zero / overflow:** Spark typically returns `NULL` or `Infinity` for division by zero and silently wraps on overflow. Feldera drops the row. `try_divide`/`try_add`/`try_subtract`/`try_multiply` all cannot be safely rewritten — mark unsupported.

### Output differences (not actionable in translation, but affect downstream consumers)

- **[GBD-FLOAT-FMT] FLOAT display format:** Spark exposes full 32-bit IEEE 754 representation when outputting FLOAT columns (e.g. `99.99` stored as FLOAT becomes `99.98999786376953`). Feldera outputs the rounded decimal (`99.99`). Both store the same bit pattern.

- **[GBD-NAN] NaN values:** Spark represents IEEE 754 NaN as `nan` in output; Feldera serializes it as `None`. Both represent the same floating point state.

- **[GBD-FP-PREC] Floating point precision:** Intermediate rounding differs between JVM (Spark) and Rust (Feldera). Mathematically exact results may have tiny noise in one system but not the other (e.g. `log(27,3)` = `3.0000000000000004` in Spark vs `3` in Feldera).

- **[GBD-ARRAY-ORDER] Array function element order:** Feldera's set-based array functions (`ARRAY_UNION`, `ARRAY_EXCEPT`) return elements in sorted order. Spark preserves the original element order. Results contain the same elements but may be in different positions.

## Type Mappings

| Spark | Feldera | Notes |
|-------|---------|-------|
| `STRING` | `VARCHAR` | Feldera uses VARCHAR for variable-length strings |
| `TEXT` | `VARCHAR` | Same as STRING |
| `INT` / `INTEGER` | `INT` | Same |
| `BIGINT` | `BIGINT` | Same |
| `BOOLEAN` | `BOOLEAN` | Same |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | Same |
| `FLOAT` | `REAL` | Feldera uses REAL instead of FLOAT. → [GBD-FLOAT-FMT] |
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

**CRITICAL: Never invent a PRIMARY KEY.** Only carry over a PRIMARY KEY that is explicitly declared in the Spark schema (e.g. `CONSTRAINT pk PRIMARY KEY (col)` or bare `PRIMARY KEY (col)`). Do NOT add a PRIMARY KEY just because a column is named `id`, `transaction_id`, or similar — if the Spark schema has no PRIMARY KEY, the Feldera schema must have none either. Adding a spurious PRIMARY KEY causes Feldera to deduplicate rows, silently corrupting aggregation results.

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

Quote column names that clash with SQL reserved words (e.g. `timestamp`, `date`, `time`, `value`, `type`, `name`, `year`) — apply consistently in both `CREATE TABLE` and every query reference. Do not quote ordinary identifiers — quoting non-reserved words makes them case-sensitive and adds noise.

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

`COUNT`, `SUM`, `MIN`, `MAX`, `COUNT(DISTINCT ...)`, `bool_or`, `bool_and` work identically in Feldera — no translation needed.

| Spark | Feldera | Notes |
|-------|---------|-------|
| `AVG(col)` | Same | → [GBD-AGG-TYPE]: integer input returns INT not DOUBLE; decimal input preserves scale |
| `STDDEV_SAMP(col)` | Same | → [GBD-AGG-TYPE]: decimal input preserves scale, not widened to DOUBLE |
| `STDDEV_POP(col)` | Same | → [GBD-AGG-TYPE]: decimal input preserves scale, not widened to DOUBLE |
| `every(col)` | Same | Alias for `bool_and` — supported in Feldera |
| `some(col)` | Same | Supported in Feldera |
| `any(col)` | `bool_or(col)` | `any` is a reserved keyword in Feldera — rewrite as `bool_or` |
| `bit_and(col)` — aggregate | `BIT_AND(col)` | Aggregate: bitwise AND over all rows in group |
| `bit_or(col)` — aggregate | `BIT_OR(col)` | Aggregate: bitwise OR over all rows in group |
| `bit_xor(col)` — aggregate | `BIT_XOR(col)` | Aggregate: bitwise XOR over all rows in group |

#### String functions

`UPPER`, `LOWER`, `LENGTH`, `SUBSTRING`, `CONCAT`, `CONCAT_WS`, `REPLACE`, `REGEXP_REPLACE`, `INITCAP`, `REVERSE`, `REPEAT`, `LEFT`, `RIGHT`, `MD5`, `ASCII`, `CHR` work identically in Feldera — no translation needed.

| Spark | Feldera | Notes |
|-------|---------|-------|
| `TRIM(s)` | Same | → [GBD-WHITESPACE] |
| `LTRIM(s)` | `TRIM(LEADING FROM s)` | Feldera does not support single-arg `LTRIM`. → [GBD-WHITESPACE] |
| `RTRIM(s)` | `TRIM(TRAILING FROM s)` | Feldera does not support single-arg `RTRIM`. → [GBD-WHITESPACE] |
| `RLIKE(s, pattern)` | Same | Infix `s RLIKE pattern` also works |
| `OCTET_LENGTH(s)` | Same | Returns byte length of string |
| `overlay(str placing repl from pos for len)` | `OVERLAY(str PLACING repl FROM pos FOR len)` | Same syntax |
| `SPLIT_PART(s, delimiter, n)` | `SPLIT_PART(s, delimiter, n)` | Feldera does not support negative indices |

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
| `array_union(a, b)` | `ARRAY_UNION(a, b)` | → [GBD-ARRAY-ORDER]: Feldera returns elements in sorted order; Spark preserves input order. |
| `array_intersect(a, b)` | `ARRAY_INTERSECT(a, b)` | |
| `array_except(a, b)` | `ARRAY_EXCEPT(a, b)` | → [GBD-ARRAY-ORDER]: Feldera returns elements in sorted order; Spark preserves input order. |
| `array_join(arr, sep)` | `ARRAY_JOIN(arr, sep)` | Alias for ARRAY_TO_STRING |
| `size(arr)` | `CARDINALITY(arr)` | Different name. **WARNING**: Spark `size()` returns `-1` for NULL input; Feldera `CARDINALITY` returns `NULL`. If input may be NULL, mark unsupported. |
| `array(v1, v2)` | `ARRAY(v1, v2)` | |

#### Higher-order array functions

All higher-order array functions (`transform`, `filter`, `exists`, `zip_with`, etc.) are unsupported in Feldera — see Unsupported section.

#### Map functions

| Spark | Feldera | Notes |
|-------|---------|-------|
| `map_keys(m)` | `MAP_KEYS(m)` | |
| `map_values(m)` | `MAP_VALUES(m)` | |

#### Date/Time functions

`YEAR`, `MONTH`, `HOUR`, `MINUTE`, `SECOND` work identically in Feldera — no translation needed.

| Spark | Feldera | Notes |
|-------|---------|-------|
| `DAY(d)` | `DAYOFMONTH(d)` | |
| `DAYOFMONTH(d)` | Same | Alias for EXTRACT(DAY FROM d) |
| `DAYOFWEEK(d)` | Same | Alias for EXTRACT(DOW FROM d); 1=Sunday…7=Saturday |
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
| `json_array_length(s)` | `CARDINALITY(CAST(PARSE_JSON(s) AS VARIANT ARRAY))` | |

Notes:
- **Feldera does NOT support lateral aliases in SELECT.** You CANNOT reference an alias defined in the same SELECT list. Always repeat `PARSE_JSON(col)` per field, or use a CTE.

Simple SELECT — repeat PARSE_JSON per field:

```sql
SELECT
  CAST(PARSE_JSON(payload)['user_id']  AS VARCHAR) AS user_id,
  CAST(PARSE_JSON(payload)['amount']   AS DOUBLE)  AS amount,
  CAST(PARSE_JSON(payload)['currency'] AS VARCHAR) AS currency
FROM raw_events;
```

- **With GROUP BY: use a CTE to pre-parse.** Repeat PARSE_JSON in GROUP BY too, or use a CTE. The CTE goes *inside* `CREATE VIEW ... AS`, not before it:

```sql
CREATE VIEW summary AS
WITH parsed AS (
  SELECT *, PARSE_JSON(payload) AS v FROM raw_events
)
SELECT
  CAST(v['user_id'] AS VARCHAR) AS user_id,
  COUNT(*) AS cnt
FROM parsed
GROUP BY CAST(v['user_id'] AS VARCHAR);
```


#### Math functions

`ABS`, `POWER`, `SQRT` work identically in Feldera — no translation needed.

| Spark | Feldera | Notes |
|-------|---------|-------|
| `CEIL(x)` | Same | For float/double input: Spark returns BIGINT, Feldera returns DOUBLE |
| `FLOOR(x)` | Same | For float/double input: Spark returns BIGINT, Feldera returns DOUBLE |
| `ROUND(x, d)` | Same | Rounding differs: Spark rounds half-up (0.5 → 1), Feldera rounds half-to-even (0.5 → 0, 1.5 → 2) |
| `BROUND(x, d)` | Same | Banker's rounding (half-to-even); Feldera supports decimal only |
| `MOD(a, b)` | Same | Feldera supports integer only; Spark also supports DECIMAL |
| `LN(x)` | Same | Spark returns NULL for 0 or negative input; Feldera returns -inf for 0 and drops the row (WorkerPanic) for negative |
| `LOG10(x)` | Same | Spark returns NULL for 0 or negative input; Feldera returns -inf for 0 and drops the row (WorkerPanic) for negative |
| `EXP(x)` | Same | Input/output: DOUBLE |
| `SIGN(x)` | Same | Returns -1, 0, or 1 |

#### Null handling

`COALESCE` works identically in Feldera — no translation needed.

| Spark | Feldera | Notes |
|-------|---------|-------|
| `NULLIF(a, b)` | Same | |
| `IFNULL(a, b)` | Same | Equivalent to COALESCE(left, right) |
| `NVL(a, b)` | `COALESCE(a, b)` | NVL not supported in Feldera |
| `ZEROIFNULL(x)` | `COALESCE(x, 0)` | |
| `NULLIFZERO(x)` | `NULLIF(x, 0)` | |
| `a <=> b` | Same | Null-safe equality — returns true when both sides are NULL |

#### Window functions (general)

`LAG`, `LEAD`, `SUM`, `AVG`, `COUNT`, `MIN`, `MAX` window functions work identically in Feldera and can be freely combined in the same query.

| Spark | Feldera | Notes |
|-------|---------|-------|
| `FIRST_VALUE(expr) OVER (...)` | Same | IGNORE NULLS not supported; ROWS/RANGE frame clauses not supported — partition must be unbounded |
| `LAST_VALUE(expr) OVER (...)` | Same | IGNORE NULLS not supported; ROWS/RANGE frame clauses not supported — partition must be unbounded |

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

### JSON

| Spark | Feldera | Notes |
|-------|---------|-------|
| `get_json_object(s, '$.key')` | `CAST(PARSE_JSON(s)['key'] AS VARCHAR)` | String values only — returns NULL for numbers/booleans. Array paths (`$.tags[0]`) not supported. |
| `json_tuple(s, k1, k2) AS (c1, c2)` | `CAST(PARSE_JSON(s)['k1'] AS VARCHAR) AS c1, CAST(PARSE_JSON(s)['k2'] AS VARCHAR) AS c2` | String values only — returns NULL for numbers/booleans. |

### Array / UNNEST

| Spark | Feldera | Notes |
|-------|---------|-------|
| `EXPLODE` / `LATERAL VIEW explode(arr)` | `UNNEST(arr) AS t(col)` | |
| `LATERAL VIEW OUTER explode(arr)` | `UNNEST(arr) AS t(col)` | OUTER semantics not replicated — NULL/empty array rows are dropped; add a warning |
| `LATERAL VIEW explode(map)` | `CROSS JOIN UNNEST(map) AS t(k, v)` | |
| `posexplode(arr)` | `SELECT pos - 1 AS pos, val FROM UNNEST(arr) WITH ORDINALITY AS t(val, pos)` | Spark pos is **0-based**; `WITH ORDINALITY` is 1-based — subtract 1. Reorder: Spark outputs (pos, col); Feldera UNNEST yields (val, pos). |
| `inline(arr_of_structs)` | `UNNEST(arr) AS t(f1, f2, ...)` | |

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
| `PIVOT(COUNT(col) FOR x IN ('A', 'B', 'C'))` | `NULLIF(COUNT(CASE WHEN x = 'A' THEN col END), 0) AS "A", ...` | Spark PIVOT returns NULL for pivot values with no matching rows; plain `COUNT(CASE WHEN ...)` returns 0. Use `NULLIF(..., 0)` to match Spark's NULL semantics. Example: `SELECT region, NULLIF(COUNT(CASE WHEN priority = 'LOW' THEN case_id END), 0) AS "LOW", NULLIF(COUNT(CASE WHEN priority = 'MEDIUM' THEN case_id END), 0) AS "MEDIUM" FROM t GROUP BY region` |

### Date / Time

| Spark | Feldera | Notes |
|-------|---------|-------|
| `CURRENT_DATE` | `CAST(NOW() AS DATE)` | |
| `date_add(d, n)` | `d + INTERVAL 'n' DAY` | For literal n; for column n use `n * INTERVAL '1' DAY` |
| `date_sub(d, n)` | `d - INTERVAL 'n' DAY` | For literal n; for column n use `n * INTERVAL '1' DAY` |
| `datediff(end, start)` | `DATEDIFF(DAY, start, end)` | Feldera takes 3 args (unit, start, end); Spark takes 2 — argument order is also reversed |
| `months_between(end, start[, roundOff])` | `DATEDIFF(MONTH, start, end)` | **WARNING**: Spark returns fractional months rounded to 8 digits (e.g. `3.94959677`); Feldera returns integer months. Add warning if result is used in arithmetic. Optional `roundOff` arg not supported — mark unsupported if present. |
| `date_trunc('MONTH', d)` | `DATE_TRUNC(d, MONTH)` | |
| `date_trunc('MONTH', ts)` | `TIMESTAMP_TRUNC(ts, MONTH)` | |
| `trunc(d, 'YYYY'/'YY')` | `DATE_TRUNC(d, YEAR)` | String arg → keyword unit |
| `trunc(d, 'MM'/'MON'/'MONTH')` | `DATE_TRUNC(d, MONTH)` | |
| `trunc(d, 'DD')` | `DATE_TRUNC(d, DAY)` | |
| `weekofyear(d)` | `EXTRACT(WEEK FROM d)` | |
| `add_months(d, n)` | `d + INTERVAL 'n' MONTH` | For literal n; for column n use `n * INTERVAL '1' MONTH` |
| `last_day(d)` | `DATE_TRUNC(d, MONTH) + INTERVAL '1' MONTH - INTERVAL '1' DAY` | |
| `MAKE_DATE(y, m, d)` | `PARSE_DATE('%Y-%m-%d', CONCAT(CAST(y AS VARCHAR), '-', RIGHT(CONCAT('0', CAST(m AS VARCHAR)), 2), '-', RIGHT(CONCAT('0', CAST(d AS VARCHAR)), 2)))` | Zero-pads month/day; years < 1000 may produce wrong results |
| `unix_timestamp(ts)` | `EXTRACT(EPOCH FROM ts)` | **Behavioral difference:** Spark interprets TIMESTAMP values in the session's local timezone when converting to epoch seconds; Feldera treats TIMESTAMP as UTC. Results will differ by the timezone offset of the Spark session. |
| `unix_millis(ts)` | `CAST(EXTRACT(EPOCH FROM ts) * 1000 AS BIGINT)` | **Behavioral difference:** Spark interprets TIMESTAMP in session's local timezone; Feldera treats as UTC. Results differ by timezone offset. |
| `unix_micros(ts)` | `CAST(EXTRACT(EPOCH FROM ts) * 1000000 AS BIGINT)` | Same timezone caveat as unix_millis. |
| `from_unixtime(n[, fmt])` | `TIMESTAMPADD(SECOND, n, DATE '1970-01-01')` | **Type difference**: Spark returns VARCHAR (`'2024-01-15 10:30:00'`); Feldera rewrite returns TIMESTAMP. No-format case is safe for timestamp arithmetic. If `fmt` arg is used (e.g. `from_unixtime(n, 'yyyy-MM-dd')`), mark unsupported — no direct Feldera equivalent. |
| `make_timestamp(y,mo,d,h,mi,s)` | `PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', CONCAT(CAST(y AS VARCHAR), '-', RIGHT(CONCAT('0', CAST(mo AS VARCHAR)), 2), '-', RIGHT(CONCAT('0', CAST(d AS VARCHAR)), 2), ' ', RIGHT(CONCAT('0', CAST(h AS VARCHAR)), 2), ':', RIGHT(CONCAT('0', CAST(mi AS VARCHAR)), 2), ':', RIGHT(CONCAT('0', CAST(s AS VARCHAR)), 2)))` | Pads all components to 2 digits |
| `to_timestamp(s[, fmt])` | `PARSE_TIMESTAMP(fmt, s)` | Argument order reversed; default fmt is `%Y-%m-%d %H:%M:%S`; translate Java fmt to strftime |
| `to_date(str, fmt)` | `PARSE_DATE(strptime_fmt, str)` | Translate Java fmt → strftime (e.g. `yyyy-MM-dd` → `%Y-%m-%d`). Use `PARSE_DATE` (NOT `PARSE_TIMESTAMP`) — `PARSE_TIMESTAMP` panics on date-only strings. No-format variant: `CAST(str AS DATE)`. |
| `date_format(d, fmt)` | date-only fmt: `FORMAT_DATE(strftime_fmt, CAST(d AS DATE))`; time-component fmt: use `CONCAT` + `FORMAT_DATE` for date part + `EXTRACT` for time parts, padding each with `CONCAT(REPEAT('0', 2-LENGTH(CAST(EXTRACT(HOUR FROM d) AS VARCHAR))), CAST(EXTRACT(HOUR FROM d) AS VARCHAR))` | Arg order reversed; Java → strftime. **Critical:** `FORMAT_DATE` always truncates time (treats input as DATE). `FORMAT_TIMESTAMP` does not exist. `LPAD` does not work for this. For formats containing `HH`, `mm`, `ss` etc., use CONCAT+EXTRACT: `date_format(ts, 'yyyy-MM-dd HH:mm')` → `CONCAT(FORMAT_DATE('%Y-%m-%d', CAST(ts AS DATE)), ' ', CONCAT(REPEAT('0', 2-LENGTH(CAST(EXTRACT(HOUR FROM ts) AS VARCHAR))), CAST(EXTRACT(HOUR FROM ts) AS VARCHAR)), ':', CONCAT(REPEAT('0', 2-LENGTH(CAST(EXTRACT(MINUTE FROM ts) AS VARCHAR))), CAST(EXTRACT(MINUTE FROM ts) AS VARCHAR)))`. |

### CAST

#### Supported / rewritable

| Spark | Feldera | Notes |
|-------|---------|-------|
| `CAST(string AS DATE)` | `CAST(string AS DATE)` | Pass through unchanged — Feldera accepts this syntax even for invalid strings. Spark returns NULL for invalid inputs; Feldera may throw at runtime, but the translation is valid. |
| `CAST(string AS TIMESTAMP)` | `CAST(string AS TIMESTAMP)` | Pass through unchanged — same rules as CAST to DATE. |
| `CAST(numeric AS TIMESTAMP)` | `CAST(numeric AS TIMESTAMP)` | Pass through unchanged — Feldera compiler accepts this syntax including edge cases like `CAST(CAST('inf' AS DOUBLE) AS TIMESTAMP)`. Do not mark as unsupported based on runtime semantics — if it compiles, translate it. |
| `CAST('<value>' AS INTERVAL <unit>)` | `INTERVAL '<value>' <unit>` | **Constant strings only** — drop the CAST, use interval literal directly. Works for single-unit (`INTERVAL '<n>' DAY`) and compound (`INTERVAL '<n>' YEAR TO MONTH`). Spark sometimes wraps the value: `CAST("interval '3-1' year to month" AS interval year to month)` — extract just the numeric part: `INTERVAL '3-1' YEAR TO MONTH`. Never works for columns or expressions — mark those unsupported. |
| `CAST(INTERVAL '...' <unit> AS <numeric>)` | Same | **SUPPORTED — do not mark unsupported.** Single-unit interval → numeric; pass through unchanged. Feldera compiler accepts this. Works for any single time unit (SECOND, MINUTE, HOUR, DAY, MONTH, YEAR) and any numeric target (DECIMAL, TINYINT, SMALLINT, INT, BIGINT). Examples: `CAST(INTERVAL '1' YEAR AS TINYINT)` ✓, `CAST(INTERVAL '10.5' SECOND AS DECIMAL)` ✓, `CAST(INTERVAL -'10.5' SECOND AS DOUBLE)` ✓. **YEAR is a single unit** — `INTERVAL '1' YEAR` is single-unit, not compound. Only compound intervals (`YEAR TO MONTH`, `DAY TO SECOND`) to numeric are unsupported. |

### String

| Spark | Feldera | Notes |
|-------|---------|-------|
| `LPAD(s, n[, pad])` | `CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(REPEAT(pad, n-LENGTH(s)), s) END` | `pad` is optional in Spark (defaults to space `' '`). For 2-arg form use `REPEAT(' ', n-LENGTH(s))`. |
| `RPAD(s, n[, pad])` | `CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(s, REPEAT(pad, n-LENGTH(s))) END` | `pad` is optional in Spark (defaults to space `' '`). For 2-arg form use `REPEAT(' ', n-LENGTH(s))`. |
| `LOCATE(substr, str)` | `POSITION(substr IN str)` | |
| `LOCATE(substr, str, pos)` | `CASE WHEN POSITION(substr IN SUBSTRING(str, pos)) = 0 THEN 0 ELSE POSITION(substr IN SUBSTRING(str, pos)) + pos - 1 END` | **CRITICAL when `pos` is itself a `LOCATE(...)` expression:** keep `substr` distinct from the inner expression. E.g. `LOCATE('.', email, LOCATE('@', email))` → `CASE WHEN POSITION('.' IN SUBSTRING(email, POSITION('@' IN email))) = 0 THEN 0 ELSE POSITION('.' IN SUBSTRING(email, POSITION('@' IN email))) + POSITION('@' IN email) - 1 END`. Never substitute the wrong character for `substr`. |
| `startswith(s, prefix)` | `LEFT(s, LENGTH(prefix)) = prefix` | String args only — if either arg is a binary (`x'...'`) literal, mark unsupported (`LEFT` does not work on binary types) |
| `endswith(s, suffix)` | `RIGHT(s, LENGTH(suffix)) = suffix` | String args only — binary args unsupported for the same reason |
| `contains(s, sub)` | `POSITION(sub IN s) > 0` | Returns NULL if either arg is NULL. Works with `x'...'` binary literals too — `contains(x'aa', x'bb')` → `POSITION(x'bb' IN x'aa') > 0` |
| `BIT_LENGTH(s)` | `OCTET_LENGTH(s) * 8` | Returns bit length; Feldera has OCTET_LENGTH (bytes) |
| `translate(s, from, to)` | Chain of `REGEXP_REPLACE` per character | e.g. `translate(s, 'aei', '123')` → `REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(s, 'a', '1'), 'e', '2'), 'i', '3')`. **WARNING**: `REGEXP_REPLACE` treats each character as a regex pattern — escape special regex chars (`.` → `[.]`, `*` → `[*]`, etc.) if they appear in the `from` string. If `from` contains many special chars, mark unsupported. |

### Math

| Spark | Feldera | Notes |
|-------|---------|-------|
| `pmod(a, b)` | `CASE WHEN MOD(a, b) < 0 AND b > 0 THEN MOD(a, b) + b ELSE MOD(a, b) END` | With positive divisor: result is always ≥ 0. With negative divisor: result has same sign as dividend. |
| `isnan(x)` | `IS_NAN(x)` | |
| `log2(x)` | `LOG(x, 2)` | |
| `log(base, x)` | `LOG(x, base)` | **CRITICAL — arg order reversed. ALWAYS swap both arguments.** Spark: first arg=base, second arg=value. Feldera: first arg=value, second arg=base. `LOG(a, b)` in Spark → `LOG(b, a)` in Feldera — mechanical swap, no exceptions, regardless of column names. Examples: `LOG(amplitude, 2)` in Spark → `LOG(2, amplitude)` in Feldera. `LOG(col, 10)` in Spark → `LOG(10, col)` in Feldera. Do NOT be misled by column alias names like `log2_col` — follow the rule, not the alias. |
| `sec(x)` | `CASE WHEN x IS NULL THEN NULL ELSE 1.0 / COS(x) END` | **ALWAYS rewrite** — Feldera has no `sec`. Works for literals too: `sec(0)` → `CASE WHEN 0 IS NULL THEN NULL ELSE 1.0 / COS(0) END`. Never leave `sec(...)` in the output. |
| `csc(x)` | `CASE WHEN x IS NULL THEN NULL ELSE 1.0 / SIN(x) END` | **ALWAYS rewrite** — Feldera has no `csc`. Works for literals too. Never leave `csc(...)` in the output. |
| `cot(x)` | `CASE WHEN x IS NULL THEN NULL ELSE 1.0 / TAN(x) END` | **ALWAYS rewrite** — Feldera has no `cot`. Works for literals too. Never leave `cot(...)` in the output. Note: `cot(0)` = `1/TAN(0)` = division by zero — Spark returns `inf`, Feldera returns NULL. |
| `positive(x)` | `x` | Unary no-op |
| `negative(x)` | `-x` | Unary negation |

### Null handling

| Spark | Feldera | Notes |
|-------|---------|-------|
| `nvl2(x, a, b)` | `CASE WHEN x IS NOT NULL THEN a ELSE b END` | |

### Joins

| Spark | Feldera | Notes |
|-------|---------|-------|
| `JOIN ... USING (col)` | `JOIN ... ON left.col = right.col` | Explicit `ON` is safer and avoids ambiguity |
| `NATURAL JOIN` | `JOIN ... ON (explicit matching columns)` | Always rewrite to explicit `ON` conditions. **CRITICAL: never use `SELECT *`** — Feldera keeps both copies of the join key, while Spark deduplicates it and places it first. Instead, list all columns explicitly, including the join key only once: `SELECT t1.join_key, t1.other_cols, t2.other_cols FROM t1 JOIN t2 ON t1.join_key = t2.join_key`. |
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

### SQL Syntax differences

#### Hex binary literals

`x'...'` hex binary literals (e.g. `x'537061726b'`) are **supported** in Feldera. Do NOT mark them unsupported. They can be used in POSITION, comparisons, and binary-typed expressions.

```sql
-- Supported
SELECT POSITION(x'537061726b' IN x'537061726b2053514c') > 0;
SELECT contains(x'aa', x'bb');  -- rewrite: POSITION(x'bb' IN x'aa') > 0
```

What is NOT supported: passing `x'...'` to VARCHAR-expecting functions like `RPAD(varchar, n, x'...')` — the type mismatch fails.

#### VALUES as table source

Spark allows `FROM VALUES (...)` directly; Feldera requires VALUES wrapped in parentheses with an alias.

| Spark | Feldera |
|-------|---------|
| `SELECT a FROM VALUES (1), (2) AS t(a)` | `SELECT a FROM (VALUES (1), (2)) AS t(a)` |
| `SELECT a FROM VALUES (1, 2) AS T(a, b)` | `SELECT a FROM (VALUES (1, 2)) AS T(a, b)` |

**Rule**: always wrap `VALUES (...)` in parentheses: `FROM (VALUES ...) AS alias(cols)`.

#### SELECT with GROUP BY / HAVING but no FROM

Spark allows `SELECT expr GROUP BY ... HAVING ...` with no table. Feldera requires a FROM clause. Add a dummy single-row table:

```sql
-- Spark (valid)
SELECT 1 GROUP BY COALESCE(1, 1) HAVING COALESCE(1, 1) = 1;

-- Feldera (add dummy FROM)
SELECT 1 AS col1 FROM (VALUES (1)) AS t(x) GROUP BY COALESCE(1, 1) HAVING COALESCE(1, 1) = 1;
```

**Rule**: when `GROUP BY` or `HAVING` is present but there is no `FROM` clause, add `FROM (VALUES (1)) AS t(x)`.

#### Operators

| Spark | Feldera | Notes |
|-------|---------|-------|
| `a == b` | `a = b` | Spark allows `==` for equality; use `=` in Feldera |
| `GROUP BY ALL` | Expand to explicit column list | Replace with all non-aggregate SELECT expressions. E.g. `SELECT a, b+1, SUM(c) ... GROUP BY ALL` → `GROUP BY a, b+1` |

#### Scalar subquery shorthand in HAVING / WHERE

Spark allows `(SELECT expr)` with no FROM clause as a shorthand. When `expr` references only outer-query columns, strip the wrapper:

```sql
-- Spark
HAVING (SELECT col1.a = 1)   →  HAVING col1.a = 1
WHERE  (SELECT x > 0)        →  WHERE  x > 0
```

**Rule**: if `(SELECT expr)` has no `FROM`, rewrite as just `expr`. If the subquery has a `FROM` clause, leave it as-is — it is a regular or correlated subquery, not this shorthand.

`(SELECT 1 WHERE cond)` with no FROM returns 1 if `cond` is true, NULL otherwise. So `a == (SELECT 1 WHERE cond)` simplifies to `a = 1 AND cond`:

```sql
-- Spark
HAVING MAX(col2) == (SELECT 1 WHERE MAX(col2) = 1)
-- Feldera: both sides must equal 1, so simplify to:
HAVING MAX(col2) = 1
```

#### SELECT alias chaining

Spark allows a later expression in SELECT to reference an alias defined earlier. Feldera does not — expand the reference.

```sql
-- Spark: valid
SELECT col1 AS a, a AS b FROM t
-- Feldera: rewrite
SELECT col1 AS a, col1 AS b FROM t
```

**Rule**: if an expression is just an alias name from an earlier column (`a AS b` where `a` was defined as `col1 AS a`), replace with the original expression (`col1 AS b`).

#### SELECT alias shadowing in HAVING / GROUP BY

Spark resolves HAVING/GROUP BY references to the **source column** even when a SELECT alias has the same name. Feldera resolves to the **SELECT alias**.

**Fix**: rename the alias to avoid the collision (e.g. prefix with `agg_` or `out_`):

```sql
-- Problematic: alias b shadows source column b
SELECT SUM(a) AS b FROM T(a, b) GROUP BY GROUPING SETS ((b)) HAVING b > 10
-- Fixed
SELECT SUM(a) AS sum_a FROM T(a, b) GROUP BY GROUPING SETS ((b)) HAVING b > 10
```

#### Struct field access

In ORDER BY or HAVING, always prefix struct column access with the table alias to avoid misparsing:

```sql
ORDER BY t.col.field   -- correct
ORDER BY col.field     -- may fail (Feldera misparses as table.column)
```

Non-aggregate HAVING with struct field → move to WHERE:

```sql
-- Spark
SELECT col1 FROM t GROUP BY col1 HAVING col1.a = 1
-- Feldera
SELECT col1 FROM t WHERE t.col1.a = 1 GROUP BY col1
```

#### Null-safe equality

| Spark | Feldera | Notes |
|-------|---------|-------|
| `equal_null(a, b)` | `a <=> b` | Returns true when both sides equal OR both NULL |

## Unsupported (no Feldera equivalent)

### Rules for unsupported constructs

When an unsupported construct is found:

1. If any part of a query is unsupported, treat the entire query as unsupported.
2. List each unsupported construct in the `unsupported` array with a brief explanation.
3. If the entire query depends on the unsupported construct, set `feldera_query` to an empty string.
4. Do NOT enter the repair loop for known-unsupported functions.

Do NOT:
- Approximate with a different function that changes semantics.
- Keep retrying after a compiler "No match found" error for a known-unsupported function.
- Silently change semantics to make the SQL compile.

### Partial translation

If any part of a query is unsupported, treat the **entire query as unsupported**. Do not emit a partial view — return the schema only (no `CREATE VIEW`) and list the unsupported constructs.

Rationale: a partial translation produces incorrect results, which is worse than no result.

### Unsupported function list

#### String

| Function | Notes |
|----------|-------|
| `substring(str, -n)` / `substr(str, -n)` | Negative position counts from end in Spark (`substring('Spark SQL', -3)` → `'SQL'`); Feldera returns full string or wrong result. Mark unsupported when position is negative. |
| `DECODE(expr, s1, r1, s2, r2, ..., default)` | Not supported in Feldera — DECODE uses Oracle-style NULL-safe equality where `NULL = NULL` is TRUE; a correct rewrite requires `IS NOT DISTINCT FROM` instead of `=`, which Feldera does not support. A naive `CASE WHEN expr = s1` rewrite silently breaks NULL-matching branches. |
| `substring_index` | No equivalent |
| `uuid()` | Non-deterministic — not supported in Feldera |
| `contains(bool, ...)` | `contains()` does not work on boolean args in Feldera — mark unsupported |
| `startswith(x'...', x'...')` / `endswith(x'...', x'...')` | Binary hex literal args not supported — `LEFT`/`RIGHT` do not work on binary types |
| `to_number(str, fmt)` | Numeric parsing with format string — not supported in Feldera |
| `to_binary(str, fmt)` | Binary conversion function — not supported in Feldera |
| `luhn_check(str)` | Luhn algorithm check — not supported in Feldera |
| `is_valid_utf8(str)` | UTF-8 validation — not supported in Feldera |
| `validate_utf8(str)` / `try_validate_utf8(str)` / `make_valid_utf8(str)` | UTF-8 validation/repair — not supported in Feldera |
| `quote(str)` | SQL quoting function — not supported in Feldera |
| `split(str, delim, limit)` | 3-argument form with limit — not supported. Use 2-argument `SPLIT(str, delim)` instead (drops the limit). |
| `hex(x)` | Binary hex encoding — not supported in Feldera |
| `unhex(s)` | Binary hex decoding — not supported in Feldera |
| `encode(str, charset)` / `decode(bytes, charset)` | Binary encode/decode — not supported in Feldera |
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
| `date_trunc('WEEK', ts)` / `date_trunc('WEEK', d)` / `trunc(d, 'WEEK')` | Spark truncates to Monday; Feldera truncates to Sunday — different first-day-of-week convention. Results differ by 1 day. |
| `next_day(d, dayOfWeek)` | No equivalent |
| `quarter(d)` | QUARTER extraction — not supported in Feldera |
| `trunc(d, 'Q')` / `date_trunc('QUARTER', d)` | DATE_TRUNC(d, QUARTER) compiles but fails at runtime — Feldera sqllib missing quarter implementation |
| `sequence(start, stop)` | Date/array range generation — not supported |
| `TIMESTAMP_NTZ` | Timezone-naive timestamp type — replace with `TIMESTAMP` in DDL; Feldera has only one timestamp type |
| Spark type suffix literals: `1Y`, `122S`, `10L`, `100BD` | Spark shorthand type suffixes (Y=tinyint, S=smallint, L=bigint, BD=decimal) not valid syntax in Feldera |

#### CAST

| Spark | Notes |
|-------|-------|
| `CAST(interval <n> unit1 <m> unit2 AS string)` | Feldera output format (`+3-01`) differs from Spark (`INTERVAL '3-1' YEAR TO MONTH`) — mark unsupported |
| `CAST(x AS INTERVAL)` | Bare `INTERVAL` without a unit — parse error, always unsupported |
| `CAST(column AS INTERVAL ...)` | Column/expression-to-interval — not supported; only constant string literals can be rewritten (see CAST section above) |
| `SELECT CAST(... AS INTERVAL ...)` as final output | `INTERVAL` cannot be a view column output type — mark unsupported even if the literal rewrite applies. Do NOT use `CREATE LOCAL VIEW` as a workaround — it changes semantics. |
| `CAST(INTERVAL 'x-y' YEAR TO MONTH AS numeric)` | Compound interval (YEAR TO MONTH, DAY TO SECOND, HOUR TO SECOND) to numeric — not supported |
| `CAST(str AS BOOLEAN)` where `str` contains `\t`, `\n`, `\r` whitespace | → [GBD-WHITESPACE] — Feldera only strips spaces before parsing, so `'\t\t true \n\r '` returns `False`. Mark unsupported if input may contain non-space whitespace. |
| `CAST(numeric AS INTERVAL ...)` | Numeric-to-interval — not supported |
| `CAST(TIME '...' AS numeric/decimal)` | TIME to numeric or decimal — not supported |

#### Aggregate

| Function | Notes |
|----------|-------|
| `collect_list(col)` | `array_agg` does not preserve insertion order — result is sorted, not order-preserving like Spark's `collect_list` |
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
| `ROWS BETWEEN ... AND ...` / `RANGE BETWEEN ...` | Custom window frames are not supported — Feldera only supports whole-partition windows (no ROWS/RANGE frame clauses) |

#### Higher-order functions (lambda)

| Function | Notes |
|----------|-------|
| `filter(arr, lambda)` | Compiler rejects — no equivalent |
| `transform(arr, lambda)` | No equivalent |
| `exists(arr, lambda)` | No equivalent |
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
| `map_entries(m)` | Returns an array of `{key, value}` structs — no equivalent in Feldera |
| `map_concat(m1, m2)` | No equivalent |
| `map_contains_key(m, k)` | No equivalent |

#### Bitwise (scalar)

All scalar bitwise operators are unsupported in Feldera — no scalar equivalent exists for any of them.

| Function | Notes |
|----------|-------|
| `a \| b` | Bitwise OR — `\|` is a parse error in Feldera |
| `a & b` | Bitwise AND — parses but "Not yet implemented" |
| `a ^ b` | Bitwise XOR — parses but "Not yet implemented" |
| `shiftleft(a, n)` / `shiftright(a, n)` / `shiftrightunsigned(a, n)` | Bitwise shift — no equivalent |

#### Math

| Function | Notes |
|----------|-------|
| `try_divide(a, b)` | Spark always returns DOUBLE and returns NULL on divide-by-zero; Feldera integer division returns INT and drops the row on divide-by-zero — not safely rewritable → [GBD-DIV-ZERO] |
| `try_add(a, b)` | Spark returns NULL on overflow; Feldera drops the row — not safely rewritable → [GBD-DIV-ZERO] |
| `try_subtract(a, b)` | Spark returns NULL on overflow; Feldera drops the row — not safely rewritable → [GBD-DIV-ZERO] |
| `try_multiply(a, b)` | Spark returns NULL on overflow; Feldera drops the row — not safely rewritable → [GBD-DIV-ZERO] |
| `width_bucket(v, min, max, n)` | No equivalent |
| `a DIV b` | No equivalent; integer division operator not supported — use `FLOOR(a / b)` as a suggestion only, semantics differ for negatives |
| `RAND()` | No equivalent; random number function not supported |

#### Hashing / Encoding

| Function | Notes |
|----------|-------|
| `SHA`, `SHA2`, `SHA256` | Not supported; MD5 is the only supported hash function |
| `base64`, `unbase64` | Not built-in; can be implemented as a Rust UDF |

#### JSON / CSV

| Function | Notes |
|----------|-------|
| `from_json(s, schema)` | Spark returns a single struct column; Feldera has no struct output equivalent |
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

