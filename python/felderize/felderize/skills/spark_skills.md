---
name: spark-skills
description: >
  Master Spark-to-Feldera SQL translation reference. Covers all function
  mappings, type syntax, DDL rewrites, behavioral differences, and unsupported
  features. Consult this before marking anything as unsupported.
---

# Spark-to-Feldera Translation Reference

## Purpose

Consult this reference BEFORE translating any Spark SQL. It covers DDL rewrites, type mappings, function mappings, and unsupported features.

All Spark function signatures in this file are from the **Apache Spark SQL reference** (`spark.apache.org/docs/latest/api/sql/index.html`). When in doubt about a function signature or argument order, the Apache SQL reference is the authority.

<!-- EXAMPLE VOCABULARY: All SQL examples in this file must use only generic names from the lists below. Never use real schema, table, or column names from any specific customer or dataset.
  Tables:  orders, customers, products, events, employees, departments, sales, items, logs, sessions, accounts, payments, transactions
  Columns: id, user_id, customer_id, order_id, product_id, amount, price, qty, status, ts, created_at, name, val, x, a, b, col, col1, col2, region, category, score, tag, flag, str, n
  Schemas/catalogs: prod_warehouse, analytics, public, audit
-->

## General Behavioral Differences

These are systematic differences between Spark and Feldera to be aware of during translation. **If any of these apply to the query being translated, add a `-- NOTE:` comment in the output SQL explaining the difference to the user.** This is important so the user understands where results may differ between Spark and Feldera.

- **[GBD-WHITESPACE] Whitespace in string values:** Spark treats `' '` (space), `\t` (tab), `\n` (newline), `\r` (carriage return), and other Unicode whitespace characters as "whitespace" when operating on CHAR/VARCHAR/STRING values. Feldera follows the SQL standard and only considers ASCII space (0x20) as whitespace. This affects `TRIM`, `LTRIM`, `RTRIM`, and any other function that implicitly strips whitespace from string values. If the input may contain `\t` or `\n` at the edges, the results will differ. Note: `CAST(str AS BOOLEAN)` is NOT affected — Feldera correctly handles non-space whitespace in boolean strings.

- **[GBD-CAST-TRIM] CAST from string auto-trims in Spark:** When casting a VARCHAR/STRING expression to a numeric type (`TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `FLOAT`, `DOUBLE`, `DECIMAL`) or a date/time type (`DATE`, `TIMESTAMP`), Spark automatically strips surrounding whitespace (spaces, tabs, newlines) before parsing. Feldera does not — leading/trailing whitespace causes a runtime error. **Always wrap the string argument with `TRIM` before casting:** `CAST(TRIM(str) AS TINYINT)`, `CAST(TRIM(str) AS DATE)`, etc. Apply whenever the source expression is VARCHAR or STRING, including column references and string literals.

- **[GBD-INT-DIV] Integer division:** When both operands are integers, Spark always performs floating point division and returns DOUBLE (e.g. `95/100 = 0.95`); Feldera performs integer division (e.g. `95/100 = 0`). Cast at least one operand to DOUBLE when fractional results are needed: `CAST(col AS DOUBLE) / divisor`.

- **[GBD-DIV-OPERATOR] Spark `div` operator (floor division):** `a div b` in Spark is floor integer division — it truncates toward negative infinity and always returns an integer. Feldera does not support the `div` keyword. Translate `a div b` as `FLOOR(CAST(a AS DECIMAL(20,0)) / b)` — always cast at least one operand to DECIMAL to ensure true division before flooring (e.g. `FLOOR(CAST(51 AS DECIMAL(20,0)) / 2)` = 25, `FLOOR(CAST(-7 AS DECIMAL(20,0)) / 2)` = -4). Do NOT use `FLOOR(a / b)` with integer operands — Feldera integer division truncates toward zero, so `FLOOR(-7 / 2)` = -3, not -4.

- **[GBD-AGG-TYPE] Aggregate return types on numeric inputs:** Spark often widens numeric aggregates regardless of input type; Feldera preserves the input type. Key cases:
  - `AVG(integer_col)` — Spark returns DOUBLE (`AVG(1,2)` = `1.5`); Feldera returns INT (`AVG(1,2)` = `1`). **Rewrite: `AVG(CAST(col AS DECIMAL(38,6)))`** only when the input type is confirmed integer (INT, BIGINT, SMALLINT, TINYINT) — derive from schema or column definition. If the type cannot be determined, leave as-is and flag [GBD-AGG-TYPE].
  - `STDDEV_SAMP/STDDEV_POP(col)` — Spark always returns DOUBLE; Feldera preserves the input type. **Rewrite: `STDDEV_SAMP(CAST(col AS DECIMAL(38,6)))`** only when the input type is confirmed non-DOUBLE (INT, BIGINT, SMALLINT, TINYINT, DECIMAL).
  - `AVG(decimal_col)` — Spark returns `DECIMAL(p+4, s+4)`; Feldera preserves the input type and returns `DECIMAL(p,s)`. **Rewrite: `AVG(CAST(col AS DECIMAL(p+4, s+4)))`** — Feldera's AVG preserves the input type, so pre-casting to the target precision/scale matches Spark's output. Only valid if `p+4 ≤ 38` (Feldera's maximum DECIMAL precision); otherwise leave as-is and flag [GBD-AGG-TYPE].
  - **NOTE (precision):** Spark actually returns DOUBLE (~15–17 significant digits) for `AVG(integer)` and for `STDDEV_*`. The `DECIMAL(38,6)` rewrite above keeps a stable, exact decimal type but only carries 6 fractional digits, so results can differ from Spark in the trailing digits (e.g. `AVG(1,2,4)` → `2.333333` vs Spark `2.3333333333333335`). This is an accepted trade-off; expect minor precision mismatches on repeating/irrational results. (Casting to `DOUBLE` instead would match Spark exactly but yields a floating type.)

- **[GBD-DIV-ZERO] Division by zero / overflow:** Spark returns `NULL` for integer division by zero and silently wraps on overflow. Feldera panics on integer division by zero or overflow. Floating-point division by zero returns `Infinity` in both engines.
  - **`try_divide(a, b)`** → `div_null(a, b)` — Feldera's `div_null` returns NULL on divide-by-zero, matching Spark's `try_divide` semantics. Always use this rewrite.
  - **`try_add` / `try_subtract` / `try_multiply`** → mark unsupported; no safe equivalent in Feldera.

### Output differences (not actionable in translation, but affect downstream consumers)

- **[GBD-FLOAT-FMT] FLOAT display format:** Spark and Feldera may produce different string representations of floating-point values because the decimal representation of a binary float can be periodic (infinite). Both store the same bit pattern; they simply apply different rounding when converting to string.


- **[GBD-FP-PREC] Floating point precision:** Spark (JVM) and Feldera (Rust) use different algorithms for rounding when converting floating-point values to strings, so results can differ. Results may also differ between CPU architectures (Intel vs ARM).

- **[GBD-ARRAY-ORDER] Array element order not guaranteed:** Feldera does not preserve insertion order for `ARRAY_AGG`, `collect_list`, `ARRAY_UNION`, `ARRAY_EXCEPT`, `ARRAY_INTERSECT`. Spark preserves input order; Feldera may return elements in any order.
  - For `ARRAY_AGG` and `collect_list`: if a deterministic order is required, use `ARRAY_AGG(col ORDER BY col)` — Feldera supports `ORDER BY` inside `ARRAY_AGG`. Otherwise translate normally and **add a NOTE comment**: `-- NOTE: [GBD-ARRAY-ORDER] element order in this array may differ from Spark — Feldera does not guarantee insertion order.`
  - For `ARRAY_UNION`, `ARRAY_EXCEPT`, `ARRAY_INTERSECT`: no order-preserving rewrite is possible — translate normally and always **add a NOTE comment**: `-- NOTE: [GBD-ARRAY-ORDER] element order in this array may differ from Spark — Feldera does not guarantee insertion order.`

- **[GBD-FLOAT-GROUP] ORDER BY on NULL values:** When a `REAL` or `DOUBLE` column may contain `NULL`, sorting that column with `ORDER BY` yields a different row order in Spark and Feldera. Feldera treats `NULL` as the **smallest** value (sorts first in `ASC`, last in `DESC`); Spark treats `NULL` as the **largest** (sorts last in `ASC`, first in `DESC`). The sort order for special float values (`-Inf`, `+Inf`, `NaN`) matches between the two engines: `-Inf` < finite values < `+Inf` < `NaN`. `GROUP BY` and aggregation are unaffected; only output row order diverges. Translate the query normally, but **add a NOTE comment** above the `ORDER BY` clause: `-- NOTE: [GBD-FLOAT-GROUP] NULL sort order differs from Spark — Feldera sorts NULLs first (smallest), Spark sorts NULLs last.`

## Type Mappings

| Spark | Feldera | Notes |
|-------|---------|-------|
| `STRING` | `STRING` | Same |
| `TEXT` | `TEXT` | Same |
| `INT` / `INTEGER` | `INT` | Same |
| `BIGINT` | `BIGINT` | Same |
| `BOOLEAN` | `BOOLEAN` | Same |
| `DECIMAL(p,s)` | `DECIMAL(p,s)` | Same |
| `FLOAT` | `REAL` | Feldera uses REAL instead of FLOAT. → [GBD-FLOAT-FMT] |
| `DOUBLE` | `DOUBLE` | Same |
| `DATE` | `DATE` | Same |
| `TIMESTAMP` | `TIMESTAMP` | Same |
| `MAP<K, V>` | `MAP<K, V>` | Translate inner types |
| `BINARY` | `VARBINARY` | Spark's `BINARY` is variable-length — always translate to Feldera `VARBINARY`. **Never use Feldera `BINARY(N)`** — it is fixed-length and pads/truncates to N bytes, which does not match Spark semantics. |
| `ARRAY<T>` | `T ARRAY` | Suffix syntax; see rules below |
| `STRUCT<a: T, b: U>` | `ROW(a T, b U)` | Field access — two valid forms depending on context: (1) in SELECT/WHERE: `(col).field` with unqualified column name; (2) in ORDER BY/HAVING: `t.col.field` with table alias prefix. **Never combine both:** `(t.col).field` is invalid. |

### Boolean literals

Spark SQL uses Python-style boolean literals (`True`, `False`). Feldera uses standard SQL keywords (`TRUE`, `FALSE`). **Never quote boolean literals as strings.**

| Spark | Feldera |
|-------|---------|
| `True` | `TRUE` |
| `False` | `FALSE` |
| `col = True` | `col = TRUE` |
| `col = False` | `col = FALSE` |
| `col is true` | `col IS TRUE` |
| `col is false` | `col IS FALSE` |
| `col is not true` | `col IS NOT TRUE` |
| `col is not false` | `col IS NOT FALSE` |

❌ Wrong: `col = 'True'`, `col = 'False'`, `col = 'true'`, `col = 'false'`
✅ Correct: `col = TRUE`, `col = FALSE`

⚠️ Exception: if the Spark query already has quotes around the literal (e.g. `col = 'true'`), the column is VARCHAR — keep the string literal as-is. Do NOT convert to `TRUE`/`FALSE`. Only convert unquoted Python-style `True`/`False`.

**IMPORTANT**: When a Spark query uses `col is true` or `col is false`, translate to `col IS TRUE` / `col IS FALSE`, NOT `col = TRUE` / `col = FALSE`. NULL handling differs: `NULL IS TRUE` = `FALSE`, but `NULL = TRUE` = `NULL`.

### Array type syntax rules

Feldera uses suffix syntax — fix DDL before addressing query-level issues:

| Spark | Feldera |
|-------|---------|
| `ARRAY<T>` | `T ARRAY` |
| `ARRAY<ARRAY<T>>` | `T ARRAY ARRAY` |
| `ARRAY<ROW(...)>` | `ROW(...) ARRAY` |
| `MAP<K, ARRAY<V>>` | `MAP<K, V ARRAY>` |

#### 🔄 Translation rules

- Never emit `ARRAY<...>` in Feldera DDL — always use suffix `T ARRAY` form
- Array literals: use `ARRAY[...]` or `ARRAY(...)`

#### 📝 Notes

- Array indexes are 1-based in both engines
- Compiler error `Encountered "<" ... ARRAY<VARCHAR>`: rewrite ALL `ARRAY<...>` to suffix form first, then re-validate

## DDL Rewrites

| Spark syntax | Action |
|-------------|--------|
| `CREATE OR REPLACE TEMP VIEW` | → `CREATE VIEW` |
| `CREATE TEMPORARY VIEW` | → `CREATE VIEW` |
| `CREATE MATERIALIZED VIEW` | → `CREATE VIEW` — Feldera's `CREATE MATERIALIZED VIEW` has a different meaning (maintains a full state snapshot for ad-hoc queries, at significant storage cost); Spark's materialized view concept maps to a regular Feldera streaming `CREATE VIEW` |
| `USING parquet` / `delta` / `csv` | Remove clause |
| `PARTITIONED BY (...)` | Remove clause |
| `COMMENT '...'` on CREATE VIEW / CREATE TABLE / columns | Remove clause — Feldera does not support the COMMENT clause in DDL. |
| `WITH SCHEMA COMPENSATION` | Remove clause — Feldera does not support this Spark Delta Lake option. Do NOT mark unsupported. |
| `TIMESTAMP_NTZ` type | → `TIMESTAMP` |
| `CONSTRAINT name PRIMARY KEY (cols)` | → `PRIMARY KEY (cols)` — drop the `CONSTRAINT name` wrapper; Feldera rejects the named constraint syntax |
| PK column without `NOT NULL` | Add `NOT NULL` — Feldera requires all PRIMARY KEY columns to be NOT NULL |
| `CREATE INDEX ...` | Remove entirely |
| `CREATE TYPE name AS (...)` | Pass through as-is — Feldera supports user-defined composite types with this exact syntax. Field access: use `T.col.field` with the table name qualifier. |
| Column order in `CREATE TABLE` | **Never reorder columns.** Keep every column in the exact same position as in the source Spark schema. INSERT statements use positional values — reordering silently maps values to wrong columns. |

### PRIMARY KEY rules

#### ⚠️ Behavioral differences (Spark vs Feldera)

- Spark accepts `CONSTRAINT name PRIMARY KEY (col)` — Feldera rejects the named wrapper
- Spark allows nullable PK columns — Feldera requires all PK columns to be `NOT NULL`

#### 🔄 Translation rules

- **Never invent a PRIMARY KEY** — only carry over a PK that is explicitly declared in the Spark schema (`CONSTRAINT pk PRIMARY KEY (col)` or bare `PRIMARY KEY (col)`). Do NOT add a PK just because a column is named `id` or `transaction_id`. Adding a spurious PK causes Feldera to deduplicate rows, silently corrupting results.
- Drop the `CONSTRAINT name` wrapper — use bare `PRIMARY KEY (col)`
- Add `NOT NULL` to all PK columns

#### 📌 Example

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

#### 📝 Notes

- Compiler error when PK column is nullable: `PRIMARY KEY cannot be nullable: PRIMARY KEY column 'user_id' has type VARCHAR, which is nullable`

### Reserved words as column names must be quoted

#### ⚠️ Behavioral differences (Spark vs Feldera)

- Spark is permissive with reserved words as column names; Feldera requires them to be quoted

#### 🔄 Translation rules

- Quote column names that clash with SQL reserved words — known reserved words that appear as column names in this codebase: `timestamp`, `date`, `time`, `variant`
- Apply quoting consistently in both `CREATE TABLE`/`CREATE VIEW` column list and every query reference
- Do not quote ordinary identifiers — quoting non-reserved words makes them case-sensitive and adds noise
- Quote identifiers with SQL-standard double quotes (`"name"`), never Spark-style backticks — Feldera/Calcite rejects backticks with a lexical error. This covers view, table, and column names that contain hyphens, dots, or reserved words (a hyphenated view name such as `string-functions_142` must be written `"string-functions_142"`, not in backticks).

#### 📌 Example

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


### Qualified table names must be stripped to the unqualified name

#### ⚠️ Behavioral differences (Spark vs Feldera)

- Spark supports 3-part qualified names: `` `catalog`.`schema`.`table` `` and 2-part: `` `schema`.`table` ``
- Feldera SQL does not support catalog/schema qualification — only the bare table name is valid

#### 🔄 Translation rules

- Always strip catalog and schema prefixes; keep only the last segment (the table name)
- Apply to all table references in FROM, JOIN, and subquery clauses
- Apply to all view references in CREATE VIEW ... AS SELECT ... FROM
- Apply to the view name in `CREATE VIEW schema.view_name` — use bare `view_name` (**the last segment after the dot, NOT the schema**). Example: `CREATE VIEW analytics.sales_summary` → `CREATE VIEW sales_summary`.
- **Name collision**: If two tables from different schemas share the same unqualified name (e.g. `public.status` and `audit.status`), disambiguate by prefixing with the immediate schema. Give the bare name to the schema that appears most frequently across the query set (or the first encountered if tied); prefix the other: `public.status` → `status`, `audit.status` → `audit_status`. Use `schema_table` form only for the colliding name, not universally. Apply the rename consistently — update both the `CREATE TABLE` declaration and every query reference to the renamed table.

#### 📌 Example

```sql
-- Spark (3-part qualified)
SELECT o.order_id, o.amount
FROM `prod_warehouse`.`public`.`orders` o
JOIN `prod_warehouse`.`public`.`customers` c ON o.customer_id = c.id;

-- Feldera (unqualified)
SELECT o.order_id, o.amount
FROM orders o
JOIN customers c ON o.customer_id = c.id;
```

```sql
-- Spark (2-part qualified)
SELECT * FROM `analytics`.`events`;

-- Feldera
SELECT * FROM events;
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

| Spark | Feldera | Notes |
|-------|---------|-------|
| `AVG(col)` | `AVG(CAST(col AS DECIMAL(38,6)))` if col is integer type; `AVG(col)` otherwise | Integer input: rewrite to avoid integer truncation → [GBD-AGG-TYPE]. Decimal/float: leave as-is → [GBD-AGG-TYPE] scale mismatch |
| `STDDEV_SAMP(col)` | `STDDEV_SAMP(CAST(col AS DECIMAL(38,6)))` if col is non-DOUBLE; `STDDEV_SAMP(col)` otherwise | → [GBD-AGG-TYPE]: Spark always returns DOUBLE; Feldera preserves input type. Rewrite for any non-DOUBLE input (INT, BIGINT, SMALLINT, TINYINT, DECIMAL, REAL). |
| `STDDEV_POP(col)` | `STDDEV_POP(CAST(col AS DECIMAL(38,6)))` if col is non-DOUBLE; `STDDEV_POP(col)` otherwise | → [GBD-AGG-TYPE]: Spark always returns DOUBLE; Feldera preserves input type. Rewrite for any non-DOUBLE input (INT, BIGINT, SMALLINT, TINYINT, DECIMAL, REAL). |
| `every(col)` | Same | Alias for `bool_and` — supported as aggregate and window function. |
| `some(col)` | Same | Supported as aggregate and window function. |
| `bool_and(col)` | Same | Supported as aggregate and window function. |
| `bool_or(col)` | Same | Supported as aggregate and window function. |
| `bit_and(col)` — aggregate | Same | |
| `bit_or(col)` — aggregate | Same | |
| `bit_xor(col)` — aggregate | Same | |
| `min_by(value, key)` — aggregate | `ARG_MIN(value, key)` | Returns the `value` associated with the minimum `key`. **Only in GROUP BY context** — as a window function (`OVER`) it is not supported; mark unsupported. |
| `max_by(value, key)` — aggregate | `ARG_MAX(value, key)` | Returns the `value` associated with the maximum `key`. **Only in GROUP BY context** — as a window function (`OVER`) it is not supported; mark unsupported. |
| `GROUPING SETS` | Same | |
| `ROLLUP(a, b)` | Same | |
| `CUBE(a, b)` | Same | |
| `grouping_id(col, ...)` | Same | Returns integer bitmask identifying which columns are aggregated |

#### 📝 Notes

- `COUNT`, `MIN`, `MAX`, `COUNT(DISTINCT ...)`, `bool_or`, `bool_and` work identically — no translation needed
- `SUM`: works identically in most cases. Exception: when the input column is a narrow integer type (TINYINT, SMALLINT, INT), Spark widens the result to avoid overflow; Feldera preserves the input type. If overflow is possible, rewrite as `SUM(CAST(col AS BIGINT))` or `SUM(CAST(col AS DECIMAL(38,0)))`.

#### String functions

| Spark | Feldera | Notes |
|-------|---------|-------|
| `TRIM(s)` | Same | → [GBD-WHITESPACE] |
| `TRIM(chars FROM s)` | `TRIM(BOTH chars FROM s)` | Spark `TRIM(chars FROM s)` without a direction keyword defaults to BOTH. Always add `BOTH` explicitly in Feldera. |
| `RLIKE(s, pattern)` | Same | Infix `s RLIKE pattern` also works → [GBD-REGEX-ESCAPE] |
| `OCTET_LENGTH(s)` | Same | |
| `overlay(str placing repl from pos for len)` | Same | |

#### ⚠️ Behavioral differences (Spark vs Feldera)

- **[GBD-REGEX-ESCAPE]** `RLIKE` / `REGEXP_REPLACE` regex pattern escaping: Spark SQL string literals apply Java-style `\\` backslash escaping, so `'\\.'` passes the regex `\.` (escaped dot) to the engine. Feldera SQL follows the SQL standard and does **not** interpret `\\` as an escape, so `'\\.'` is the two-character sequence `\.` in the regex — which may match differently. Use POSIX character classes instead: `[.]` for literal dot, `[+]` for literal plus, `[*]` for literal star, etc.

#### 📝 Notes

- `UPPER`, `LOWER`, `LENGTH`, `SUBSTRING`, `CONCAT`, `CONCAT_WS`, `REPLACE`, `REGEXP_REPLACE`, `INITCAP`, `REPEAT`, `LEFT`, `RIGHT`, `MD5`, `ASCII`, `CHR` work identically — no translation needed

#### Array functions

| Spark | Feldera | Notes |
|-------|---------|-------|
| `array_contains(arr, val)` | Same | |
| `sort_array(arr)` | Same | |
| `sort_array(arr, false)` | Same | |
| `array_distinct(arr)` | Same| |
| `array_position(arr, val)` | Same | |
| `array_remove(arr, val)` | Same| |
| `arrays_overlap(a, b)` | Same| |
| `array_repeat(val, n)` | Same | |
| `array_union(a, b)` | `ARRAY_UNION(a, b)` | → [GBD-ARRAY-ORDER]: element order may differ from Spark — translate as-is and add NOTE comment per rule |
| `array_intersect(a, b)` | `ARRAY_INTERSECT(a, b)` | → [GBD-ARRAY-ORDER]: element order may differ from Spark — translate as-is and add NOTE comment per rule |
| `array_except(a, b)` | `ARRAY_EXCEPT(a, b)` | → [GBD-ARRAY-ORDER]: element order may differ from Spark — translate as-is and add NOTE comment per rule |
| `array_join(arr, sep)` | `ARRAY_JOIN(arr, sep)` | Alias for ARRAY_TO_STRING |
| `size(arr)` | `COALESCE(CARDINALITY(arr), -1)` | → [GBD-SIZE-NULL]. Do NOT mark unsupported. |
| `array_size(arr)` | `COALESCE(CARDINALITY(arr), -1)` | Alias of `size()` — same rewrite. Add a warning. Do NOT mark unsupported. |
| `get(arr, index)` | `arr[index + 1]` | Spark `get()` is **0-based**; Feldera array indexing is **1-based** — add 1 to the index. Add a warning. Do NOT mark unsupported. |
| `array(v1, v2)` | `ARRAY(v1, v2)` | |
| `sequence(start, stop)` — integer inputs | `SEQUENCE(start, stop)` | Returns `INTEGER ARRAY` from start to stop (inclusive); empty array if stop < start. ⚠️ Behavioral difference: Spark auto-selects step = -1 when start > stop and returns a descending array (e.g. `sequence(5,1)` → `[5,4,3,2,1]`); Feldera returns an empty array. Add a NOTE comment if start may exceed stop. |

#### ⚠️ Behavioral differences (Spark vs Feldera)

- **[GBD-SIZE-NULL]** `size(arr)` returns `-1` for NULL input in Spark; Feldera `CARDINALITY` returns `NULL`. Rewrite as `COALESCE(CARDINALITY(arr), -1)`. If the column is `NOT NULL`, `CARDINALITY(arr)` alone is sufficient.

#### Higher-order array functions

| Spark | Feldera | Notes |
|-------|---------|-------|
| `transform(arr, x -> expr)` | `TRANSFORM(arr, x -> expr)` | |
| `exists(arr, x -> expr)` | `ARRAY_EXISTS(arr, x -> expr)` | |

#### 📝 Notes

- `TRANSFORM(arr, x -> expr)` and `ARRAY_EXISTS(arr, x -> expr)` are **supported** — pass through (with name change for `exists`). Do NOT mark them unsupported.
- `filter`, `zip_with`, `aggregate`, `forall`, `map_filter`, `transform_keys`, `transform_values` are unsupported — see Unsupported section.

#### Map functions

| Spark | Feldera | Notes |
|-------|---------|-------|
| `map_keys(m)` | Same | → [GBD-ARRAY-ORDER]: element order may differ from Spark — translate as-is and add NOTE comment per rule |
| `map_values(m)` | Same | → [GBD-ARRAY-ORDER]: element order may differ from Spark — translate as-is and add NOTE comment per rule |
| `map_contains_key(m, k)` | Same | |

#### Date/Time functions

| Spark | Feldera | Notes |
|-------|---------|-------|
| `DAYOFMONTH(d)` | Same | Alias for EXTRACT(DAY FROM d) |
| `DAYOFWEEK(d)` | Same | Alias for EXTRACT(DOW FROM d); 1=Sunday…7=Saturday |
| `MAKE_DATE(y, m, d)` | Same | |
| `make_timestamp(y,mo,d,h,mi,s)` | Same | `s` can be any numeric type (supports sub-second precision) |
| `CURRENT_TIMESTAMP` | `NOW()` | → [GBD-NONDETERMINISTIC] |

#### ⚠️ Behavioral differences (Spark vs Feldera)

- **[GBD-NONDETERMINISTIC]** `CURRENT_TIMESTAMP` / `NOW()` — value is captured at execution time and will differ between Spark and Feldera runs

#### 📝 Notes

- `YEAR`, `MONTH`, `HOUR`, `MINUTE`, `SECOND` work identically — no translation needed
- **DATE vs TIMESTAMP in comparisons / BETWEEN**: When comparing DATE and TIMESTAMP columns (e.g. `date_col BETWEEN timestamp_col AND timestamp_col`), cast the DATE side to TIMESTAMP: `CAST(date_col AS TIMESTAMP) BETWEEN ts1 AND ts2`. **Never cast to VARCHAR** — VARCHAR comparison of dates produces wrong ordering.

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

| Spark | Feldera | Notes |
|-------|---------|-------|
| `parse_json(s)` | `PARSE_JSON(s)` | Returns VARIANT |
| `to_json(v)` | `TO_JSON(v)` | |
| `json_array_length(s)` | `CARDINALITY(CAST(PARSE_JSON(s) AS VARIANT ARRAY))` | |

#### 🔄 Translation rules

- JSON field access (e.g. `v['age']`) always returns `VARIANT` — always cast to the target type before use: `CAST(v['age'] AS INT)`, `CAST(v['name'] AS VARCHAR)`, etc.
- When translating JSON extraction: **never expose `PARSE_JSON(col) AS v` as a top-level view column** — `v` becomes a real output column of the view and does not appear in the original Spark query. Two safe patterns:

Simple SELECT — parse inline (**preferred** when only extracting fields; avoids the subquery entirely):

```sql
SELECT
  id,
  CAST(PARSE_JSON(payload)['user_id']  AS VARCHAR) AS user_id,
  CAST(PARSE_JSON(payload)['amount']   AS DOUBLE)  AS amount
FROM events;
```

Simple SELECT — lateral alias with subquery (**mandatory subquery** when using `v`; wrap the inner SELECT so `v` is hidden from the view's output columns):

```sql
SELECT id, user_id, amount FROM (
  SELECT
    id,
    PARSE_JSON(payload)                    AS v,
    CAST(v['user_id']  AS VARCHAR)         AS user_id,
    CAST(v['amount']   AS DOUBLE)          AS amount
  FROM events
);
```

**CRITICAL**: `SELECT PARSE_JSON(payload) AS v, CAST(v['x'] AS T) AS x FROM t` without the outer `SELECT x FROM (...)` wrapper is WRONG — it leaks `v` into the view output.

- **With GROUP BY:** use a CTE to pre-parse. The CTE goes *inside* `CREATE VIEW ... AS`, not before it:

```sql
CREATE VIEW summary AS
WITH parsed AS (
  SELECT *, PARSE_JSON(payload) AS v FROM events
)
SELECT
  CAST(v['user_id'] AS VARCHAR) AS user_id,
  COUNT(*) AS cnt
FROM parsed
GROUP BY CAST(v['user_id'] AS VARCHAR);
```


#### Math functions

| Spark | Feldera | Notes |
|-------|---------|-------|
| `CEIL(x)` | Same | → [GBD-CEIL-FLOOR] |
| `FLOOR(x)` | Same | → [GBD-CEIL-FLOOR] |
| `ROUND(x, d)` | Same | |
| `BROUND(x, d)` | Same | Banker's rounding (half-to-even); Feldera supports decimal only. When Spark uses `bround(col, n)::Decimal(p,s)`, translate as `CAST(BROUND(col, n) AS DECIMAL(p,s))` — the cast applies to the **result**, never the input. **Never drop the `::Decimal` cast** — always preserve it on the BROUND result. |
| `MOD(a, b)` / `a % b` | Same | Supported for both integer and DECIMAL |
| `LN(x)` | `LN(GREATEST(x, 0))` | Guard against negative input. → [GBD-LOG-DOMAIN] |
| `LOG10(x)` | `LOG10(GREATEST(x, 0))` | Guard against negative input. → [GBD-LOG-DOMAIN] |
| `EXP(x)` | Same | Input/output: DOUBLE |
| `SIGN(x)` | Same | |
| `sec(x)` | Same | |
| `csc(x)` | Same | |
| `cot(x)` | Same | |

#### ⚠️ Behavioral differences (Spark vs Feldera)

- **[GBD-CEIL-FLOOR]** `CEIL`/`FLOOR` on float/double input: Spark returns `BIGINT`; Feldera returns `DOUBLE`
- **[GBD-LOG-DOMAIN]** `LN`/`LOG10` on invalid input: Spark returns `NULL`; Feldera returns `-inf` for 0 and panics (WorkerPanic) for negative values. Always wrap the argument: `LN(GREATEST(x, 0))` / `LOG10(GREATEST(x, 0))` to prevent the panic.

#### 📝 Notes

- `ABS`, `POWER`, `SQRT` work identically — no translation needed

#### Null handling

| Spark | Feldera | Notes |
|-------|---------|-------|
| `NULLIF(a, b)` | Same | |
| `IFNULL(a, b)` | Same | Equivalent to COALESCE(left, right) |
| `a <=> b` | Same | Null-safe equality — returns true when both sides are NULL |

#### 📝 Notes

- `COALESCE` works identically — no translation needed

#### Joins

| Spark | Feldera | Notes |
|-------|---------|-------|
| `JOIN ... USING (col)` | Same | |
| `NATURAL JOIN` | Same | |

#### Window functions (general)

| Spark | Feldera | Notes |
|-------|---------|-------|
| `FIRST_VALUE(expr) OVER (...)` | Same | → [GBD-WINDOW-FIRST-LAST] |
| `LAST_VALUE(expr) OVER (...)` | Same | → [GBD-WINDOW-FIRST-LAST] |
| `SUM/AVG/... OVER (... RANGE BETWEEN ... AND ...)` | Same | `RANGE BETWEEN` fully supported |

#### ⚠️ Behavioral differences (Spark vs Feldera)

- **[GBD-WINDOW-FIRST-LAST]** `FIRST_VALUE`/`LAST_VALUE`: `IGNORE NULLS` not supported; `ROWS BETWEEN UNBOUNDED PRECEDING AND ...` is supported; bounded `ROWS BETWEEN N PRECEDING AND ...` is not supported

#### 📝 Notes

- `LAG`, `LEAD`, `SUM`, `AVG`, `COUNT`, `MIN`, `MAX` window functions work identically and can be freely combined in the same query
- The `QUALIFY` clause is **fully supported directly in Feldera** — do NOT rewrite it into a subquery. When Spark uses a subquery-then-WHERE to filter on a window result, rewrite to QUALIFY.

#### Window functions

| Spark | Feldera | Notes |
|-------|---------|-------|
| `ROW_NUMBER()` | Same | Pass through as-is in ALL contexts. **Never mark ROW_NUMBER unsupported.** |
| `RANK()` | Same | |
| `DENSE_RANK()` | Same | |

#### 📌 Example

```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY col1 ORDER BY col2 ASC, id ASC) AS rn
  FROM t
) sub
WHERE rn <= 10
```



## Rewritable patterns (Spark syntax → Feldera syntax)

These require translation but ARE supported.

### JSON

| Spark | Feldera | Notes |
|-------|---------|-------|
| `get_json_object(s, '$.key')` | `CAST(PARSE_JSON(s)['key'] AS <type>)` | Cast to the appropriate type. Array indexing supported: `$.tags[0]` → `PARSE_JSON(s)['tags'][0]`. |

### Array / UNNEST

| Spark | Feldera | Notes |
|-------|---------|-------|
| `EXPLODE` / `LATERAL VIEW explode(arr)` | `UNNEST(arr) AS t(col)` | |
| `LATERAL VIEW OUTER explode(arr)` | `UNNEST(arr) AS t(col)` | → [GBD-OUTER-EXPLODE]: OUTER semantics not replicated — add a warning |
| `LATERAL VIEW explode(map)` | `CROSS JOIN UNNEST(map) AS t(k, v)` | |
| `posexplode(arr)` | `UNNEST(arr) WITH ORDINALITY AS t(val, pos_1based)` then use `pos_1based - 1` wherever `pos` appears | Spark pos is **0-based**; `WITH ORDINALITY` is 1-based — subtract 1. Reorder: Spark outputs (pos, col); Feldera UNNEST yields (val, pos). Add a warning. Do NOT mark unsupported. |
| `inline(arr_of_structs)` | `UNNEST(arr) AS t(f1, f2, ...)` | `f1, f2, ...` are the field names of the struct element type — replace with actual field names from the schema. E.g. for `ARRAY<STRUCT<id BIGINT, name VARCHAR>>`, write `UNNEST(arr) AS t(id, name)`. |
| `inline(array(from_json(col, 'f1 T1, f2 T2'))) AS (out1, out2)` | `CAST(PARSE_JSON(col)['f1'] AS T1) AS out1, CAST(PARSE_JSON(col)['f2'] AS T2) AS out2` | Single-row struct expansion in SELECT clause — extract each field via PARSE_JSON. Add a warning. Do NOT mark unsupported. |
| `LATERAL VIEW inline(from_json(json_col, 'Array<struct<f1 T1, f2 T2>>')) AS (out1, out2)` | `, UNNEST(CAST(PARSE_JSON(json_col) AS ROW(f1 T1, f2 T2) ARRAY)) AS t(out1, out2)` | Multi-row array-of-structs expansion — replace LATERAL VIEW with comma-join UNNEST; cast JSON string to typed ROW ARRAY. Add a warning. Do NOT mark unsupported. See [GBD-INLINE-STRUCT-ARRAY]. |

#### ⚠️ Behavioral differences (Spark vs Feldera)

**[GBD-OUTER-EXPLODE]** `LATERAL VIEW OUTER explode` — Feldera has no OUTER equivalent. Rows where the array is NULL or empty are excluded from the join result (Feldera uses INNER join semantics for UNNEST; there is no LEFT JOIN / OUTER option). Add a warning when translating OUTER.

#### 📝 Notes

- `UNNEST` on NULL returns no rows (empty).
- For arrays of ROW values, UNNEST expands struct fields as output columns automatically.
- Always provide explicit column aliases.
- `WITH ORDINALITY`: ordinal column comes **after** the value column — opposite of Spark's `posexplode` which puts position first; reorder in SELECT.
- `EXPLODE(sequence(...))` for date range generation — `sequence()` is unsupported; mark the entire pattern unsupported.
- `LATERAL VIEW OUTER` — see [GBD-OUTER-EXPLODE].

#### 📌 Example

```sql
-- Spark: inline(arr_of_structs)
SELECT o.id, item.product_id, item.qty
FROM orders o LATERAL VIEW inline(o.line_items) item AS product_id, qty

-- Feldera
SELECT o.id, item.product_id, item.qty
FROM orders o, UNNEST(o.line_items) AS item(product_id, qty)
```

#### 🔄 Translation rules

```sql
-- Array
FROM table, UNNEST(array_col) AS alias(col_name)
-- Map (two columns)
FROM table CROSS JOIN UNNEST(map_col) AS alias(key_col, val_col)
-- With position index (1-based; subtract 1 to match Spark's 0-based posexplode)
FROM table, UNNEST(array_col) WITH ORDINALITY AS alias(val_col, pos_col)
```

### Structs

| Spark | Feldera | Notes |
|-------|---------|-------|
| `named_struct('a', v1, 'b', v2)` | `CAST(ROW(v1, v2) AS ROW(a T, b S))` | Use CAST to preserve field names |

### Aggregation

| Spark | Feldera | Notes |
|-------|---------|-------|
| `any(col)` | `bool_or(col)` | `any` is a reserved keyword in Feldera — rewrite as `bool_or` |
| `collect_list(col)` | `ARRAY_AGG(col)` | → [GBD-ARRAY-ORDER]: add a NOTE comment warning that element order may differ |
| `collect_list(distinct col)` | `ARRAY_DISTINCT(ARRAY_AGG(col))` | → [GBD-ARRAY-ORDER]: add a NOTE comment |
| `collect_list(struct(a, b))` | `ARRAY_AGG(ROW(a, b))` | Struct aggregate — `struct(...)` becomes `ROW(...)` inside the aggregate |
| `concat_ws(sep, array_col)` — where `array_col` is an array | `ARRAY_JOIN(array_col, sep)` | When the second arg is an array (not varargs strings). `ARRAY_JOIN` is fully supported in Feldera. Example: `concat_ws(',', sort_array(collect_list(distinct col)))` → `ARRAY_JOIN(sort_array(ARRAY_DISTINCT(ARRAY_AGG(col))), ',')` |
| `ARRAY_AGG(col)` | Same | → [GBD-ARRAY-ORDER]: add a NOTE comment warning that element order may differ |
| `first(col)` | `MAX(col)` | Feldera has no `first()`. **Semantics differ**: Spark returns an arbitrary (non-deterministic) value; `MAX` returns the maximum. Add a `-- NOTE:` warning. This is an approximation — verify the result is acceptable for the use case. |
| `last(col)` | `MAX(col)` | Feldera has no `last()`. **Semantics differ**: Spark returns an arbitrary (non-deterministic) value; `MAX` returns the maximum. Add a `-- NOTE:` warning. This is an approximation — verify the result is acceptable for the use case. |
| `count_if(cond)` | `COUNTIF(cond)` | Direct equivalent — Feldera's `COUNTIF` counts rows where the boolean condition is true. |
| `SUM(expr) FILTER (WHERE cond)` | Same | Also applies to `COUNT`, `AVG`, `MIN`, `MAX`. |
| `PIVOT(COUNT(col) FOR x IN ('A', 'B', 'C'))` | Same | Feldera supports native PIVOT — pass through unchanged when the IN-list is compile-time constants. If the IN-list is a dynamic subquery or computed expression, mark unsupported. **Never rewrite as CASE WHEN COUNT.** → [GBD-PIVOT-COUNT] |
| `UNPIVOT (val FOR col IN (c1, c2, c3))` | Same | Works in Feldera though not officially documented — pass through unchanged. Do NOT rewrite as UNION ALL. |

**[GBD-PIVOT-COUNT]** Feldera `PIVOT(COUNT(...))` returns **0** for pivot cells with no matching rows; Spark returns **NULL**. Add a warning. If exact NULL semantics are required, the PIVOT cannot be faithfully translated — mark unsupported and add a warning.

### Date / Time

| Spark | Feldera | Notes |
|-------|---------|-------|
| `DAY(d)` | `DAYOFMONTH(d)` | |
| `CURRENT_DATE` / `CURRENT_DATE()` | `CAST(NOW() AS DATE)` | Spark allows both `CURRENT_DATE` and `CURRENT_DATE()` (with parens); Feldera requires no parens → `CAST(NOW() AS DATE)`. → [GBD-NONDETERMINISTIC] |
| `date_add(d, n)` | `d + INTERVAL 'n' DAY` or `d + INTERVAL n DAY` | For literal n use either form; for column n use `d + n * INTERVAL '1' DAY` |
| `date_sub(d, n)` | `d - INTERVAL 'n' DAY` or `d - INTERVAL n DAY` | For literal n use either form; for column n use `d - n * INTERVAL '1' DAY` |
| `date_col - integer_expr` | `date_col - integer_expr * INTERVAL '1' DAY` | Spark allows subtracting an integer directly from a DATE; Feldera does not — multiply by `INTERVAL '1' DAY`. |
| `quarter(d)` | Same | |
| `trunc(d, 'Q')` / `date_trunc('QUARTER', d)` | `DATE_TRUNC(d, QUARTER)` | |
| `from_utc_timestamp(ts, zone)` | `CONVERT_TIMEZONE('UTC', zone, ts)` | Arg order changed; `zone` can be an IANA name (e.g. `'America/New_York'`) or fixed offset (e.g. `'+05:30'`). Returns TIMESTAMP. **Exceptions — do NOT apply CONVERT_TIMEZONE:** (1) When `zone` is the literal `'UTC'` the call is a no-op → use `ts` directly. (2) When the pattern is `from_utc_timestamp(to_utc_timestamp(col, tz), tz)` (round-trip to same zone) → no-op → use `col` directly, or just `NOW()` if `col` is `current_timestamp()`. (3) When the result is immediately cast to DATE with `to_date(from_utc_timestamp(...))` → use `CAST(ts AS DATE)` instead; timezone rarely changes the calendar date and CONVERT_TIMEZONE+CAST is unnecessarily complex. |
| `to_utc_timestamp(ts, zone)` | `CONVERT_TIMEZONE(zone, 'UTC', ts)` | Inverse of from_utc_timestamp. When `zone` is `'UTC'` the call is a no-op → use `ts` directly. |
| `datediff(end, start)` | `DATEDIFF(DAY, start, end)` | **Arguments are REVERSED and a unit is added.** Spark: `datediff(end, start)` — end is first, start is second. Feldera: `DATEDIFF(DAY, start, end)` — start is second arg, end is third arg. The Spark first arg becomes the Feldera LAST arg. Example: `datediff(due_date, created_at)` → `DATEDIFF(DAY, created_at, due_date)`. Both compute `end - start` in days. |
| `months_between(end, start[, roundOff])` | `DATEDIFF(MONTH, start, end)` or `DATEDIFF(DAY, start, end)` | → [GBD-MONTHS-BETWEEN] |
| `date_trunc('MONTH', d)` | `DATE_TRUNC(d, MONTH)` | |
| `date_trunc('MONTH', ts)` | `TIMESTAMP_TRUNC(ts, MONTH)` | |
| `date_trunc('WEEK', d)` / `trunc(d, 'WEEK')` | `DATE_TRUNC(d - INTERVAL '1' DAY, WEEK) + INTERVAL '1' DAY` | Feldera truncates to Sunday; Spark truncates to Monday. Subtract 1 day before truncating to handle Sundays correctly, then add 1 day. Verified correct for all 7 days of the week. |
| `trunc(d, 'YYYY'/'YY')` | `DATE_TRUNC(d, YEAR)` | String arg → keyword unit |
| `trunc(d, 'MM'/'MON'/'MONTH')` | `DATE_TRUNC(d, MONTH)` | |
| `trunc(d, 'DD')` | `DATE_TRUNC(d, DAY)` | |
| `weekofyear(d)` | `EXTRACT(WEEK FROM d)` | |
| `date_part('field', d)` | `EXTRACT(field FROM d)` | Quote is dropped and field becomes a keyword: `date_part('year', d)` → `EXTRACT(YEAR FROM d)` |
| `add_months(d, n)` | `d + INTERVAL 'n' MONTH` or `d + INTERVAL n MONTH` | For literal n use either form; for column n use `d + n * INTERVAL '1' MONTH` |
| `last_day(d)` | `DATE_TRUNC(d, MONTH) + INTERVAL '1' MONTH - INTERVAL '1' DAY` | |
| `unix_timestamp(ts)` | `EXTRACT(EPOCH FROM ts)` | → [GBD-TIMEZONE] |
| `unix_millis(ts)` | `EXTRACT(EPOCH FROM ts) * 1000 + EXTRACT(MILLISECOND FROM ts) % 1000` | → [GBD-TIMEZONE]. `EXTRACT(EPOCH FROM ts)` returns whole seconds (BIGINT); add sub-second milliseconds separately. |
| `unix_micros(ts)` | `EXTRACT(EPOCH FROM ts) * 1000000 + EXTRACT(MICROSECOND FROM ts) % 1000000` | → [GBD-TIMEZONE]. Same pattern — add sub-second microseconds to avoid losing precision. |
| `from_unixtime(n[, fmt])` | no fmt: `TIMESTAMPADD(SECOND, n, DATE '1970-01-01')`; with fmt: `FORMAT_TIMESTAMP(strftime_fmt, TIMESTAMPADD(SECOND, n, DATE '1970-01-01'))` | → [GBD-TIMEZONE]. Translate Java fmt → strftime. No-fmt rewrite returns TIMESTAMP; fmt rewrite returns VARCHAR matching Spark. |
| `to_timestamp(s[, fmt])` | `PARSE_TIMESTAMP(fmt, s)` | Argument order reversed; default fmt is `%Y-%m-%d %H:%M:%S`; translate Java fmt to Rust strftime |
| `try_to_timestamp(s[, fmt])` | `PARSE_TIMESTAMP(strptime_fmt, s)` | `PARSE_TIMESTAMP` returns NULL on invalid input strings, matching Spark's NULL-on-failure behavior. Convert the Spark format to strftime (e.g. `yyyyMMdd` → `%Y%m%d`, `HH:mm:ss` → `%H:%M:%S`). |
| `to_date(try_to_timestamp(str, fmt))` | `PARSE_DATE(strptime_fmt, str)` | Common TPC-DI pattern to parse a date from a yyyyMMdd string. `try_to_timestamp` → `PARSE_TIMESTAMP` → `PARSE_DATE` simplifies to one `PARSE_DATE`. Map `yyyyMMdd` → `%Y%m%d`. |
| `to_date(str, fmt)` | `PARSE_DATE(strptime_fmt, str)` | Translate Java fmt to Rust strftime (e.g. `yyyy-MM-dd` → `%Y-%m-%d`). Use `PARSE_DATE` (NOT `PARSE_TIMESTAMP`) — `PARSE_TIMESTAMP` panics on date-only strings. No-format variant: `CAST(str AS DATE)`. |
| `date_format(d, fmt)` — d is TIMESTAMP | `FORMAT_TIMESTAMP(strftime_fmt, d)` | Arg order reversed; translate Spark fmt to Rust strftime (`yyyy`→`%Y`, `MM`→`%m`, `dd`→`%d`, `HH`→`%H`, `hh`→`%I`, `mm`→`%M`, `ss`→`%S`, `a`→`%p`, `E`→`%a`). For date-only inputs use `FORMAT_DATE(strftime_fmt, d)`. |
| `date_format(d, 'E')` | `FORMAT_DATE('%a', d)` | Spark `'E'` = abbreviated weekday name (Sun, Mon, …); Feldera strftime `%a` gives the same 3-letter English abbreviation. |
| `date_format(d, fmt)` — d is VARCHAR time string | `FORMAT_TIME(strftime_fmt, CAST(d AS TIME))` | Use when the input is a VARCHAR time string (e.g. built with `concat(lpad(h,2,'0'),':',lpad(m,2,'0'))`). Cast to TIME first, then format. Example: `date_format(concat(lpad(h,2,'0'),':',lpad(m,2,'0')), 'hh:mm a')` → `FORMAT_TIME('%I:%M %p', CAST(CONCAT(LPAD(CAST(h AS VARCHAR),2,'0'),':',LPAD(CAST(m AS VARCHAR),2,'0')) AS TIME))`. |

#### ⚠️ Behavioral differences (Spark vs Feldera)

**[GBD-TIMEZONE]** Feldera treats `TIMESTAMP` as UTC; Spark uses the session timezone. Epoch↔timestamp conversions (`unix_timestamp`, `from_unixtime`, etc.) are only equivalent when `spark.sql.session.timeZone=UTC`.

**[GBD-MONTHS-BETWEEN]** Spark `months_between` returns fractional months; Feldera has no fractional-month equivalent. Use `DATEDIFF(MONTH, start, end)` for integer months or `DATEDIFF(DAY, start, end)` for exact day-precision — choose the unit that fits the downstream computation. Add a warning. The optional `roundOff` argument is not supported — mark unsupported if present.

### Spark functional cast syntax

Spark allows type names as functions for casting: `bigint(x)`, `string(x)`, `int(x)`, `double(x)`, `boolean(x)`, `date(x)`, `timestamp(x)`, etc. Feldera does not support this functional form — rewrite as `CAST(x AS type)`. Spark also allows PostgreSQL-style `::type` cast syntax (e.g. `expr::VARCHAR`, `expr::DECIMAL(38,5)`) — Feldera supports `::type` too, so pass through as-is.

#### `::type` — pass through (both Spark and Feldera support)

| Spark | Feldera |
|-------|---------|
| `expr::BIGINT` | Same |
| `expr::VARCHAR` | Same |
| `expr::INT` | Same |
| `expr::DOUBLE` | Same |
| `expr::BOOLEAN` | Same |
| `expr::DECIMAL(p,s)` | Same |

#### Functional cast forms — rewrite to `CAST`

| Spark | Feldera |
|-------|---------|
| `bigint(expr)` | `CAST(expr AS BIGINT)` |
| `string(expr)` | `CAST(expr AS VARCHAR)` |
| `int(expr)` | `CAST(expr AS INT)` |
| `double(expr)` | `CAST(expr AS DOUBLE)` |
| `boolean(expr)` | `CAST(expr AS BOOLEAN)` |
| `date(str)`| `CAST(str AS DATE)` |
| `timestamp(date_expr)` | `CAST(date_expr AS TIMESTAMP)` |

### CAST

| Spark | Feldera | Notes |
|-------|---------|-------|
| `CAST(str_expr AS numeric_or_date_type)` | `CAST(TRIM(BOTH ' \t\n\r' FROM str_expr) AS numeric_or_date_type)` | → [GBD-CAST-TRIM]: Spark strips all whitespace and reads `\t`/`\n` escapes; Feldera's default `TRIM` strips only spaces and treats `\` literally. The set `' \t\n\r'` (= {space, `\`, `t`, `n`, `r`}) removes leading/trailing spaces AND literal `\t`/`\n`/`\r` artifacts, preventing pipeline init failure. Safe here because a valid numeric/date operand never starts or ends with those characters. |
| `TRY_CAST(expr AS type)` | `SAFE_CAST(expr AS type)` | → [GBD-SAFE-CAST] |
| `TRY_TO_TIMESTAMP(expr)` | `SAFE_CAST(expr AS TIMESTAMP)` | Snowflake function — approximate with SAFE_CAST (returns NULL on failure for VARCHAR inputs). Add a warning. Do NOT mark unsupported. |
| `TRY_TO_DATE(expr)` | `SAFE_CAST(expr AS DATE)` | Snowflake function — approximate with SAFE_CAST (returns NULL on failure for VARCHAR inputs). Add a warning. Do NOT mark unsupported. |
| `CAST(string AS DATE)` | `SAFE_CAST(string AS DATE)` | Spark returns NULL for invalid inputs; use `SAFE_CAST` to match that behavior. |
| `CAST(string AS TIMESTAMP)` | `SAFE_CAST(string AS TIMESTAMP)` | Spark returns NULL for invalid inputs; use `SAFE_CAST` to match that behavior. |
| `CAST(numeric AS TIMESTAMP)` | `CAST(CAST(numeric AS BIGINT) * 1000 AS TIMESTAMP)` | Spark interprets numeric as seconds since epoch; Feldera as milliseconds — multiply by 1000 to convert. Cast to BIGINT first to avoid INT overflow. |
| `CAST('<value>' AS INTERVAL <unit>)` | `INTERVAL '<value>' <unit>` | For constant strings: drop the CAST, use interval literal directly (`INTERVAL '3' DAY`, `INTERVAL '3-1' YEAR TO MONTH`). For string expressions: `CAST(col AS INTERVAL DAY)` or `INTERVAL col DAY` both work in Feldera when used inside arithmetic (e.g. `d + CAST(col AS INTERVAL DAY)`). |
| `CAST(INTERVAL '...' <unit> AS <numeric>)` | Same | Pass through unchanged. Single time units (SECOND, MINUTE, HOUR, DAY, MONTH, YEAR) to any numeric type are supported — including fractional values like `INTERVAL '10.123' SECOND` (the decimal point is part of the value, NOT a compound separator). Compound intervals (`YEAR TO MONTH`, `DAY TO SECOND`) to numeric are unsupported. |

#### ⚠️ Behavioral differences (Spark vs Feldera)

**[GBD-SAFE-CAST]** ⚠️ Note: for REAL/DOUBLE/BOOLEAN targets, both `CAST` and `SAFE_CAST` return the type default (`0.0` for numeric, `false` for boolean) instead of NULL or an error on invalid input. For example, `SAFE_CAST('not_a_number' AS DOUBLE)` returns `0.0` instead of NULL. This affects `TRY_CAST` translations too. Add a warning when translating to DOUBLE or BOOLEAN targets.

**[GBD-SAFE-CAST-TIMESTAMP-ALIAS]** ⚠️ Spark's `TIMESTAMP_NTZ` / `TIMESTAMP_LTZ` map to Feldera `TIMESTAMP`. When the source is a *string* cast to one of these aliases, apply the `CAST(string AS TIMESTAMP)` → `SAFE_CAST` rule above — e.g. `CAST('a' AS TIMESTAMP_NTZ)` → `SAFE_CAST('a' AS TIMESTAMP)`, which yields NULL on an invalid string instead of panicking at runtime, matching Spark.

**[GBD-SAFE-CAST-NUMERIC-TIMESTAMP]** ⚠️ For the `CAST(numeric AS TIMESTAMP)` chain above, use `SAFE_CAST` on the cast steps when the numeric source can be non-finite or out of range (e.g. `CAST('inf' AS DOUBLE)`): `SAFE_CAST(SAFE_CAST(numeric AS BIGINT) * 1000 AS TIMESTAMP)`. A plain `CAST` of `±Infinity`/`NaN`/overflow to BIGINT or TIMESTAMP panics at runtime, whereas Spark returns NULL — `SAFE_CAST` matches that. (DOUBLE/BOOLEAN targets still follow `[GBD-SAFE-CAST]`.)

### String

| Spark | Feldera | Notes |
|-------|---------|-------|
| `IF(cond, t, f)` | Same | |
| `iff(cond, a, b)` / `IFF(cond, a, b)` | `CASE WHEN cond THEN a ELSE b END` | Spark/Snowflake alias for IF — rewrite as CASE WHEN. Do NOT mark unsupported. |
| `INSTR(str, substr)` | `POSITION(substr IN str)` | Arg order reversed: Spark `INSTR(str, substr)` → `POSITION(substr IN str)` |
| `LTRIM(s)` | `TRIM(LEADING FROM s)` | Feldera does not support single-arg `LTRIM`. → [GBD-WHITESPACE] |
| `RTRIM(s)` | `TRIM(TRAILING FROM s)` | Feldera does not support single-arg `RTRIM`. → [GBD-WHITESPACE] |
| `LPAD(s, n[, pad])` | `CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(REPEAT(pad, n-LENGTH(s)), s) END` | `pad` is optional in Spark (defaults to space `' '`). For 2-arg form use `REPEAT(' ', n-LENGTH(s))`. |
| `RPAD(s, n[, pad])` | `CASE WHEN LENGTH(s) >= n THEN SUBSTRING(s,1,n) ELSE CONCAT(s, REPEAT(pad, n-LENGTH(s))) END` | `pad` is optional in Spark (defaults to space `' '`). For 2-arg form use `REPEAT(' ', n-LENGTH(s))`. |
| `substring(str, -n)` / `substr(str, -n)` | `SUBSTR(str, -n)` | Negative position counts from end in Spark (`substring('Spark SQL', -3)` → `'SQL'`). Use `SUBSTR` — it handles negative positions correctly in Feldera. Do not use `SUBSTRING` with negative positions — it returns the full string. |
| `LOCATE(substr, str)` | `POSITION(substr IN str)` | |
| `LOCATE(substr, str, pos)` | `CASE WHEN POSITION(substr IN SUBSTRING(str, pos)) = 0 THEN 0 ELSE POSITION(substr IN SUBSTRING(str, pos)) + pos - 1 END` | See Notes below — CRITICAL for nested LOCATE. |
| `startswith(s, prefix)` | `LEFT(s, LENGTH(prefix)) = prefix` | String args only — if either arg is a binary (`x'...'`) literal, mark unsupported (`LEFT` does not work on binary types) |
| `endswith(s, suffix)` | `RIGHT(s, LENGTH(suffix)) = suffix` | String args only — binary args unsupported for the same reason |
| `contains(s, sub)` | `POSITION(sub IN s) > 0` | Returns NULL if either arg is NULL. Works with `x'...'` binary literals too — `contains(x'aa', x'bb')` → `POSITION(x'bb' IN x'aa') > 0` |
| `regexp_like(s, pat)` | `s RLIKE pat` | Feldera does not support `REGEXP_LIKE` as a function — use the infix `RLIKE` operator instead. → [GBD-REGEX-ESCAPE] |
| `s ILIKE pattern` | `s ILIKE pattern` | Case-insensitive LIKE — pass through, fully supported in Feldera. **Do NOT rewrite as `LIKE`** (LIKE is case-sensitive). |
| `s ILIKE ANY (p1, p2, ...)` | Same | |
| `split_part(str, delim, n)` | Same | |
| `s \|\| t` | Same | |
| `BIT_LENGTH(s)` | `OCTET_LENGTH(s) * 8` | Returns bit length; Feldera has OCTET_LENGTH (bytes) |
| `translate(s, from, to)` | Chain of `REPLACE` per character | See Notes below. |

#### 📝 Notes

**LOCATE(substr, str, pos) — nested LOCATE:** When `pos` is itself a `LOCATE(...)` expression, keep `substr` distinct from the inner expression. Never substitute the wrong character for `substr`.

```sql
-- Spark
LOCATE('.', email, LOCATE('@', email))

-- Feldera
CASE WHEN POSITION('.' IN SUBSTRING(email, POSITION('@' IN email))) = 0
     THEN 0
     ELSE POSITION('.' IN SUBSTRING(email, POSITION('@' IN email))) + POSITION('@' IN email) - 1
END
```

**translate(s, from, to):** Replace each character in `from` with the corresponding character in `to` using chained `REPLACE` calls. If `from` and `to` differ in length, mark unsupported (deletion semantics not supported).

```sql
-- translate(s, 'aei', '123')
REPLACE(REPLACE(REPLACE(s, 'a', '1'), 'e', '2'), 'i', '3')
```

### Math

| Spark | Feldera | Notes |
|-------|---------|-------|
| `greatest(a, b, ...)` | `GREATEST_IGNORE_NULLS(a, b, ...)` | Use `GREATEST_IGNORE_NULLS` to match Spark's semantics of skipping NULL values. |
| `least(a, b, ...)` | `LEAST_IGNORE_NULLS(a, b, ...)` | Use `LEAST_IGNORE_NULLS` to match Spark's semantics of skipping NULL values. |
| `pmod(a, b)` | `CASE WHEN MOD(a, b) < 0 AND b > 0 THEN MOD(a, b) + b ELSE MOD(a, b) END` | With positive divisor: result is always ≥ 0. With negative divisor: result has same sign as dividend. |
| `isnan(x)` | `IS_NAN(x)` | |
| `log2(x)` | `LOG(x, 2)` | |
| `log(base, x)` | `LOG(x, base)` | **CRITICAL — arg order reversed.** See Notes below. |
| `positive(x)` | `x` | Unary no-op — drop the function call, keep the argument as-is. |
| `negative(x)` | `-x` | Unary negation — replace with `-x`. |
| `try_divide(a, b)` | `div_null(a, b)` | Feldera v0.297+ has native `div_null` — returns NULL on divide-by-zero, matching Spark semantics. Pass both args as-is; no DOUBLE cast needed unless fractional result is required. → [GBD-DIV-ZERO] |

#### 📝 Notes

**log(base, x) — arg order is reversed:** Spark: first arg = base, second arg = value. Feldera: first arg = value, second arg = base. **The output SQL MUST have the arguments in the opposite order from the input.** A comment saying "args swapped" without actually swapping them in the SQL is wrong.

```sql
-- Spark input         →  Feldera output (args physically swapped)
LOG(val, 2)    →  LOG(2, val)
LOG(col, 10)   →  LOG(10, col)
LOG(b, x)      →  LOG(x, b)
```

Do NOT be misled by column alias names like `log2_col` — follow the rule, not the alias. If `LOG(A, B)` appears in Spark, the Feldera output must be `LOG(B, A)` — always.

### Null handling

| Spark | Feldera | Notes |
|-------|---------|-------|
| `NVL(a, b)` | `COALESCE(a, b)` | |
| `ZEROIFNULL(x)` | `COALESCE(x, 0)` | |
| `NULLIFZERO(x)` | `NULLIF(x, 0)` | |
| `isnull(x)` | `x IS NULL` | |
| `isnotnull(x)` | `x IS NOT NULL` | |
| `nvl2(x, a, b)` | `CASE WHEN x IS NOT NULL THEN a ELSE b END` | |

### Joins

| Spark | Feldera | Notes |
|-------|---------|-------|
| `LEFT ANTI JOIN ... ON cond` | `WHERE NOT EXISTS (SELECT 1 FROM ... WHERE cond)` | |
| `FULL OUTER JOIN` | `FULL OUTER JOIN` | Fully supported in Feldera — pass through unchanged. |

### DATE_ADD / DATE_SUB — wrong forms to avoid

#### 🔄 Translation rules

`date_add` and `date_sub` take exactly 2 args: `(date_expr, integer_days)` → `expr ± INTERVAL 'n' DAY`. For unit-first 3-arg form use `TIMESTAMPADD(DAY, 30, d)` — it is a different function.

#### ⚠️ Behavioral differences (Spark vs Feldera)

Do NOT emit these wrong forms:

```sql
DATE_ADD(d, 30, 'DAY')                  -- 3-arg hybrid: wrong
DATE_ADD(d, INTERVAL '30' DAY, 'DAY')   -- 3-arg hybrid: wrong
DATE_ADD(DAY, 30, d)                    -- TIMESTAMPADD signature: wrong
```

### named_struct field access

#### 🔄 Translation rules

`named_struct('a', v1, 'b', v2)` → `CAST(ROW(v1, v2) AS ROW(a T, b S))` when field names matter, or plain `ROW(v1, v2)` when they don't.

- Named ROW field access: `(row_val).field_name` — **always wrap the column in parentheses**. Bare `row_val.field_name` is a **syntax error** (not a compiler bug): standard SQL parses `x.y` as `table.column`, so the compiler reports "Table 'row_val' not found".
- Anonymous ROW field access (no CAST): `row_val[1]` (1-based index)
- **Struct field access inside lambdas**: `transform(arr, x -> x.field_name)` where elements are anonymous `ROW(...)` — use 1-based index instead: `transform(arr, x -> x[n])`. Example: `transform(arr, x -> x.name)` where arr is `ARRAY_AGG(ROW(ordinality, name))` → `transform(arr, x -> x[2])` (name is the 2nd field).
- **STRUCT column in DDL becomes ROW column** — all field accesses in SELECT and WHERE must use `(col).field` syntax:
  ```sql
  -- Spark: col.field
  SELECT details.department, details.salary FROM t WHERE details.salary > 0
  -- Feldera: (col).field
  SELECT (details).department, (details).salary FROM t WHERE (details).salary > 0
  ```

#### 📌 Example

```sql
-- Spark
SELECT source, COUNT(DISTINCT named_struct('l', left_id, 'r', right_id)) AS unique_pair_count
FROM events GROUP BY source

-- Feldera (anonymous ROW)
SELECT source, COUNT(DISTINCT ROW(left_id, right_id)) AS unique_pair_count
FROM events GROUP BY source

-- Feldera (named fields via CAST)
SELECT source, COUNT(DISTINCT CAST(ROW(left_id, right_id) AS ROW(l BIGINT, r BIGINT))) AS unique_pair_count
FROM events GROUP BY source
```

### SQL Syntax differences

#### Hex binary literals

#### 📝 Notes

##### Supported

- `POSITION(x'...' IN x'...')` — binary search, pass through unchanged
- `OCTET_LENGTH(x'...')` — correct byte count (use instead of `LENGTH`, which counts hex chars not bytes)
- `SUBSTRING(x'...' FROM n FOR m)` — binary slicing
- `REPLACE(x'...', x'...', x'...')` — binary replace
- Comparisons with `x'...'` literals

```sql
-- Spark
contains(x'aa', x'bb')
-- Feldera
POSITION(x'bb' IN x'aa') > 0

-- Spark (POSITION with binary literals passes through unchanged)
SELECT POSITION(x'537061726b' IN x'537061726b2053514c') > 0
-- Feldera
SELECT POSITION(x'537061726b' IN x'537061726b2053514c') > 0
```

##### Not supported or wrong semantics

- `CONCAT(x'...', x'...')` — fails: BINARY not supported in CONCAT
- `TRIM(x'...')` — fails: TRIM does not accept BINARY
- `RPAD(varchar_col, n, x'...')` — fails: binary literal where VARCHAR expected
- `LENGTH(x'...')` — compiles but returns hex char count, not byte count; use `OCTET_LENGTH` instead
- `UPPER(x'...')` / `LOWER(x'...')` — compile but are no-ops on binary; do not use
- `CAST(x'...' AS VARCHAR)` — returns the hex string (e.g. `48656c6c6f`), not the decoded string (`Hello`)

#### VALUES as table source

| Spark | Feldera |
|-------|---------|
| `SELECT a FROM VALUES (1), (2) AS t(a)` | `SELECT a FROM (VALUES (1), (2)) AS t(a)` |
| `SELECT a FROM VALUES (1, 2) AS T(a, b)` | `SELECT a FROM (VALUES (1, 2)) AS T(a, b)` |

#### 🔄 Translation rules

Always wrap `VALUES (...)` in parentheses and add an alias: `FROM (VALUES ...) AS alias(cols)`.

#### 📝 Notes

**Preserve implicit column names:** When Spark uses `FROM VALUES (...)` without explicit column aliases, it assigns implicit names like `col1`, `col2`. If the query references those names, the alias in `AS t(...)` must use the same names.

```sql
-- Spark: VALUES column is implicitly named 'col1'
SELECT 1 AS col1, col1 FROM VALUES (10) ORDER BY col1;

-- Feldera WRONG — renamed to x, but Spark query references 'col1'
SELECT 1 AS col1, x FROM (VALUES (10)) AS t(x) ORDER BY col1;

-- Feldera CORRECT — preserve 'col1' so all references still work
SELECT 1 AS col1, col1 FROM (VALUES (10)) AS t(col1) ORDER BY col1;
```

#### SELECT with GROUP BY / HAVING but no FROM

#### 🔄 Translation rules

When `GROUP BY` or `HAVING` is present but there is no `FROM` clause, add `FROM (VALUES (1)) AS t(x)`.

#### 📌 Example

```sql
-- Spark (valid)
SELECT 1 GROUP BY COALESCE(1, 1) HAVING COALESCE(1, 1) = 1;

-- Feldera (add dummy FROM)
SELECT 1 AS col1 FROM (VALUES (1)) AS t(x) GROUP BY COALESCE(1, 1) HAVING COALESCE(1, 1) = 1;
```

#### Operators

| Spark | Feldera | Notes |
|-------|---------|-------|
| `a == b` | `a = b` | Spark allows `==` for equality; use `=` in Feldera |
| `GROUP BY ALL` | Expand to explicit column list | Replace with all non-aggregate SELECT expressions. E.g. `SELECT a, b+1, SUM(c) ... GROUP BY ALL` → `GROUP BY a, b+1` |
| `/*+ BROADCAST(table) */` | `/*+ broadcast(alias) */` | Both are performance-only join hints — no effect on results. Spark uses the table name; Feldera requires the table **alias** in the hint (e.g. `FROM customers c` → `/*+ broadcast(c) */`). Feldera hints are experimental. |

#### Scalar subquery shorthand in HAVING / WHERE

#### 🔄 Translation rules

- If `(SELECT expr)` has no `FROM`, rewrite as just `expr`.
- If the subquery has a `FROM` clause, leave it as-is — it is a regular or correlated subquery, not this shorthand.
- `(SELECT 1 WHERE cond)` returns 1 if `cond` is true, NULL otherwise — so `a = (SELECT 1 WHERE cond)` simplifies to `a = 1 AND cond`.

#### 📌 Example

```sql
-- Spark
HAVING (SELECT col1.a = 1)   →  HAVING col1.a = 1
WHERE  (SELECT x > 0)        →  WHERE  x > 0

-- Spark
HAVING MAX(col2) == (SELECT 1 WHERE MAX(col2) = 1)
-- Feldera
HAVING MAX(col2) = 1
```

#### SELECT alias chaining

Feldera supports lateral aliases in SELECT — no rewrite needed. `SELECT col1 AS a, a AS b FROM t` passes through unchanged.

#### 📌 Example

```sql
-- Spark: valid
SELECT col1 AS a, a AS b FROM t
-- Feldera: also valid — pass through unchanged
SELECT col1 AS a, a AS b FROM t
```

#### SELECT alias shadowing in HAVING / GROUP BY

No rewrite needed in general — Feldera resolves names in HAVING/GROUP BY to the source column, same as Spark.

#### ⚠️ Behavioral differences (Spark vs Feldera)

When the SELECT alias reuses a GROUP BY source column name with GROUPING SETS/CUBE/ROLLUP, Feldera resolves HAVING to the aggregate alias; Spark resolves it to the source column. Workaround: reference the source column explicitly in HAVING.

#### 📌 Example

```sql
-- Data: VALUES (1, 10), (2, 20) AS T(a, b)
-- 'b' is both a source column (values 10, 20) and a SELECT alias for SUM(a) (values 1, 2)

-- Spark: HAVING b → column b (10 or 20) → 2 rows returned (b=20 > 10, appears twice in GROUPING SETS)
-- Feldera: HAVING b → alias SUM(a) (1 or 2) → 0 rows returned
SELECT SUM(a) AS b FROM T GROUP BY GROUPING SETS ((b), (a, b)) HAVING b > 10;

-- Workaround: rename the SELECT alias to avoid shadowing the source column
SELECT SUM(a) AS sum_a FROM T GROUP BY GROUPING SETS ((b), (a, b)) HAVING b > 10;
```

#### Struct field access

#### 🔄 Translation rules

- In ORDER BY or HAVING, always prefix struct column access with the table alias to avoid misparsing.
- Non-aggregate HAVING with struct field → move to WHERE.

#### 📌 Example

```sql
ORDER BY t.col.field   -- correct
ORDER BY col.field     -- may fail (Feldera misparses as table.column)

-- Spark
SELECT col1 FROM t GROUP BY col1 HAVING col1.a = 1
-- Feldera
SELECT col1 FROM t WHERE t.col1.a = 1 GROUP BY col1
```

#### SELECT * EXCEPT / table.* EXCEPT

#### 🔄 Translation rules

Feldera supports both `EXCEPT` and `EXCLUDE` for column exclusion — pass through unchanged, no rewrite needed.

#### 📌 Example

```sql
-- Spark
SELECT SEC.* EXCEPT(Status, conameorcik), ...

-- Feldera: same — pass through unchanged
SELECT SEC.* EXCEPT(Status, conameorcik), ...
```

#### Null-safe equality

| Spark | Feldera | Notes |
|-------|---------|-------|
| `equal_null(a, b)` | `a <=> b` | Returns true when both sides equal OR both NULL |

## Compiler errors: syntax errors vs. unsupported features

When the Feldera compiler rejects a query, distinguish the cause before deciding how to respond:

### Syntax errors (fix the translation)

These are **not compiler bugs** — the SQL you emitted is invalid. Fix the translation and retry.

| Error message pattern | Likely cause | Fix |
|-----------------------|--------------|-----|
| `Table 'X' not found` | Used `col.field` on a ROW column — parser reads `col` as a table name | Change to `(col).field` |
| `Column 'X' not found` | Wrong table alias, missing alias, or column not in scope | Check the FROM clause and aliases |
| `No match found for function signature F(<TYPE>)` | Called a function with wrong argument types | Cast arguments or use the correct overload |
| `Illegal use of 'NULL'` | NULL in a context that requires a typed expression | Add `CAST(NULL AS <type>)` |
| `Aggregate function not allowed` | Aggregate in WHERE or non-aggregate context | Move to HAVING or wrap in a subquery |

**Key rule**: if the error points to a specific line and column with a clear parse or type message, it is a syntax/type error — fix the SQL. Do **not** mark the construct as unsupported.

### Unsupported features (mark as unsupported)

These are cases where the syntax is valid SQL but Feldera has no implementation:

- `No match found for function signature F(...)` for a function that does not exist in Feldera at all (e.g. `REVERSE`, `uuid`, `substring_index`)
- `Feature not yet implemented` messages

For these, mark as unsupported per the rules below. Do **not** retry with different syntax — the feature is absent, not broken.

## Unsupported (no Feldera equivalent)

### Rules for unsupported constructs

When an unsupported construct is found:

1. List each unsupported construct in the `unsupported` array with a brief explanation.
2. **Still produce a `CREATE VIEW`**: replace each unsupported expression with `CAST(NULL AS <type>)`, translate all other parts normally. An empty `feldera_query` is never acceptable.
3. Do NOT enter the repair loop for known-unsupported functions.

Do NOT:
- Approximate with a different function that changes semantics.
- Keep retrying after a compiler "No match found" error for a known-unsupported function.
- Silently change semantics to make the SQL compile.
- Flag a table as unsupported just because it is referenced inside a dependency view (e.g. a staging view provided in the schema) but not directly used by the main query being translated. Missing tables in dependency views are a deployment concern, not a translation error — do not list them in `unsupported`.

### Unsupported function list

#### String

| Function | Notes |
|----------|-------|
| `REVERSE(str)` | Not implemented in Feldera for string types — not documented in Feldera SQL docs. Mark unsupported. |
| `DECODE(expr, s1, r1, s2, r2, ..., default)` | **When all search values are non-NULL literals** (e.g. `'ACTV'`, `'CMPT'`): rewrite as `CASE WHEN expr = s1 THEN r1 WHEN expr = s2 THEN r2 ... ELSE default END`. This is safe because the only difference between DECODE and CASE WHEN is NULL-safe matching, which only matters when a search value could be NULL. **When any search value could be NULL**: not supported — DECODE uses Oracle-style `NULL = NULL` is TRUE which requires `IS NOT DISTINCT FROM`, not supported in Feldera. |
| `substring_index` | No equivalent |
| `uuid()` | Not supported in Feldera — mark unsupported. |
| `monotonically_increasing_id()` | Not supported in Feldera — generates a unique, monotonically increasing 64-bit integer per row using internal partition information. No Feldera equivalent. Mark as unsupported and suggest the user replace with a surrogate key strategy (e.g., sequence, UUID, or computed key from existing columns). |
| `min_by(value, key) OVER (...)` | Window function form not supported — `ARG_MIN` is only a GROUP BY aggregate in Feldera, not a window function. Mark unsupported when `OVER` is present. |
| `max_by(value, key) OVER (...)` | Window function form not supported — `ARG_MAX` is only a GROUP BY aggregate in Feldera, not a window function. Mark unsupported when `OVER` is present. |
| `contains(bool, ...)` | `contains()` does not work on boolean args in Feldera — mark unsupported |
| `startswith(x'...', x'...')` / `endswith(x'...', x'...')` | Binary hex literal args not supported — `LEFT`/`RIGHT` do not work on binary types |
| `to_number(str, fmt)` | Numeric parsing with format string — not supported in Feldera |
| `to_binary(str, fmt)` | Binary conversion function — not supported in Feldera |
| `luhn_check(str)` | Luhn algorithm check — not supported in Feldera |
| `is_valid_utf8(str)` | UTF-8 validation — not supported in Feldera |
| `validate_utf8(str)` / `try_validate_utf8(str)` / `make_valid_utf8(str)` | UTF-8 validation/repair — not supported in Feldera |
| `quote(str)` | SQL quoting function — not supported in Feldera |
| `split(str, delim, limit)` | 3-argument form with limit — not supported. Use 2-argument `SPLIT(str, delim)` instead (drops the limit). |
| `split(str, regex_pattern)` | Feldera's `SPLIT` does not support regex patterns — only literal string delimiters work. If the pattern is a regex (e.g. `[1-9]+`, `\\s+`), there is no Feldera equivalent — mark unsupported. |
| `hex(x)` | `UPPER(TO_HEX(x))` when input is `BINARY`/`VARBINARY` — Feldera `TO_HEX` returns lowercase; Spark `hex()` returns uppercase; ALWAYS wrap with `UPPER()`. Use `UPPER(TO_HEX(CAST(x AS VARBINARY)))` if the column type is `BINARY`. For integer or string inputs, `TO_HEX` is not supported in Feldera — mark as unsupported. |
| `unhex(s)` | Binary hex decoding — not supported in Feldera |
| `encode(str, charset)` / `decode(bytes, charset)` | Binary encode/decode — not supported in Feldera. Mark unsupported even when nested inside other functions: `btrim(encode(...))`, `trim(encode(...))`, `hex(encode(...))` are all unsupported — do NOT attempt to rewrite as `CAST(str AS VARBINARY)`, which produces hex output instead of the original string. |
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
| `sequence(start, stop, INTERVAL 1 DAY)` — date inputs | Not supported — Feldera `SEQUENCE` is integers only. Mark as unsupported. |
| Spark type suffix literals: `1Y`, `122S`, `10L`, `100BD` | Spark shorthand type suffixes (Y=tinyint, S=smallint, L=bigint, BD=decimal) not valid syntax in Feldera |

#### CAST

| Spark | Notes |
|-------|-------|
| `CAST(interval <n> unit1 <m> unit2 AS string)` | Feldera output format (`+3-01`) differs from Spark (`INTERVAL '3-1' YEAR TO MONTH`) — mark unsupported |
| `CAST(x AS INTERVAL)` | Bare `INTERVAL` without a unit — parse error, always unsupported |
| `CAST(column AS INTERVAL ...)` | Column/expression-to-interval — not supported; only constant string literals can be rewritten (see CAST section above) |
| `SELECT CAST(... AS INTERVAL ...)` as final output | `INTERVAL` cannot be a column type in a regular `CREATE VIEW` (exposed to connectors). If the interval is intermediate, use `CREATE LOCAL VIEW` — local views support `INTERVAL` columns. Mark unsupported only when the interval must be the final output to a connector or client. |
| `CAST(INTERVAL 'x-y' YEAR TO MONTH AS numeric)` | Compound interval (YEAR TO MONTH, DAY TO SECOND, HOUR TO SECOND) to numeric — not supported |
| `CAST(numeric AS TIMESTAMP)` where numeric is **seconds** since epoch | Spark interprets as **seconds**; Feldera interprets as **milliseconds** — off by factor of 1,000. Mark unsupported when the numeric value represents seconds (the typical Spark use case). |
| `CAST(numeric AS INTERVAL ...)` | Numeric-to-interval — not supported |
| `CAST(TIME '...' AS numeric/decimal)` | TIME to numeric or decimal — not supported |

#### Aggregate

| Function | Notes |
|----------|-------|
| `approx_count_distinct`, `APPROX_DISTINCT`, `percentile_approx`, `approx_percentile` | Approximate aggregates — not supported |
| `CORR` | Statistical aggregate — not supported |
| `collect_set(col)` | Feldera has no SET type — mark unsupported. |

#### Window

| Function | Notes |
|----------|-------|
| `PERCENT_RANK`, `CUME_DIST`, `NTILE`, `NTH_VALUE` | Not implemented |
| `ROWS BETWEEN ... AND ...` | Not supported |

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
| `map_entries(m)` | Returns an array of `{key, value}` structs — no equivalent in Feldera |
| `map_concat(m1, m2)` | No equivalent |

#### JSON

| Function | Notes |
|----------|-------|
| `json_tuple(s, k1, k2, ...) AS (c1, c2, ...)` | Multi-column JSON extraction — conversion too complex to rewrite safely |

#### Bitwise (scalar)

All scalar bitwise operators are unsupported in Feldera — no scalar equivalent exists for any of them.

| Function | Notes |
|----------|-------|
| `a \| b` | Bitwise OR — `\|` is a parse error in Feldera |
| `a & b` | Bitwise AND — parses but "Not yet implemented" |
| `a ^ b` | Bitwise XOR — parses but "Not yet implemented" |
| `shiftleft(a, n)` / `shiftright(a, n)` / `shiftrightunsigned(a, n)` | Not supported — mark unsupported. Java bit-shift semantics (overflow wrapping, sign extension, zero-fill) cannot be replicated correctly in SQL without bitwise operators. |

#### Math

| Function | Notes |
|----------|-------|
| `try_add(a, b)` | Spark returns NULL on overflow; Feldera may panic — not safely rewritable → [GBD-DIV-ZERO] |
| `try_subtract(a, b)` | Spark returns NULL on overflow; Feldera may panic — not safely rewritable → [GBD-DIV-ZERO] |
| `try_multiply(a, b)` | Spark returns NULL on overflow; Feldera may panic — not safely rewritable → [GBD-DIV-ZERO] |
| `width_bucket(v, min, max, n)` | No equivalent |
| `RAND()` | No equivalent; random number function not supported |

#### Hashing / Encoding

| Function | Notes |
|----------|-------|
| `SHA`, `SHA2`, `SHA256` | Not supported; MD5 is the only supported hash function |
| `base64`, `unbase64` | Not built-in; can be implemented as a Rust UDF |

#### JSON / CSV

### `from_json` rewrite rules

Spark's `from_json(col, schema)` parses a JSON string. In Feldera, use `CAST(PARSE_JSON(col) AS type)` where `type` is the Feldera equivalent of the Spark schema string (e.g., `MAP<VARCHAR,VARCHAR>`, `VARCHAR ARRAY`, `ROW(field1 TYPE1, field2 TYPE2)`). Specific patterns below.

| Spark pattern | Feldera |
|---------------|---------|
| `LATERAL VIEW explode(from_json(col, 'array<map<string,string>>')) AS m` | `CROSS JOIN UNNEST(CAST(PARSE_JSON(col) AS MAP<VARCHAR,VARCHAR> ARRAY)) AS t(m)` — then access map values as `m['key']` (already VARCHAR, no extra CAST needed). |
| `explode(from_json(col, 'MAP<STRING,STRING>'))` / `LATERAL VIEW explode(from_json(col, 'MAP<K,V>'))` | `CROSS JOIN UNNEST(CAST(PARSE_JSON(col) AS MAP<VARCHAR,VARCHAR>)) AS t(key, value)` — JSON object → MAP (not ARRAY of maps); key and value are both VARCHAR. |

| `from_json(col, schema) IS NOT NULL` | `col IS NOT NULL` — `from_json` never returns NULL for non-null input (bad JSON → struct with NULL fields), so the check reduces to the column itself |
| `from_json(col, 'field TYPE')['field']` | `CAST(PARSE_JSON(col)['field'] AS type)` |
| `COALESCE(from_json(col, schema)['field'], default)` | `COALESCE(CAST(PARSE_JSON(col)['field'] AS type), default)` — **never drop the COALESCE or its default value** |

### Spark colon-syntax (semi-structured field access)

Databricks/Spark SQL allows `col:field` as shorthand for extracting a JSON sub-field from a VARCHAR/STRING column containing JSON. Translate it as PARSE_JSON bracket access:

| Spark | Feldera |
|-------|---------|
| `col:field` | `PARSE_JSON(col)['field']` — returns VARIANT; cast to the required type as needed |
| `col:field:subfield` | `PARSE_JSON(col)['field']['subfield']` — chain bracket accesses |
| `col:field[0]` | `PARSE_JSON(col)['field'][0]` — array index access |

**[GBD-INLINE-STRUCT-ARRAY]** When translating `inline(from_json(json_col, 'Array<struct<f1 T1, f2 T2>>'))` from a LATERAL VIEW or SELECT clause:
- `json_col` may itself be a colon-syntax expression — translate it first (e.g. `col:steps` → `PARSE_JSON(col)['steps']`)
- Move any SELECT-clause `inline(...)` into a FROM-clause comma join
- Full translation pattern:
```sql
-- Spark (LATERAL VIEW form)
SELECT t.id, steps.step_name, steps.step_uuid
FROM t
LATERAL VIEW inline(from_json(t.payload:steps, 'Array<struct<name STRING, uuid STRING>>')) steps AS step_name, step_uuid

-- Feldera
SELECT t.id, steps.step_name, steps.step_uuid
FROM t
, UNNEST(CAST(PARSE_JSON(t.payload)['steps'] AS ROW(name VARCHAR, uuid VARCHAR) ARRAY)) AS steps(step_name, step_uuid)
```
- Map struct field types using the [Type Mappings](#type-mappings) section.
- Add a warning. Do NOT mark unsupported.

| Function | Notes |
|----------|-------|
| `json_object_keys` | No equivalent |
| `schema_of_json`, `schema_of_csv` | Schema inference — not supported |
| `from_csv`, `to_csv` | CSV serialization — not supported |

#### DDL / Structural

| Function | Notes |
|----------|-------|
| `stack()` | Unpivot via function — not supported |
| `INSERT OVERWRITE` | Not supported |

