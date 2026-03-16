---
name: query-rewrite
description: Rewrites Spark SQL query patterns that have no direct Feldera equivalent into semantically equivalent Feldera SQL. Covers PIVOT, GROUPING SETS, ROLLUP, CUBE, and get_json_object.
---

# Query Rewrite

## Purpose

Use this skill when Spark SQL contains query constructs that Feldera does not support directly but can be rewritten to equivalent SQL.

## PIVOT

Feldera does not support `PIVOT` syntax. Rewrite to conditional aggregation with `CASE WHEN`.

### Pattern

Spark:
```sql
SELECT * FROM source
PIVOT (
  agg_func(value_col)
  FOR pivot_col IN ('val1', 'val2', 'val3')
)
```

Feldera:
```sql
SELECT
  <non-pivot columns>,
  agg_func(CASE WHEN pivot_col = 'val1' THEN value_col END) AS val1,
  agg_func(CASE WHEN pivot_col = 'val2' THEN value_col END) AS val2,
  agg_func(CASE WHEN pivot_col = 'val3' THEN value_col END) AS val3
FROM source
GROUP BY <non-pivot columns>
```

### Rules
- Preserve the aggregation function (COUNT, SUM, AVG, etc.).
- Each PIVOT value becomes a separate `CASE WHEN` expression.
- The non-pivot, non-value columns become the GROUP BY columns.
- Column aliases come from the IN list values.
- Double-check GROUP BY spelling — do not introduce typos.

### Example

Input:
```sql
SELECT * FROM (
  SELECT team_name, status, ticket_id
  FROM support_tickets
) src
PIVOT (
  COUNT(ticket_id)
  FOR status IN ('OPEN', 'IN_PROGRESS', 'CLOSED')
)
```

Output:
```sql
CREATE VIEW result AS
SELECT
  team_name,
  COUNT(CASE WHEN status = 'OPEN' THEN ticket_id END) AS OPEN,
  COUNT(CASE WHEN status = 'IN_PROGRESS' THEN ticket_id END) AS IN_PROGRESS,
  COUNT(CASE WHEN status = 'CLOSED' THEN ticket_id END) AS CLOSED
FROM support_tickets
GROUP BY team_name
```

## GROUPING SETS

Feldera does not support `GROUPING SETS`. Rewrite to `UNION ALL` of separate `GROUP BY` queries.

### Pattern

Spark:
```sql
SELECT a, b, SUM(x) AS total
FROM t
GROUP BY GROUPING SETS ((a, b), (a), ())
```

Feldera:
```sql
SELECT a, b, SUM(x) AS total FROM t GROUP BY a, b
UNION ALL
SELECT a, CAST(NULL AS <type_of_b>) AS b, SUM(x) AS total FROM t GROUP BY a
UNION ALL
SELECT CAST(NULL AS <type_of_a>) AS a, CAST(NULL AS <type_of_b>) AS b, SUM(x) AS total FROM t
```

### Rules
- Each grouping set becomes a separate SELECT with its own GROUP BY.
- Columns not in a grouping set must be NULL with proper CAST to match types.
- All branches must have identical column names, types, and order.
- Use CAST(NULL AS type) rather than bare NULL to avoid type mismatches in UNION ALL.

## ROLLUP

Feldera does not support `ROLLUP`. Expand to equivalent UNION ALL.

`GROUP BY ROLLUP(a, b, c)` is equivalent to:
```
GROUPING SETS ((a, b, c), (a, b), (a), ())
```

Then apply the GROUPING SETS rewrite above.

## CUBE

`GROUP BY CUBE(a, b)` is equivalent to:
```
GROUPING SETS ((a, b), (a), (b), ())
```

Then apply the GROUPING SETS → UNION ALL rewrite above.

Note: `grouping_id()` function is NOT available in Feldera. If the query uses `grouping_id()`, mark it as unsupported.

## named_struct

Spark's `named_struct('field1', val1, 'field2', val2, ...)` creates a struct with named fields.

### Feldera equivalent

Use `ROW(val1, val2, ...)` constructor.

```sql
-- Spark
named_struct('l', left_id, 'r', right_id)

-- Feldera
ROW(left_id, right_id)
```

### Rules
- Drop the field name strings — Feldera ROW constructors are positional.
- Preserve the value expressions in order.
- `ROW(x, y, z)` creates an anonymous struct.
- Field access on ROW values uses dot notation: `row_val.field_name`.

### Example

Input:
```sql
SELECT source, COUNT(DISTINCT named_struct('l', left_id, 'r', right_id)) AS unique_pair_count
FROM pair_events GROUP BY source
```

Output:
```sql
SELECT source, COUNT(DISTINCT ROW(left_id, right_id)) AS unique_pair_count
FROM pair_events GROUP BY source
```

## nvl2

Spark's `nvl2(expr, val_if_not_null, val_if_null)` returns `val_if_not_null` when `expr` is not NULL, otherwise `val_if_null`.

Rewrite to CASE WHEN:
```sql
-- Spark
nvl2(col, 'has value', 'missing')
-- Feldera
CASE WHEN col IS NOT NULL THEN 'has value' ELSE 'missing' END
```

This is a safe, semantics-preserving rewrite. Do NOT mark as unsupported.

## pmod (positive modulo)

Spark's `pmod(a, b)` returns the positive modulo (always non-negative result).

Rewrite to:
```sql
-- Spark
pmod(a, b)
-- Feldera
MOD(MOD(a, b) + b, b)
```

This ensures the result is always non-negative. Do NOT mark as unsupported.

## from_unixtime / to_timestamp from epoch

Spark's `from_unixtime(unix_seconds)` and `to_timestamp(unix_seconds)` convert unix epoch seconds to timestamp.

Mark as unsupported — Feldera does not support `to_timestamp(<NUMERIC>)` or `from_unixtime`.

## LPAD / RPAD

Feldera does not have `LPAD` or `RPAD` functions. Rewrite using `CONCAT`, `REPEAT`, and `LENGTH`.

### LPAD(string, length, pad)

```sql
-- Spark
LPAD(code, 8, '0')
-- Feldera
CASE WHEN LENGTH(code) >= 8 THEN SUBSTRING(code, 1, 8)
     ELSE CONCAT(REPEAT('0', 8 - LENGTH(code)), code) END
```

### RPAD(string, length, pad)

```sql
-- Spark
RPAD(name, 20, ' ')
-- Feldera
CASE WHEN LENGTH(name) >= 20 THEN SUBSTRING(name, 1, 20)
     ELSE CONCAT(name, REPEAT(' ', 20 - LENGTH(name))) END
```

### Rules
- When the string is already >= target length, truncate to target length (matches Spark behavior).
- The pad character defaults to `' '` (space) if not specified.
- Do NOT mark as unsupported — this is a safe, semantics-preserving rewrite.

## LEFT ANTI JOIN

Feldera does not support `LEFT ANTI JOIN`. Rewrite to `NOT EXISTS`.

```sql
-- Spark
SELECT a.id, a.name
FROM customers a
LEFT ANTI JOIN blacklist b ON a.id = b.id

-- Feldera
SELECT a.id, a.name
FROM customers a
WHERE NOT EXISTS (SELECT 1 FROM blacklist b WHERE b.id = a.id)
```

Do NOT mark as unsupported — this is a safe, semantics-preserving rewrite.

## weekofyear

Feldera does not have `weekofyear()`. Rewrite to `EXTRACT(WEEK FROM ...)`.

```sql
-- Spark
weekofyear(event_date)
-- Feldera
EXTRACT(WEEK FROM event_date)
```

Do NOT mark as unsupported — this is a safe, semantics-preserving rewrite.

## JOIN clause rewrites

```text
JOIN ... USING (col)  →  JOIN ... ON left.col = right.col
NATURAL JOIN          →  JOIN ... ON (explicit matching columns)
```

- Feldera supports `USING` in some cases but explicit `ON` is safer and avoids ambiguity.
- `NATURAL JOIN` should always be rewritten to explicit `ON` conditions.

## Notes

- Always verify the rewritten SQL produces the same column names and types as the original.
- Prefer simple, readable rewrites over clever tricks.
- Double-check all SQL keywords for typos before returning.
