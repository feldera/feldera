---
name: query-rewrite
description: Rewrites Spark SQL query patterns that have no direct Feldera equivalent into semantically equivalent Feldera SQL. Covers named_struct, nvl2, pmod, LPAD/RPAD, LEFT ANTI JOIN, and weekofyear.
---

# Query Rewrite

## Purpose

Use this skill when Spark SQL contains query constructs that Feldera does not support directly but can be rewritten to equivalent SQL.

Note: `grouping_id()` function is NOT available in Feldera. If the query uses `grouping_id()`, mark it as unsupported.

## named_struct

Spark's `named_struct('field1', val1, 'field2', val2, ...)` creates a struct with named fields.

### Feldera equivalent

Use `ROW(val1, val2, ...)` constructor, or `CAST(ROW(val1, val2) AS ROW(field1 T1, field2 T2))` to preserve field names, or a user-defined type.

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
- Field access on named ROW values uses dot notation: `row_val.field_name`. For anonymous structs (no field names), use 1-based index access: `row_val[1]`.

### Example

Input:
```sql
SELECT source, COUNT(DISTINCT named_struct('l', left_id, 'r', right_id)) AS unique_pair_count
FROM pair_events GROUP BY source
```

Output (anonymous ROW):
```sql
SELECT source, COUNT(DISTINCT ROW(left_id, right_id)) AS unique_pair_count
FROM pair_events GROUP BY source
```

Output (named fields via CAST):
```sql
SELECT source, COUNT(DISTINCT CAST(ROW(left_id, right_id) AS ROW(l BIGINT, r BIGINT))) AS unique_pair_count
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

See function-reference skill for rewrites using `TIMESTAMPADD`.

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
