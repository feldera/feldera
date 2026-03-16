---
name: unnest
description: Translates Spark EXPLODE, LATERAL VIEW, posexplode, and inline patterns to Feldera UNNEST. Use when queries expand arrays or maps into rows.
---

# UNNEST (Table Expansion)

## Purpose

Translate Spark's array/map expansion patterns to Feldera's `UNNEST` operator. This covers `EXPLODE`, `LATERAL VIEW`, `posexplode`, and `inline`.

## Core Syntax

Feldera `UNNEST` expands an array or map into rows:

```sql
-- Array: one output column per element
FROM table, UNNEST(array_col) AS alias(col_name)

-- Map: two output columns (key, value)
FROM table CROSS JOIN UNNEST(map_col) AS alias(key_col, val_col)

-- Array with position index
FROM table, UNNEST(array_col) WITH ORDINALITY AS alias(val_col, pos_col)
```

Key rules:
- `UNNEST` on NULL returns an empty table (no rows).
- For arrays of ROW values, UNNEST produces columns matching the struct fields.
- Always provide explicit column aliases.

## Spark → Feldera Patterns

### LATERAL VIEW explode (array)

```sql
-- Spark
SELECT col, tag FROM data LATERAL VIEW explode(tags) t AS tag

-- Feldera
SELECT col, tag FROM data, UNNEST(tags) AS t(tag)
```

### LATERAL VIEW OUTER explode

```sql
-- Spark (preserves rows where array is NULL/empty)
SELECT col, tag FROM data LATERAL VIEW OUTER explode(tags) t AS tag

-- Feldera
SELECT col, tag FROM data, UNNEST(tags) AS t(tag)
```

Warning: OUTER semantics are not exactly replicated — rows with NULL/empty arrays will be dropped. Add a warning when translating OUTER.

### LATERAL VIEW explode (map)

```sql
-- Spark
SELECT col, k, v FROM data LATERAL VIEW explode(attrs) t AS k, v

-- Feldera
SELECT col, k, v FROM data CROSS JOIN UNNEST(attrs) AS t(k, v)
```

### posexplode (array with position)

```sql
-- Spark
SELECT col, pos, tag FROM data LATERAL VIEW posexplode(tags) t AS pos, tag

-- Feldera
SELECT col, pos, tag FROM data, UNNEST(tags) WITH ORDINALITY AS t(tag, pos)
```

Note: In Feldera `WITH ORDINALITY`, the ordinal column comes AFTER the value column — opposite order from Spark's `posexplode` which puts position first.

### inline (array of structs)

```sql
-- Spark
SELECT col, f1, f2 FROM data LATERAL VIEW inline(arr_of_structs) t AS f1, f2

-- Feldera
SELECT col, f1, f2 FROM data, UNNEST(arr_of_structs) AS t(f1, f2)
```

For arrays of ROW values, UNNEST automatically expands struct fields as output columns.

## When NOT to use UNNEST

- `EXPLODE` with `sequence()` for date range generation — `sequence()` is unsupported, so the whole pattern fails.
- `LATERAL VIEW OUTER` when preserving NULL rows is essential — add an unsupported/warning diagnostic.

## Examples

Multi-level unnest:
```sql
-- Spark
SELECT o.id, item.product_id, item.qty
FROM orders o
LATERAL VIEW inline(o.line_items) item AS product_id, qty

-- Feldera
SELECT o.id, item.product_id, item.qty
FROM orders o, UNNEST(o.line_items) AS item(product_id, qty)
```
