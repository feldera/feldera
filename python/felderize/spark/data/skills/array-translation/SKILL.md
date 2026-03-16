---
name: array-types
description: Rewrites Spark ARRAY<T> type declarations to Feldera suffix syntax. Use when schemas contain ARRAY<...> types or when the compiler reports parser errors on angle-bracket array syntax.
---

# Array Types

## Purpose

Rewrite Spark/Trino-style `ARRAY<T>` type declarations to Feldera's suffix syntax. This is a DDL-level concern — fix it before addressing any query-level issues.

## Rules

- Feldera uses suffix syntax: `T ARRAY`, not `ARRAY<T>`.
- Array literals use `ARRAY[...]` (square brackets).
- Array indexes are 1-based.
- Never emit `ARRAY<...>` in Feldera DDL.

## Rewrite Patterns

```text
ARRAY<T>           →  T ARRAY
ARRAY<ARRAY<T>>    →  T ARRAY ARRAY
ARRAY<ROW(...)>    →  ROW(...) ARRAY
MAP<K, ARRAY<V>>   →  MAP<K, V ARRAY>
```

Concrete examples:

```text
ARRAY<STRING>                      →  VARCHAR ARRAY
ARRAY<INT>                         →  INT ARRAY
ARRAY<ROW(id BIGINT, tag STRING)>  →  ROW(id BIGINT, tag VARCHAR) ARRAY
MAP<VARCHAR, ARRAY<INT>>           →  MAP<VARCHAR, INT ARRAY>
```

## Error-Driven Repair

If the compiler reports:

```text
Encountered "<" ... ARRAY<VARCHAR>
```

1. Rewrite ALL `ARRAY<...>` declarations to suffix form.
2. Preserve column names and query shape.
3. Re-validate before attempting unrelated changes.

This is a type-syntax issue, not a semantic issue. Fix it first.

## Examples

Input:
```sql
CREATE TABLE t(items ARRAY<STRING>)
```
Output:
```sql
CREATE TABLE t(items VARCHAR ARRAY)
```

Input:
```sql
CREATE TABLE s(
  items ARRAY<ROW(sku VARCHAR, qty INT)>,
  attrs MAP<VARCHAR, ARRAY<INT>>
)
```
Output:
```sql
CREATE TABLE s(
  items ROW(sku VARCHAR, qty INT) ARRAY,
  attrs MAP<VARCHAR, INT ARRAY>
)
```
