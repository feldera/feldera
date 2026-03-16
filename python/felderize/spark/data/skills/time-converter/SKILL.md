---
name: time-converter
description: Translates Spark date/time functions to Feldera SQL. Covers DATE_ADD, DATE_SUB, DATEDIFF, DATE_TRUNC, TIMESTAMP_TRUNC, DAY extraction, and common compiler errors.
---

# Time Converter

## Translations

### DATE_ADD / DATE_SUB

```sql
-- Spark
date_add(d, 30)          →  d + INTERVAL '30' DAY
date_sub(d, 7)           →  d - INTERVAL '7' DAY
```

WRONG forms (do NOT emit):
```sql
DATE_ADD(d, 30, 'DAY')                    -- 3-arg hybrid
DATE_ADD(d, INTERVAL '30' DAY, 'DAY')     -- 3-arg hybrid
DATE_ADD(DAY, 30, d)                       -- TIMESTAMPADD signature
```

`DATE_ADD`/`DATE_SUB` take exactly 2 args: `(expr, INTERVAL)`. If you need the 3-arg unit-first form, use `TIMESTAMPADD(DAY, 30, d)` instead — it's a different function.

### DATEDIFF

```sql
-- Spark
datediff(end, start)     →  TIMESTAMPDIFF(DAY, start, end)
```

Note argument order: `TIMESTAMPDIFF(unit, left, right)` computes `right - left`.

### DATE_TRUNC / TIMESTAMP_TRUNC

```sql
-- Spark (string unit, then expr)
date_trunc('MONTH', d)   →  DATE_TRUNC(d, MONTH)       -- for DATE
date_trunc('MONTH', ts)  →  TIMESTAMP_TRUNC(ts, MONTH)  -- for TIMESTAMP
```

Feldera flips the argument order and uses unquoted unit keywords. Use `DATE_TRUNC` for DATE columns, `TIMESTAMP_TRUNC` for TIMESTAMP columns.

### DAY extraction

```sql
-- Spark
DAY(ts_expr)             →  EXTRACT(DAY FROM ts_expr)
```

`YEAR()` and `MONTH()` work as-is. Only `DAY()` needs rewriting for timestamps.

### Unsupported

These have no Feldera equivalent — return as unsupported immediately:

| Function | Reason |
|----------|--------|
| `months_between(end, start)` | Spark-specific fractional semantics |
| `from_unixtime(epoch)` | No epoch → timestamp conversion |
| `to_timestamp(<NUMERIC>)` | No epoch → timestamp conversion |
| `last_day(date)` | Not available |
| `next_day(date, 'Mon')` | Not available |
| `MAKE_DATE(y, m, d)` | Not available |

## Compiler Error Fixes

### `Invalid number of arguments to function 'DATE_ADD'`

You emitted a 3-arg form. Fix to 2-arg:
```sql
DATE_ADD(col, INTERVAL '30' DAY)
-- or simply:
col + INTERVAL '30' DAY
```

### `TIMESTAMPDIFF expects 3 arguments`

You kept Spark's 2-arg `DATEDIFF`. Fix to 3-arg:
```sql
TIMESTAMPDIFF(DAY, start_expr, end_expr)
```

### `No match found for function signature day(<TIMESTAMP>)`

Replace `DAY(ts)` with `EXTRACT(DAY FROM ts)`.
