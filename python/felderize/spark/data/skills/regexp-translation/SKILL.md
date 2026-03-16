---
name: regexp-translation
description: Translates Spark regex functions to Feldera. REGEXP_REPLACE and RLIKE are supported. REGEXP_EXTRACT is not.
---

# Regexp Translation

## Supported

| Spark | Feldera | Notes |
|-------|---------|-------|
| `REGEXP_REPLACE(str, pattern, replacement)` | Same | Direct equivalent |
| `RLIKE(str, pattern)` | Same | Direct equivalent |
| `str RLIKE pattern` | Same | Infix syntax also works |

## Unsupported

| Spark | Reason |
|-------|--------|
| `REGEXP_EXTRACT(str, pattern, group)` | No Feldera equivalent — do NOT approximate with REGEXP_REPLACE |

## Examples

Supported:
```sql
-- Spark
SELECT REGEXP_REPLACE(phone, '[^0-9]', '') AS digits FROM contacts
-- Feldera (same)
SELECT REGEXP_REPLACE(phone, '[^0-9]', '') AS digits FROM contacts
```

Unsupported:
```sql
-- Spark
SELECT REGEXP_EXTRACT(url, '(https?://[^/]+)', 1) AS domain FROM logs
-- Return as unsupported
```
