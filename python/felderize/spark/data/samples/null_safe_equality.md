---
categories: [comparisons]
---

# Null-safe equality <=>

Spark:
```sql
ON p.email <=> e.email AND p.phone <=> e.phone
```

Feldera:
```sql
ON (p.email = e.email OR (p.email IS NULL AND e.email IS NULL))
AND (p.phone = e.phone OR (p.phone IS NULL AND e.phone IS NULL))
```

Rewrites: `a <=> b`→`(a = b OR (a IS NULL AND b IS NULL))`.
