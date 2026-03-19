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
ON p.email <=> e.email AND p.phone <=> e.phone
```

Rewrites: none — `<=>` is supported in Feldera.
