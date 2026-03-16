---
categories: [datetime]
---

# datediff rewrite

Spark:
```sql
MAX(datediff(delivered_at, shipped_at)) AS max_days
```

Feldera:
```sql
MAX(TIMESTAMPDIFF(DAY, shipped_at, delivered_at)) AS max_days
```

Rewrites: `datediff(end, start)`→`TIMESTAMPDIFF(DAY, start, end)` (argument order reversed).
