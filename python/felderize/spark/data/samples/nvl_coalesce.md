---
categories: [comparisons]
---

# nvl → COALESCE

Spark:
```sql
SELECT row_id, grp, nvl(amount, discount) AS resolved_amount
FROM scalar_function_rows;
```

Feldera:
```sql
SELECT row_id, grp, COALESCE(amount, discount) AS resolved_amount
FROM scalar_function_rows;
```

