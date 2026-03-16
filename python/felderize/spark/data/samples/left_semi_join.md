---
categories: [comparisons]
---

# LEFT SEMI JOIN

Spark:
```sql
SELECT c.customer_id, c.customer_name, c.segment
FROM customer_dimension c
LEFT SEMI JOIN order_facts o ON c.customer_id = o.customer_id
WHERE o.amount >= 500;
```

Feldera:
```sql
SELECT DISTINCT c.customer_id, c.customer_name, c.segment
FROM customer_dimension c
INNER JOIN order_facts o ON c.customer_id = o.customer_id
WHERE o.amount >= 500;
```

Rewrites: `LEFT SEMI JOIN`→`INNER JOIN` + `SELECT DISTINCT`.
