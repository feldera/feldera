---
categories: [datetime]
---

# datediff rewrite

Spark `datediff(endDate, startDate)` takes 2 args with end first.
Feldera `DATEDIFF(unit, start, end)` takes 3 args with unit first and start/end swapped.

Spark:
```sql
SELECT order_id,
  datediff(delivered_at, shipped_at) AS days_to_deliver,
  datediff(shipped_at, ordered_at)   AS days_to_ship
FROM orders;
```

Feldera:
```sql
SELECT order_id,
  DATEDIFF(DAY, shipped_at, delivered_at) AS days_to_deliver,
  DATEDIFF(DAY, ordered_at, shipped_at)   AS days_to_ship
FROM orders;
```
