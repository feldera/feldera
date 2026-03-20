---
categories: [array]
---

# ARRAY lambda (transform)

Spark:
```sql
SELECT row_id, transform(nums, x -> x + 1) AS nums_plus_one
FROM collection_events;
```

Feldera:
```sql
SELECT row_id, TRANSFORM(nums, x -> x + 1) AS nums_plus_one
FROM collection_events;
```

