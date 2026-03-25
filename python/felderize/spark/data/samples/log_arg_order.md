---
categories: [math]
---

# LOG argument order reversal (critical)

Spark `log(base, value)` — base first, value second.
Feldera `LOG(value, base)` — value first, base second. Always swap.

Spark:
```sql
SELECT
  log(2, signal_strength)        AS log2_strength,
  log(10, revenue)               AS log10_revenue,
  log(amplitude, 2)              AS custom_log
FROM measurements;
```

Feldera:
```sql
-- NOTE: LOG argument order is reversed vs Spark.
-- Do NOT be misled by column alias names like log2_strength.
SELECT
  LOG(signal_strength, 2)        AS log2_strength,
  LOG(revenue, 10)               AS log10_revenue,
  LOG(2, amplitude)              AS custom_log
FROM measurements;
```
