---
categories: [aggregates]
---

# ROW_NUMBER TopK

Spark:
```sql
SELECT category, product_id, revenue FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) AS rn
  FROM category_product_sales
) ranked WHERE rn <= 3;
```

Feldera:
```sql
CREATE VIEW top_products AS
SELECT category, product_id, revenue FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) AS rn
  FROM category_product_sales
) ranked WHERE rn <= 3;
```

ROW_NUMBER with TopK filter (`WHERE rn <= N`) passes through directly.
