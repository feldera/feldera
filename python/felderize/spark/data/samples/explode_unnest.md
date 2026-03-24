---
categories: [array]
---

# EXPLODE / LATERAL VIEW → UNNEST

Feldera uses UNNEST instead of EXPLODE/LATERAL VIEW.
LATERAL VIEW OUTER has no equivalent — NULL/empty array rows are dropped (add NOTE).

Spark:
```sql
-- Basic explode
SELECT order_id, item
FROM orders LATERAL VIEW explode(items) t AS item;

-- posexplode (0-based position)
SELECT order_id, pos, item
FROM orders LATERAL VIEW posexplode(items) t AS pos, item;

-- inline (array of structs)
SELECT order_id, item.product_id, item.qty
FROM orders LATERAL VIEW inline(line_items) item AS product_id, qty;
```

Feldera:
```sql
-- Basic explode
SELECT order_id, item
FROM orders, UNNEST(items) AS t(item);

-- posexplode: WITH ORDINALITY is 1-based; subtract 1 to match Spark's 0-based pos
SELECT order_id, pos - 1 AS pos, item
FROM orders, UNNEST(items) WITH ORDINALITY AS t(item, pos);

-- inline (array of structs)
SELECT order_id, item.product_id, item.qty
FROM orders, UNNEST(line_items) AS item(product_id, qty);
```
