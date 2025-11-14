# SQL for ASOF Join in DuckDB (Equivalent to Feldera SQL)

---
### Example 1: `LEFT JOIN` on `DATE` columns:

```sql
CREATE TABLE asof_tbl1 (
    id INT,
    datee DATE
);

CREATE TABLE asof_tbl2 (
    id INT,
    datee DATE
);

INSERT INTO asof_tbl1 VALUES
(1,  DATE '2000-06-21'),
(2,  DATE '2019-06-21');

INSERT INTO asof_tbl2 VALUES
(1,  DATE '2020-06-21'),
(2,  DATE '2021-06-21');

SELECT t1.id, t1.datee AS t1_datee, t2.datee AS t2_datee
FROM asof_tbl1 t1
ASOF LEFT JOIN asof_tbl2 t2
      ON t1.id = t2.id
      AND t1.datee >= t2.datee;
```