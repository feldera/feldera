# Alternative for FILTER(WHERE...) in MySQL
Since MySQL doesn't support `FILTER (WHERE ...)`, following alternatives are used to validate the tests in MySQL:

---

### Alternative query for `FILTER (WHERE condition)` without GROUP BY:

```sql
SELECT
  (SELECT aggregate(col1) FROM tbl WHERE condition) AS col1,
                         ...............
  (SELECT aggregate(coln) FROM tbl WHERE condition) AS coln;
```


### Alternative query for `FILTER (WHERE condition)`  with GROUP BY:

```sql
SELECT id, aggregate(col1) AS agg_c1 FROM tbl WHERE condition GROUP BY id;,
                        ...............
SELECT id, aggregate(coln) AS agg_cn FROM tbl WHERE condition GROUP BY id;
```