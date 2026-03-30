---
categories: [json]
---

# JSON extraction with get_json_object / json_tuple

Feldera uses VARIANT type. Parse with PARSE_JSON, then access fields with bracket syntax.
get_json_object returns STRING only — numbers/booleans need CAST to correct type.
Feldera supports lateral aliases — parse once and reuse: `SELECT PARSE_JSON(col) AS v, v['field'] ...`

Spark:
```sql
SELECT
  get_json_object(payload, '$.user_id')   AS user_id,
  get_json_object(payload, '$.amount')    AS amount,
  get_json_object(payload, '$.currency')  AS currency
FROM raw_events;
```

Feldera:
```sql
-- Parse once using lateral alias, reuse v per field
SELECT
  PARSE_JSON(payload)                      AS v,
  CAST(v['user_id']  AS VARCHAR)           AS user_id,
  CAST(v['amount']   AS DOUBLE)            AS amount,
  CAST(v['currency'] AS VARCHAR)           AS currency
FROM raw_events;
```

---

Spark (multi-field with GROUP BY):
```sql
SELECT
  get_json_object(payload, '$.region') AS region,
  COUNT(*)                             AS cnt
FROM raw_events
GROUP BY get_json_object(payload, '$.region');
```

Feldera (use CTE to pre-parse when GROUP BY needs the parsed value):
```sql
CREATE VIEW summary AS
WITH parsed AS (
  SELECT *, PARSE_JSON(payload) AS v FROM raw_events
)
SELECT
  CAST(v['region'] AS VARCHAR) AS region,
  COUNT(*)                     AS cnt
FROM parsed
GROUP BY CAST(v['region'] AS VARCHAR);
```
