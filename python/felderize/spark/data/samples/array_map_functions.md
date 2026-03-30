---
categories: [array, map]
---

# ARRAY/MAP types and collection functions

Key rewrites: ARRAY<T> → T ARRAY, size → CARDINALITY, element_at(map, key) → map[key].
Note: size() returns -1 for NULL in Spark; CARDINALITY returns NULL in Feldera.
For exact NULL behavior use COALESCE(CARDINALITY(tags), -1). If the column is NOT NULL, CARDINALITY alone is sufficient.

Spark:
```sql
CREATE TABLE session_profiles (
  session_id BIGINT, user_id BIGINT,
  tags ARRAY<STRING>, attributes MAP<STRING, STRING>,
  event_time TIMESTAMP
) USING parquet;

SELECT user_id,
  size(tags)                          AS tag_count,
  array_contains(tags, 'vip')         AS has_vip_tag,
  element_at(attributes, 'source')    AS traffic_source,
  element_at(attributes, 'campaign')  AS campaign
FROM session_profiles;
```

Feldera:
```sql
CREATE TABLE session_profiles (
  session_id BIGINT, user_id BIGINT,
  tags VARCHAR ARRAY, attributes MAP<VARCHAR, VARCHAR>,
  event_time TIMESTAMP
);

-- NOTE: COALESCE(CARDINALITY(tags), -1) matches Spark size() NULL behavior exactly.
SELECT user_id,
  COALESCE(CARDINALITY(tags), -1)     AS tag_count,
  ARRAY_CONTAINS(tags, 'vip')         AS has_vip_tag,
  attributes['source']                AS traffic_source,
  attributes['campaign']              AS campaign
FROM session_profiles;
```
