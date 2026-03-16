---
categories: [array, map]
---

# ARRAY/MAP types and collection functions

Spark:
```sql
CREATE TABLE session_profiles (
  session_id BIGINT, user_id BIGINT,
  tags ARRAY<STRING>, attributes MAP<STRING, STRING>,
  event_time TIMESTAMP
) USING parquet;

SELECT user_id, size(tags) AS tag_count,
  array_contains(tags, 'vip') AS has_vip_tag,
  element_at(attributes, 'source') AS traffic_source
FROM session_profiles;
```

Feldera:
```sql
CREATE TABLE session_profiles (
  session_id BIGINT, user_id BIGINT,
  tags VARCHAR ARRAY, attributes MAP<VARCHAR, VARCHAR>,
  event_time TIMESTAMP
);

SELECT user_id, CARDINALITY(tags) AS tag_count,
  ARRAY_CONTAINS(tags, 'vip') AS has_vip_tag,
  attributes['source'] AS traffic_source
FROM session_profiles;
```

Rewrites: `ARRAY<STRING>`→`VARCHAR ARRAY`, `size()`→`CARDINALITY()`, `element_at(map, key)`→`map[key]`.
