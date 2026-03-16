CREATE TABLE session_profiles (
  session_id BIGINT,
  user_id BIGINT,
  tags VARCHAR ARRAY,
  attributes MAP<VARCHAR, VARCHAR>,
  event_time TIMESTAMP
);

CREATE VIEW session_tag_summary AS
SELECT
  user_id,
  CARDINALITY(tags) AS tag_count,
  ARRAY_CONTAINS(tags, 'vip') AS has_vip_tag,
  attributes['source'] AS traffic_source
FROM session_profiles;
