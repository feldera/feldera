CREATE OR REPLACE TEMP VIEW session_tag_summary AS
SELECT
  user_id,
  size(tags) AS tag_count,
  array_contains(tags, 'vip') AS has_vip_tag,
  element_at(attributes, 'source') AS traffic_source
FROM session_profiles;
