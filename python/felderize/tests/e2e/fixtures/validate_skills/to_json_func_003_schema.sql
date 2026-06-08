-- rule: to_json_func
-- spark: to_json(v) — serialize a struct/map/array value to a JSON string
-- feldera: TO_JSON(v) — same function, supported directly in Feldera
CREATE TABLE events_t3 (
  event_id BIGINT,
  event_name STRING,
  timestamp_val TIMESTAMP,
  event_code INT
);
