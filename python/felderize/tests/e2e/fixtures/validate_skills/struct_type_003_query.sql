-- rule: struct_type
-- spark: STRUCT<a: T, b: U> type in DDL
-- feldera: ROW(a T, b U) — Feldera uses ROW with positional syntax
CREATE OR REPLACE TEMP VIEW recent_events_v3 AS
SELECT
  event_id,
  event_name,
  event_metadata.timestamp,
  event_metadata.location,
  event_metadata.attendee_count
FROM event_log
WHERE event_metadata.attendee_count > 50;
