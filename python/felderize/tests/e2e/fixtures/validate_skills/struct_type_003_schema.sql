-- rule: struct_type
-- spark: STRUCT<a: T, b: U> type in DDL
-- feldera: ROW(a T, b U) — Feldera uses ROW with positional syntax
CREATE TABLE event_log (
  event_id INT,
  event_name STRING,
  event_metadata STRUCT<timestamp: TIMESTAMP, location: STRING, attendee_count: INT>
);
