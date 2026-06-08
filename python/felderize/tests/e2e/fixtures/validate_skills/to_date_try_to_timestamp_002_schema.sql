-- rule: to_date_try_to_timestamp
-- spark: to_date(try_to_timestamp(str, 'yyyyMMdd')) — common TPC-DI pattern: parse a date from an 8-char yyyyMMdd string via timestamp; returns NULL on parse failure
-- feldera: PARSE_DATE('%Y%m%d', str) — collapse the two-step parse into a single PARSE_DATE call; translate 'yyyyMMdd' → '%Y%m%d'
CREATE TABLE event_log (
  event_id BIGINT,
  event_date_str STRING,
  event_name STRING
);
