-- rule: to_date_try_to_timestamp
-- spark: to_date(try_to_timestamp(str, 'yyyyMMdd')) — common TPC-DI pattern: parse a date from an 8-char yyyyMMdd string via timestamp; returns NULL on parse failure
-- feldera: PARSE_DATE('%Y%m%d', str) — collapse the two-step parse into a single PARSE_DATE call; translate 'yyyyMMdd' → '%Y%m%d'
CREATE OR REPLACE TEMP VIEW events_with_dates_v2 AS SELECT
  event_id,
  event_name,
  to_date(try_to_timestamp(event_date_str, 'yyyyMMdd')) AS parsed_event_date
FROM event_log
WHERE event_id > 0;
