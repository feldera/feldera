-- rule: make_timestamp
-- spark: make_timestamp(y, mo, d, h, mi, s) — construct TIMESTAMP from year/month/day/hour/minute/second integers
-- feldera: Same — MAKE_TIMESTAMP is natively supported in Feldera
CREATE OR REPLACE TEMP VIEW event_timestamps_v1 AS SELECT event_id, make_timestamp(year_val, month_val, day_val, hour_val, minute_val, second_val) AS created_at FROM event_log;
