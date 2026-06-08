-- rule: greatest_least_nulls
-- spark: greatest(a, b, ...) / least(a, b, ...) — return maximum/minimum value; Spark skips NULL values
-- feldera: GREATEST_IGNORE_NULLS(a, b, ...) / LEAST_IGNORE_NULLS(a, b, ...) — use the IGNORE_NULLS variant to match Spark's null-skipping semantics
CREATE OR REPLACE TEMP VIEW event_timeline_v3 AS SELECT event_id, greatest(ts1, ts2, ts3) AS latest_time, least(ts1, ts2, ts3) AS earliest_time FROM timestamp_events;
