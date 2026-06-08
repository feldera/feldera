-- rule: make_timestamp
-- spark: make_timestamp(y, mo, d, h, mi, s) — construct TIMESTAMP from year/month/day/hour/minute/second integers
-- feldera: Same — MAKE_TIMESTAMP is natively supported in Feldera
CREATE OR REPLACE TEMP VIEW audit_timestamps_v2 AS SELECT audit_id, user_name, make_timestamp(evt_year, evt_month, evt_day, evt_hour, evt_minute, evt_second) AS event_time FROM audit_events ORDER BY audit_id;
