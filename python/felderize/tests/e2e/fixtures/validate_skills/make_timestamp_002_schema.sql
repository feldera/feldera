-- rule: make_timestamp
-- spark: make_timestamp(y, mo, d, h, mi, s) — construct TIMESTAMP from year/month/day/hour/minute/second integers
-- feldera: Same — MAKE_TIMESTAMP is natively supported in Feldera
CREATE TABLE audit_events (audit_id INT, evt_year INT, evt_month INT, evt_day INT, evt_hour INT, evt_minute INT, evt_second INT, user_name VARCHAR(100));
