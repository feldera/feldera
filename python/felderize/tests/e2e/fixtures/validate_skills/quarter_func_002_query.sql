-- rule: quarter_func
-- spark: quarter(d) — extract quarter number (1-4) from a DATE or TIMESTAMP
-- feldera: QUARTER(d) — same function name, supported directly in Feldera
CREATE OR REPLACE TEMP VIEW events_by_quarter_v2 AS SELECT event_id, event_timestamp, quarter(event_timestamp) AS q, event_type FROM event_logs;
