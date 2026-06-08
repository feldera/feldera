-- rule: lag_lead
-- spark: LAG(expr, offset, default) / LEAD(expr, offset, default) — access previous/next row
-- feldera: LAG / LEAD — same syntax
CREATE OR REPLACE TEMP VIEW user_event_sequence_v3 AS SELECT user_id, event_id, event_timestamp, event_type, LAG(event_type, 1, 'NONE') OVER (PARTITION BY user_id ORDER BY event_timestamp) AS prev_event_type, LEAD(event_type, 1, 'NONE') OVER (PARTITION BY user_id ORDER BY event_timestamp) AS next_event_type FROM event_log;
