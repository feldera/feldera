-- rule: if_func
-- spark: IF(condition, true_val, false_val) — conditional expression
-- feldera: CASE WHEN condition THEN true_val ELSE false_val END
CREATE OR REPLACE TEMP VIEW user_status_v3 AS SELECT user_id, event_timestamp, IF(is_active = true, 'active', 'inactive') AS status FROM user_events;
