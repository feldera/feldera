-- rule: if_func
-- spark: IF(condition, true_val, false_val) — conditional expression
-- feldera: CASE WHEN condition THEN true_val ELSE false_val END
CREATE TABLE user_events (user_id INT, event_timestamp TIMESTAMP, is_active BOOLEAN);
