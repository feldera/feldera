-- rule: instr
-- spark: instr(str, substr) — 1-based position of first occurrence (same as LOCATE but arg order reversed)
-- feldera: POSITION(substr IN str)
CREATE OR REPLACE TEMP VIEW error_detection_v3 AS SELECT log_id, message, instr(message, 'ERROR') AS error_pos, CASE WHEN instr(message, 'ERROR') > 0 THEN 'error_found' ELSE 'no_error' END AS status FROM log_messages;
