-- rule: locate_pos
-- spark: LOCATE(substr, str, pos) — find position starting from pos
-- feldera: CASE WHEN POSITION(substr IN SUBSTRING(str, pos)) = 0 THEN 0 ELSE POSITION(substr IN SUBSTRING(str, pos)) + pos - 1 END
CREATE OR REPLACE TEMP VIEW message_analysis_v2 AS SELECT entry_id, message, substring_find, LOCATE(substring_find, message, 10) AS pos_after_tenth, LOCATE(substring_find, message, 1) AS pos_start FROM log_messages;
