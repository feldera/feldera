-- rule: locate_pos
-- spark: LOCATE(substr, str, pos) — find position starting from pos
-- feldera: CASE WHEN POSITION(substr IN SUBSTRING(str, pos)) = 0 THEN 0 ELSE POSITION(substr IN SUBSTRING(str, pos)) + pos - 1 END
CREATE TABLE log_messages (entry_id INT, message VARCHAR(200), substring_find VARCHAR(30));
