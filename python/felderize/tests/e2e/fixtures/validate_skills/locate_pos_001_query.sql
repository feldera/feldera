-- rule: locate_pos
-- spark: LOCATE(substr, str, pos) — find position starting from pos
-- feldera: CASE WHEN POSITION(substr IN SUBSTRING(str, pos)) = 0 THEN 0 ELSE POSITION(substr IN SUBSTRING(str, pos)) + pos - 1 END
CREATE OR REPLACE TEMP VIEW email_search_v1 AS SELECT id, email_address, search_pattern, LOCATE(search_pattern, email_address, 1) AS pos_from_start, LOCATE(search_pattern, email_address, 5) AS pos_from_fifth FROM email_log;
