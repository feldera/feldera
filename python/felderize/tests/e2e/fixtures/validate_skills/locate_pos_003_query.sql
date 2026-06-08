-- rule: locate_pos
-- spark: LOCATE(substr, str, pos) — find position starting from pos
-- feldera: CASE WHEN POSITION(substr IN SUBSTRING(str, pos)) = 0 THEN 0 ELSE POSITION(substr IN SUBSTRING(str, pos)) + pos - 1 END
CREATE OR REPLACE TEMP VIEW url_parsing_v3 AS SELECT url_id, full_url, fragment, LOCATE(fragment, full_url, 15) AS pos_after_15th_char, LOCATE(fragment, full_url, 1) AS pos_from_beginning FROM url_data;
