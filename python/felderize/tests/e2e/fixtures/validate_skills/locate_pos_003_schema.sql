-- rule: locate_pos
-- spark: LOCATE(substr, str, pos) — find position starting from pos
-- feldera: CASE WHEN POSITION(substr IN SUBSTRING(str, pos)) = 0 THEN 0 ELSE POSITION(substr IN SUBSTRING(str, pos)) + pos - 1 END
CREATE TABLE url_data (url_id INT, full_url VARCHAR(250), fragment VARCHAR(50));
