-- rule: endswith
-- spark: endswith(s, suffix) — true if string ends with suffix
-- feldera: RIGHT(s, LENGTH(suffix)) = suffix
CREATE TABLE file_names (id INT, file_path STRING, extension STRING);
