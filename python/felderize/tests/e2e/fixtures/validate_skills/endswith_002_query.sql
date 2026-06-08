-- rule: endswith
-- spark: endswith(s, suffix) — true if string ends with suffix
-- feldera: RIGHT(s, LENGTH(suffix)) = suffix
CREATE OR REPLACE TEMP VIEW filtered_files_v2 AS SELECT id, file_path, extension FROM file_names WHERE endswith(file_path, extension);
