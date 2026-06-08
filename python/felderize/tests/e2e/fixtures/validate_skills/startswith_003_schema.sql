-- rule: startswith
-- spark: startswith(s, prefix) — true if string starts with prefix
-- feldera: LEFT(s, LENGTH(prefix)) = prefix
CREATE TABLE url_paths (id INT, path STRING);
