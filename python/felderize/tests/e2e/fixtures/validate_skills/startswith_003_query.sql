-- rule: startswith
-- spark: startswith(s, prefix) — true if string starts with prefix
-- feldera: LEFT(s, LENGTH(prefix)) = prefix
CREATE OR REPLACE TEMP VIEW startswith_result_003 AS SELECT id, path, startswith(path, '/api/') AS is_api_path FROM url_paths;
