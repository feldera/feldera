CREATE OR REPLACE TEMP VIEW val121_url_prefix AS
SELECT row_id, substring_index(full_url, '/', 3) AS url_prefix
FROM scalar_function_rows
WHERE nullable_str IS NOT NULL;
