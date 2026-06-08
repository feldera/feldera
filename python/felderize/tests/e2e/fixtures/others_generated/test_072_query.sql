CREATE OR REPLACE TEMP VIEW val26_url_prefix AS
SELECT row_id, substring_index(full_url, '/', 3) AS url_prefix
FROM text_date_events;
