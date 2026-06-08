-- rule: rtrim
-- spark: RTRIM(s) — removes trailing whitespace
-- feldera: TRIM(TRAILING FROM s)
CREATE OR REPLACE TEMP VIEW clean_descriptions AS SELECT item_id, RTRIM(desc) AS trimmed_desc FROM descriptions;
