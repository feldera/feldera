-- rule: ltrim
-- spark: LTRIM(s) — removes leading whitespace
-- feldera: TRIM(LEADING FROM s)
CREATE OR REPLACE TEMP VIEW bio_view AS SELECT user_id, LTRIM(bio) AS cleaned_bio FROM user_bios WHERE LTRIM(bio) != '';
