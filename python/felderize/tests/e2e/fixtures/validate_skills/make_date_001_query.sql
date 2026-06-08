-- rule: make_date
-- spark: MAKE_DATE(y, m, d) — construct a DATE from year, month, day integers
-- feldera: Same — MAKE_DATE is natively supported in Feldera
CREATE OR REPLACE TEMP VIEW birth_dates_v1 AS SELECT id, MAKE_DATE(birth_year, birth_month, birth_day) AS full_date FROM birth_records ORDER BY id;
