-- rule: make_date
-- spark: MAKE_DATE(y, m, d) — construct a DATE from year, month, day integers
-- feldera: Same — MAKE_DATE is natively supported in Feldera
CREATE TABLE birth_records (id INT, birth_year INT, birth_month INT, birth_day INT);
