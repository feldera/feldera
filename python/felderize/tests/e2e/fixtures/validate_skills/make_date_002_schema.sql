-- rule: make_date
-- spark: MAKE_DATE(y, m, d) — construct a DATE from year, month, day integers
-- feldera: Same — MAKE_DATE is natively supported in Feldera
CREATE TABLE event_scheduling (event_id INT, event_year INT, event_month INT, event_day INT, event_name VARCHAR(100));
