-- rule: make_date
-- spark: MAKE_DATE(y, m, d) — construct a DATE from year, month, day integers
-- feldera: Same — MAKE_DATE is natively supported in Feldera
CREATE TABLE historical_logs (log_id INT, year_val INT, month_val INT, day_val INT, description VARCHAR(100));
