-- rule: make_date
-- spark: MAKE_DATE(y, m, d) — construct a DATE from year, month, day integers
-- feldera: Same — MAKE_DATE is natively supported in Feldera
CREATE OR REPLACE TEMP VIEW historical_dates_v3 AS SELECT log_id, MAKE_DATE(year_val, month_val, day_val) AS log_date, description FROM historical_logs ORDER BY MAKE_DATE(year_val, month_val, day_val);
