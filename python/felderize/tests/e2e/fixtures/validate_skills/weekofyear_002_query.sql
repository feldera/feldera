-- rule: weekofyear
-- spark: weekofyear(d) — ISO week number of year
-- feldera: EXTRACT(WEEK FROM d)
CREATE OR REPLACE TEMP VIEW sales_week_v2 AS SELECT trans_id, trans_date, amount, weekofyear(trans_date) AS week_num FROM sales_transaction;
