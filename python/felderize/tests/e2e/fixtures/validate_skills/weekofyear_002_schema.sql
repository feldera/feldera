-- rule: weekofyear
-- spark: weekofyear(d) — ISO week number of year
-- feldera: EXTRACT(WEEK FROM d)
CREATE TABLE sales_transaction (trans_id INT, trans_date DATE, amount DECIMAL(10,2));
